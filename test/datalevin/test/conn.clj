(ns datalevin.test.conn
  (:require
   [clojure.string :as str]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check :as tc]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as dc]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.ha.replication :as repl]
   [datalevin.interface :as i]
   [datalevin.constants :as c]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as lmdb]
   [datalevin.main :as main]
   [datalevin.search :as search]
   [datalevin.sparselist :as sl]
   [datalevin.storage :as s]
   [datalevin.txlog :as txlog]
   [datalevin.util :as u])
  (:import
   [datalevin.idoc IdocIndex]
   [datalevin.sparselist SparseIntArrayList]
   [datalevin.spill SpillableMap]
   [datalevin.utl LRUCache]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.vector VectorIndex]
   [java.nio ByteBuffer]
   [java.util Arrays Date Random UUID]
   [java.util.concurrent CountDownLatch TimeUnit]
   [org.roaringbitmap.buffer ImmutableRoaringBitmap]))

(use-fixtures :each db-fixture)

(defn- conn-store
  [conn]
  (.-store ^DB @conn))

(defn- conn-search-engine
  [conn domain]
  (get (.-search-engines ^Store (conn-store conn)) domain))

(defn- conn-env-opts
  [conn]
  (i/env-opts (.-lmdb ^Store (conn-store conn))))

(defn- txlog-ops
  [db]
  (mapcat :ops (kv/open-tx-log db 1)))

(defn- txlog-dbi-registration-op
  [ops op dbi-name]
  (some (fn [row]
          (when (and (= op (nth row 0 nil))
                     (= c/kv-info (nth row 1 nil))
                     (= [:dbis dbi-name] (nth row 2 nil)))
            row))
        ops))

(defn- txlog-record-has-dbi?
  [record dbi-name]
  (some #(= dbi-name (nth % 1 nil)) (:ops record)))

(def ^:private idoc-fulltext-fuzz-statuses
  ["active" "inactive" "archived"])

(def ^:private idoc-fulltext-fuzz-tags
  ["red" "blue" "green" "gold"])

(def ^:private idoc-fulltext-fuzz-tokens
  ["alpha" "bravo" "charlie" "delta" "echo"])

(defn- idoc-fulltext-schema
  []
  {:doc/idoc {:db/valueType :db.type/idoc
              :db/domain    "profiles"}
   :doc/text {:db/valueType           :db.type/string
              :db/fulltext            true
              :db.fulltext/autoDomain true}})

(defn- idoc-fulltext-opts
  []
  {:wal?                      true
   :wal-durability-profile    :strict
   :snapshot-bootstrap-force? false
   :search-domains            {"doc/text" {:index-position? true}}})

(def ^:private idoc-fulltext-idoc-q
  '[:find [?e ...]
    :in $ ?query
    :where
    [(idoc-match $ :doc/idoc ?query)
     [[?e ?a ?v]]]])

(def ^:private idoc-fulltext-token-q
  '[:find [?e ...]
    :in $ ?token
    :where
    [(fulltext $ :doc/text ?token)
     [[?e ?a ?v]]]])

(def ^:private idoc-fulltext-combined-q
  '[:find [?e ...]
    :in $ ?query ?token
    :where
    [(idoc-match $ :doc/idoc ?query)
     [[?e ?ia ?iv]]]
    [(fulltext $ :doc/text ?token)
     [[?e ?ta ?tv]]]])

(defn- pick
  [^Random rng xs]
  (nth xs (.nextInt rng (count xs))))

(defn- idoc-fulltext-doc
  [^Random rng step eid]
  (let [tag-a (pick rng idoc-fulltext-fuzz-tags)
        tag-b (pick rng idoc-fulltext-fuzz-tags)]
    {:status (pick rng idoc-fulltext-fuzz-statuses)
     :tags   (vec (distinct [tag-a tag-b]))
     :entity eid
     :step   step}))

(defn- idoc-fulltext-text
  [^Random rng step eid]
  (str (pick rng idoc-fulltext-fuzz-tokens) " "
       (pick rng idoc-fulltext-fuzz-tokens) " "
       (pick rng idoc-fulltext-fuzz-tokens) " "
       "entity-" eid " step-" step))

(defn- idoc-fulltext-upsert-action
  [^Random rng step eid]
  (let [doc  (idoc-fulltext-doc rng step eid)
        text (idoc-fulltext-text rng step eid)]
    {:action :upsert
     :eid    eid
     :doc    doc
     :text   text
     :tx     [{:db/id eid :doc/idoc doc :doc/text text}]}))

(defn- idoc-fulltext-retract-action
  [eid {:keys [doc text]}]
  {:action :retract
   :eid    eid
   :tx     [[:db/retract eid :doc/idoc doc]
            [:db/retract eid :doc/text text]]})

(defn- idoc-fulltext-fuzz-actions
  [^Random rng step oracle]
  (let [n (inc (.nextInt rng 4))]
    (loop [actions []
           oracle' oracle
           seen    #{}]
      (if (= n (count actions))
        [actions oracle']
        (let [eid       (inc (.nextInt rng 12))
              existing  (get oracle' eid)
              retract?  (and existing (< (.nextInt rng 5) 2))
              action    (if retract?
                          (idoc-fulltext-retract-action eid existing)
                          (idoc-fulltext-upsert-action rng step eid))
              oracle'' (if retract?
                         (dissoc oracle' eid)
                         (assoc oracle' eid
                                {:doc (:doc action) :text (:text action)}))]
          (if (seen eid)
            (recur actions oracle' seen)
            (recur (conj actions action) oracle'' (conj seen eid))))))))

(defn- text-has-token?
  [text token]
  (some #{token} (str/split text #"\s+")))

(defn- expected-idoc-fulltext-status
  [oracle status]
  (->> oracle
       (keep (fn [[eid {:keys [doc]}]]
               (when (= status (:status doc)) eid)))
       set))

(defn- expected-idoc-fulltext-tag
  [oracle tag]
  (->> oracle
       (keep (fn [[eid {:keys [doc]}]]
               (when (some #{tag} (:tags doc)) eid)))
       set))

(defn- expected-idoc-fulltext-token
  [oracle token]
  (->> oracle
       (keep (fn [[eid {:keys [text]}]]
               (when (text-has-token? text token) eid)))
       set))

(defn- expected-idoc-fulltext-combined
  [oracle status token]
  (->> oracle
       (keep (fn [[eid {:keys [doc text]}]]
               (when (and (= status (:status doc))
                          (text-has-token? text token))
                 eid)))
       set))

(defn- verify-idoc-fulltext-oracle!
  [db oracle seed step]
  (doseq [status idoc-fulltext-fuzz-statuses]
    (is (= (expected-idoc-fulltext-status oracle status)
           (set (d/q idoc-fulltext-idoc-q db {:status status})))
        (pr-str {:seed seed :step step :status status})))
  (doseq [tag idoc-fulltext-fuzz-tags]
    (is (= (expected-idoc-fulltext-tag oracle tag)
           (set (d/q idoc-fulltext-idoc-q db {:tags tag})))
        (pr-str {:seed seed :step step :tag tag})))
  (doseq [token idoc-fulltext-fuzz-tokens]
    (is (= (expected-idoc-fulltext-token oracle token)
           (set (d/q idoc-fulltext-token-q db token)))
        (pr-str {:seed seed :step step :token token}))))

(defn- verify-idoc-fulltext-combined!
  [db oracle seed step status token]
  (is (= (expected-idoc-fulltext-combined oracle status token)
         (set (d/q idoc-fulltext-combined-q db {:status status} token)))
      (pr-str {:seed seed :step step :status status :token token})))

(defn- idoc-fulltext-query-mismatch
  [query expected actual details]
  (when-not (= expected actual)
    (assoc details :query query :expected expected :actual actual)))

(defn- idoc-fulltext-probe-mismatch
  [db oracle status tag token]
  (or
    (idoc-fulltext-query-mismatch
      :idoc-status
      (expected-idoc-fulltext-status oracle status)
      (set (d/q idoc-fulltext-idoc-q db {:status status}))
      {:status status})
    (idoc-fulltext-query-mismatch
      :idoc-tag
      (expected-idoc-fulltext-tag oracle tag)
      (set (d/q idoc-fulltext-idoc-q db {:tags tag}))
      {:tag tag})
    (idoc-fulltext-query-mismatch
      :fulltext-token
      (expected-idoc-fulltext-token oracle token)
      (set (d/q idoc-fulltext-token-q db token))
      {:token token})
    (idoc-fulltext-query-mismatch
      :combined-status-token
      (expected-idoc-fulltext-combined oracle status token)
      (set (d/q idoc-fulltext-combined-q db {:status status} token))
      {:status status :token token})))

(defn- idoc-fulltext-full-mismatch
  [db oracle]
  (or
    (some (fn [status]
            (idoc-fulltext-query-mismatch
              :idoc-status
              (expected-idoc-fulltext-status oracle status)
              (set (d/q idoc-fulltext-idoc-q db {:status status}))
              {:status status}))
          idoc-fulltext-fuzz-statuses)
    (some (fn [tag]
            (idoc-fulltext-query-mismatch
              :idoc-tag
              (expected-idoc-fulltext-tag oracle tag)
              (set (d/q idoc-fulltext-idoc-q db {:tags tag}))
              {:tag tag}))
          idoc-fulltext-fuzz-tags)
    (some (fn [token]
            (idoc-fulltext-query-mismatch
              :fulltext-token
              (expected-idoc-fulltext-token oracle token)
              (set (d/q idoc-fulltext-token-q db token))
              {:token token}))
          idoc-fulltext-fuzz-tokens)
    (some (fn [[status token]]
            (idoc-fulltext-query-mismatch
              :combined-status-token
              (expected-idoc-fulltext-combined oracle status token)
              (set (d/q idoc-fulltext-combined-q db {:status status} token))
              {:status status :token token}))
          (for [status idoc-fulltext-fuzz-statuses
                token  idoc-fulltext-fuzz-tokens]
            [status token]))))

(defn- throw-idoc-fulltext-mismatch!
  [db oracle context mismatch-fn]
  (when-let [mismatch (mismatch-fn db oracle)]
    (throw
      (ex-info "idoc/fulltext query mismatch"
               (merge context mismatch)))))

(defn- idoc-fulltext-apply-property-action
  [{:keys [oracle tx]} {:keys [op eid doc text]}]
  (case op
    :upsert
    {:oracle (assoc oracle eid {:doc doc :text text})
     :tx     (conj tx {:db/id eid :doc/idoc doc :doc/text text})}

    :retract
    (let [{old-doc :doc old-text :text} (get oracle eid)]
      {:oracle (dissoc oracle eid)
       :tx     (conj tx
                     [:db/retract eid :doc/idoc (or old-doc doc)]
                     [:db/retract eid :doc/text (or old-text text)])})

    :retract-entity
    {:oracle (dissoc oracle eid)
     :tx     (conj tx [:db/retractEntity eid])}))

(defn- idoc-fulltext-property-batch-result
  [oracle actions]
  (reduce idoc-fulltext-apply-property-action
          {:oracle oracle :tx []}
          actions))

(defn- distinct-actions-by-eid
  [actions]
  (second
    (reduce (fn [[seen acc] {:keys [eid] :as action}]
              (if (seen eid)
                [seen acc]
                [(conj seen eid) (conj acc action)]))
            [#{} []]
            actions)))

(def ^:private idoc-fulltext-property-action-gen
  (gen/let [op      (gen/frequency [[6 (gen/return :upsert)]
                                    [2 (gen/return :retract)]
                                    [1 (gen/return :retract-entity)]])
            eid     (gen/choose 1 18)
            status  (gen/elements idoc-fulltext-fuzz-statuses)
            tags    (gen/vector
                      (gen/elements idoc-fulltext-fuzz-tags) 1 3)
            tokens  (gen/vector
                      (gen/elements idoc-fulltext-fuzz-tokens) 1 5)
            tier    (gen/elements ["free" "pro" "enterprise"])
            score   (gen/choose 0 1000)
            active? gen/boolean]
    (let [doc  {:status  status
                :tags    (vec (distinct tags))
                :entity  eid
                :profile {:tier   tier
                          :score  score
                          :active active?}
                :flags   [active? (not active?)]}
          text (str/join " "
                         (conj tokens
                               (str "entity-" eid)
                               status
                               tier))]
      {:op op :eid eid :doc doc :text text})))

(def ^:private idoc-fulltext-property-actions-gen
  (gen/fmap distinct-actions-by-eid
            (gen/vector idoc-fulltext-property-action-gen 1 6)))

(def ^:private idoc-fulltext-property-batch-gen
  (gen/let [actions idoc-fulltext-property-actions-gen
            status  (gen/elements idoc-fulltext-fuzz-statuses)
            tag     (gen/elements idoc-fulltext-fuzz-tags)
            token   (gen/elements idoc-fulltext-fuzz-tokens)]
    {:actions actions
     :status  status
     :tag     tag
     :token   token}))

(def ^:private idoc-fulltext-property-scenario-gen
  (gen/vector idoc-fulltext-property-batch-gen 1 24))

(defn- run-idoc-fulltext-property-scenario!
  [scenario]
  (let [dir  (u/tmp-dir (str "test-idoc-fulltext-property-"
                             (UUID/randomUUID)))
        opts (idoc-fulltext-opts)
        conn (d/create-conn dir (idoc-fulltext-schema) opts)]
    (try
      (let [final-oracle
            (loop [step    0
                   oracle  {}
                   batches scenario]
              (if (seq batches)
                (let [{:keys [actions status tag token] :as batch} (first batches)
                      {:keys [oracle tx]} (idoc-fulltext-property-batch-result
                                            oracle actions)
                      context {:step    step
                               :batch   batch
                               :tx-data tx}]
                  (try
                    (d/transact! conn tx)
                    (throw-idoc-fulltext-mismatch!
                      @conn oracle context
                      #(idoc-fulltext-probe-mismatch
                         %1 %2 status tag token))
                    (catch Throwable t
                      (throw
                        (ex-info "idoc/fulltext property scenario failed"
                                 (assoc context :oracle oracle)
                                 t))))
                  (recur (inc step) oracle (rest batches)))
                oracle))]
        (throw-idoc-fulltext-mismatch!
          @conn final-oracle {:step :final}
          idoc-fulltext-full-mismatch)
        (d/close conn)
        (let [conn2 (d/create-conn dir nil opts)]
          (try
            (throw-idoc-fulltext-mismatch!
              @conn2 final-oracle {:step :reopen}
              idoc-fulltext-full-mismatch)
            (finally
              (d/close conn2)))))
      true
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

(defn- copy-snapshot-files-to-env!
  [snapshot-path env-dir]
  (doseq [^java.io.File f (or (u/list-files snapshot-path) [])
          :when (.isFile f)
          :when (not= "snapshot.edn" (.getName f))]
    (u/copy-file (.getPath f)
                 (str env-dir u/+separator+ (.getName f)))))

(defn- first-wal-segment-path
  [txlog-dir]
  (->> (or (u/list-files txlog-dir) [])
       (map #(.getPath ^java.io.File %))
       (filter #(str/ends-with? % ".wal"))
       sort
       first))

(defn- test-ha-opts
  [raft-dir]
  (let [group-id    (str "test-ha-group-" (UUID/randomUUID))
        db-identity (str "test-ha-db-" (UUID/randomUUID))
        members     [{:node-id 1 :endpoint "127.0.0.1:19001"}
                     {:node-id 2 :endpoint "127.0.0.1:19002"}
                     {:node-id 3 :endpoint "127.0.0.1:19003"}]
        voters      [{:peer-id "127.0.0.1:19101"
                      :ha-node-id 1
                      :promotable? true}
                     {:peer-id "127.0.0.1:19102"
                      :ha-node-id 2
                      :promotable? true}
                     {:peer-id "127.0.0.1:19103"
                      :ha-node-id 3
                      :promotable? true}]]
    {:db-identity db-identity
     :ha-mode :consensus-lease
     :ha-lease-renew-ms 1000
     :ha-lease-timeout-ms 5000
     :ha-promotion-base-delay-ms 100
     :ha-promotion-rank-delay-ms 200
     :ha-max-promotion-lag-lsn 0
     :ha-clock-skew-budget-ms 1000
     :ha-members members
     :ha-control-plane {:backend :sofa-jraft
                        :group-id group-id
                        :voters voters
                        :rpc-timeout-ms 1000
                        :election-timeout-ms 1000
                        :operation-timeout-ms 1000
                        :raft-dir raft-dir}}))

(deftest test-datalog-wal-default-is-opt-in
  (let [dir  (u/tmp-dir (str "test-datalog-wal-default-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (try
      (is (false? (:wal? (conn-env-opts conn))))
      (is (false? (:wal? (i/opts (conn-store conn)))))
      (is (not (u/file-exists (str dir u/+separator+ "txlog"))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest new-db-does-not-enqueue-empty-secondary-index-work-test
  (let [dir      (u/tmp-dir (str "test-empty-secondary-index-work-"
                                 (UUID/randomUUID)))
        enqueued (atom 0)]
    (try
      (with-redefs [s/enqueue-secondary-index-work!
                    (fn [store]
                      (swap! enqueued inc)
                      store)]
        (let [conn (d/create-conn dir)]
          (try
            (is (zero? @enqueued))
            (finally
              (d/close conn)))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-wal-opt-in-defaults-to-relaxed
  (let [dir  (u/tmp-dir (str "test-datalog-wal-relaxed-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir nil {:wal? true})]
    (try
      (is (true? (:wal? (conn-env-opts conn))))
      (is (= :relaxed (:wal-durability-profile (conn-env-opts conn))))
      (is (true? (:wal? (i/opts (conn-store conn)))))
      (is (= :relaxed
             (:wal-durability-profile (i/opts (conn-store conn)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-datalog-kv-exposes-backing-kv-handle
  (let [dir  (u/tmp-dir (str "test-datalog-kv-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (try
      (let [kv (d/datalog-kv conn)]
        (is (= dir (d/dir kv)))
        (d/open-dbi kv "app-state")
        (d/transact-kv kv "app-state" [[:put "k" "v"]] :string :string)
        (is (= "v" (d/get-value kv "app-state" "k" :string :string true)))
        (is (= kv (d/datalog-kv @conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-auto-dump-load-mixed-datalog-and-kv
  (let [src       (u/tmp-dir (str "test-auto-dump-mixed-src-"
                                  (UUID/randomUUID)))
        dst       (u/tmp-dir (str "test-auto-dump-mixed-dst-"
                                  (UUID/randomUUID)))
        dump-dir  (u/tmp-dir (str "test-auto-dump-mixed-file-"
                                  (UUID/randomUUID)))
        dump-file (str dump-dir u/+separator+ "dump.edn")
        schema    {:person/name {:db/valueType :db.type/string
                                 :db/unique    :db.unique/identity}}]
    (try
      (u/create-dirs dump-dir)
      (let [conn (d/create-conn src schema)]
        (try
          (d/transact! conn [{:person/name "Ada"}])
          (let [kv (d/datalog-kv conn)]
            (d/open-dbi kv "app-state")
            (d/transact-kv kv "app-state" [[:put "theme" "dark"]]
                           :string :string))
          (finally
            (d/close conn))))
      (main/dump src dump-file nil false false false)
      (main/load dst dump-file nil false)
      (let [conn (d/create-conn dst)]
        (try
          (is (= #{["Ada"]}
                 (d/q '[:find ?name
                        :where [_ :person/name ?name]]
                      @conn)))
          (let [kv (d/datalog-kv conn)]
            (d/open-dbi kv "app-state")
            (is (= "dark"
                   (d/get-value kv "app-state" "theme"
                                :string :string true))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files src)
        (u/delete-files dst)
        (u/delete-files dump-dir)))))

(deftest test-kv-wal-opt-in-defaults-to-relaxed
  (let [dir (u/tmp-dir (str "test-kv-wal-relaxed-"
                            (UUID/randomUUID)))
        db  (d/open-kv dir {:wal? true})]
    (try
      (is (= :relaxed (:durability-profile (d/txlog-watermarks db))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(deftest test-kv-wal-logs-dbi-lifecycle
  (let [dir  (u/tmp-dir (str "test-kv-wal-dbi-lifecycle-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "live" {:key-size 64})
          (d/drop-dbi db "live")
          (kv/force-txlog-sync! db)
          (let [ops    (txlog-ops db)
                put-op (txlog-dbi-registration-op ops :put "live")
                del-op (txlog-dbi-registration-op ops :del "live")]
            (is (some? put-op))
            (is (= [:keyword :string] (nth put-op 4)))
            (is (= :data (nth put-op 5)))
            (is (some? del-op))
            (is (= [:keyword :string] (nth del-op 3))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-ha-forces-safe-wal
  (let [dir  (u/tmp-dir (str "test-datalog-ha-wal-"
                             (UUID/randomUUID)))
        conn (d/create-conn
              dir
              nil
              (test-ha-opts
               (str dir u/+separator+ "ha-control-raft")))]
    (try
      (is (true? (:wal? (conn-env-opts conn))))
      (is (= :strict (:wal-durability-profile (conn-env-opts conn))))
      (is (true? (:wal? (i/opts (conn-store conn)))))
      (is (= :strict
             (:wal-durability-profile (i/opts (conn-store conn)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-datalog-ha-reopen-persists-wal-runtime-opts-for-kv-open
  (let [dir (u/tmp-dir (str "test-datalog-ha-wal-reopen-"
                            (UUID/randomUUID)))]
    (try
      (let [conn (d/create-conn dir)]
        (d/close conn))
      (let [conn (d/create-conn
                  dir
                  nil
                  (test-ha-opts
                   (str dir u/+separator+ "ha-control-raft")))]
        (try
          (is (true? (:wal? (conn-env-opts conn))))
          (is (= :strict (:wal-durability-profile (conn-env-opts conn))))
          (finally
            (d/close conn))))
      (let [db (d/open-kv dir)]
        (try
          (is (true? (i/get-value db c/kv-info :wal? :keyword :data)))
          (is (= :strict
                 (i/get-value db c/kv-info :wal-durability-profile
                              :keyword :data)))
          (is (true? (:wal? (kv/txlog-watermarks db))))
          (is (= :strict (:durability-profile (kv/txlog-watermarks db))))
          (is (vector? (vec (kv/open-tx-log-rows db 1 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-ha-open-metadata-does-not-consume-replicated-wal
  (let [dir  (u/tmp-dir (str "test-datalog-ha-open-metadata-wal-"
                             (UUID/randomUUID)))
        opts (test-ha-opts (str dir u/+separator+ "ha-control-raft"))]
    (try
      (let [conn (d/create-conn dir nil opts)]
        (try
          (let [records (vec (kv/open-tx-log
                              (.-lmdb ^Store (conn-store conn))
                              1))]
            (is (seq records))
            (is (not-any? #(txlog-record-has-dbi? % c/opts) records)))
          (finally
            (d/close conn))))
      (let [conn (d/create-conn dir nil opts)]
        (try
          (let [records (vec (kv/open-tx-log
                              (.-lmdb ^Store (conn-store conn))
                              1))]
            (is (seq records))
            (is (not-any? #(txlog-record-has-dbi? % c/opts) records)))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-datalog-ha-rejects-relaxed-wal
  (let [dir (u/tmp-dir (str "test-datalog-ha-relaxed-"
                            (UUID/randomUUID)))]
    (try
      (is (thrown-with-msg?
            Exception
            #"Consensus-lease HA requires :wal-durability-profile :strict or :extra"
            (d/create-conn dir nil
                           (assoc (test-ha-opts
                                   (str dir u/+separator+ "ha-control-raft"))
                                  :wal-durability-profile :relaxed))))
      (finally
        (u/delete-files dir)))))

(deftest test-close
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (not (d/closed? conn)))
    (d/close conn)
    (is (d/closed? conn))
    (is (nil? @conn))
    (u/delete-files dir)))

(deftest test-update-schema
  (let [dir1  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        dir2  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn1 (d/create-conn dir1)
        aid0   (count c/implicit-schema)
        s     {:a/b {:db/valueType :db.type/string}}
        s1    {:c/d {:db/valueType :db.type/string}}
        txs   [{:c/d "cd" :db/id -1}
               {:a/b "ab" :db/id -2}]
        conn2 (d/create-conn dir2 s)]
    (is (= (d/schema conn2) (d/update-schema conn1 s)))
    (d/update-schema conn1 s1)
    (is (= (d/schema conn1) (-> (merge c/implicit-schema s s1)
                                (assoc-in [:a/b :db/aid] aid0)
                                (assoc-in [:c/d :db/aid] (inc aid0)))))
    (d/transact! conn1 txs)
    (is (= 2 (count (d/datoms @conn1 :eav))))

    (is (thrown-with-msg? Exception #"Cannot delete attribute"
                          (d/update-schema conn1 {} #{:c/d})))

    (d/transact! conn1 [[:db/retractEntity 1]])
    (is (= (d/schema conn2)
           (d/update-schema conn1 {} #{:c/d})
           (d/schema conn1)))

    (d/update-schema conn1 nil nil {:a/b :e/f})
    (is (= (d/schema conn1) (assoc c/implicit-schema :e/f
                                   {:db/valueType :db.type/string
                                    :db/aid       aid0})))

    (d/close conn1)
    (d/close conn2)
    (u/delete-files dir1)
    (u/delete-files dir2)))

(deftest test-update-schema-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)
        aid0 (count c/implicit-schema)]
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0))))
    (d/update-schema conn {:stuff {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:things :db/aid] aid0)
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {} [:things])
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0)))))
    (d/update-schema conn {:things {}})
    (is (= (d/schema conn) (-> c/implicit-schema
                               (assoc-in [:stuff :db/aid] (inc aid0))
                               (assoc-in [:things :db/aid] (+ aid0 2)))))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-update-schema-ensure-no-duplicate-aids
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (d/update-schema conn {:up/a {}})
    (d/transact! conn [{:foo 1}])
    (let [aids (map :db/aid (vals (d/schema conn)))]
      (is (= (count aids) (count (set aids))))
      (d/close conn)
      (u/delete-files dir))))

(deftest test-update-schema-validates-new-attrs
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    ;; :db/isComponent true requires :db/valueType :db.type/ref
    (is (thrown-with-msg?
          Exception #"isComponent.*should also have.*ref"
          (d/update-schema conn {:bad/attr {:db/isComponent true
                                            :db/valueType   :db.type/string}})))
    ;; invalid :db/valueType
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/valueType :db.type/bogus}})))
    ;; invalid :db/cardinality
    (is (thrown-with-msg?
          Exception #"Bad attribute specification"
          (d/update-schema conn {:bad/attr {:db/cardinality :db.cardinality/bogus}})))
    ;; valid schema still works
    (d/update-schema conn {:good/attr {:db/valueType :db.type/string}})
    (is (:good/attr (d/schema conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-update-schema-property-patches
  (let [dir    (u/tmp-dir (str "schema-patch-" (UUID/randomUUID)))
        schema {:user/email  {:db/valueType   :db.type/string
                              :db/cardinality :db.cardinality/one
                              :db/doc         "Email address"}
                :user/friend {:db/valueType :db.type/ref}}
        conn   (d/create-conn dir schema)]
    (try
      (let [email-aid (get-in (d/schema conn) [:user/email :db/aid])]
        ;; A supplied property map patches, rather than replaces, the existing
        ;; attribute definition.
        (d/update-schema conn
                         {:user/email {:db/unique :db.unique/identity}})
        (is (= {:db/valueType   :db.type/string
                :db/cardinality :db.cardinality/one
                :db/doc         "Email address"
                :db/unique      :db.unique/identity
                :db/aid         email-aid}
               (get (d/schema conn) :user/email)))

        ;; An empty property patch is a no-op, and a patch may rely on a stored
        ;; property to satisfy whole-definition validation.
        (let [before (d/schema conn)]
          (is (= before
                 (d/update-schema conn {:user/email {}}))))
        (d/update-schema conn {:user/friend {:db/isComponent true}})
        (is (= {:db/valueType  :db.type/ref
                :db/isComponent true}
               (dissoc (get (d/schema conn) :user/friend) :db/aid)))

        ;; Property removal is explicit. Incoming internal attribute IDs are
        ;; ignored for both existing and new attributes.
        (d/update-schema conn
                         {:user/email {:db/unique :db/retract
                                       :db/aid    -1}
                          :user/age   {:db/valueType :db.type/long
                                       :db/aid       email-aid}})
        (is (= email-aid
               (get-in (d/schema conn) [:user/email :db/aid])))
        (is (not (contains? (get (d/schema conn) :user/email) :db/unique)))
        (is (not= email-aid
                  (get-in (d/schema conn) [:user/age :db/aid])))
        (is (= (count (d/schema conn))
               (count (set (map :db/aid (vals (d/schema conn)))))))

        ;; `schema` output, including :db/aid, is safe to feed back as input.
        (let [before (d/schema conn)]
          (is (= before (d/update-schema conn before))))

        ;; Retractions are validated against the resulting complete
        ;; definition, and a rejected patch leaves the schema unchanged.
        (let [before (d/schema conn)]
          (is (thrown-with-msg?
                Exception #"isComponent.*should also have.*ref"
                (d/update-schema
                  conn {:user/friend {:db/valueType :db/retract}})))
          (is (= before (d/schema conn)))))
      (finally
        (d/close conn)))

    ;; Opening an existing database with a schema also applies property maps as
    ;; patches instead of erasing omitted properties.
    (let [conn' (d/create-conn dir {:user/email {:db/doc "Primary email"}})]
      (try
        (is (= :db.type/string
               (get-in (d/schema conn') [:user/email :db/valueType])))
        (is (= :db.cardinality/one
               (get-in (d/schema conn') [:user/email :db/cardinality])))
        (is (= "Primary email"
               (get-in (d/schema conn') [:user/email :db/doc])))
        (is (not (contains? (get (d/schema conn') :user/email) :db/unique)))
        (finally
          (d/close conn')
          (u/delete-files dir))))))

(deftest test-update-schema-delete-and-rename-replays
  (let [dir    (u/tmp-dir (str "schema-replay-" (UUID/randomUUID)))
        schema {:old/name    {:db/valueType :db.type/string}
                :delete/me   {}
                :other/name  {:db/valueType :db.type/string}}
        conn   (d/create-conn dir schema)]
    (try
      (let [after-delete (d/update-schema conn nil #{:delete/me})
            modified     (i/last-modified (conn-store conn))]
        (is (not (contains? after-delete :delete/me)))
        (is (= after-delete
               (d/update-schema conn nil #{:delete/me})))
        (is (= modified (i/last-modified (conn-store conn)))))

      (let [aid       (get-in (d/schema conn) [:old/name :db/aid])
            update    {:old/name {:db/doc "Renamed attribute"}}
            renames   {:old/name :new/name}
            after     (d/update-schema conn update nil renames)
            modified  (i/last-modified (conn-store conn))]
        (is (not (contains? after :old/name)))
        (is (= aid (get-in after [:new/name :db/aid])))
        (is (= "Renamed attribute" (get-in after [:new/name :db/doc])))
        ;; Replaying the original request redirects its property patch to the
        ;; target instead of recreating the old attribute.
        (is (= after (d/update-schema conn update nil renames)))
        (is (= modified (i/last-modified (conn-store conn)))))

      (let [before (d/schema conn)]
        (is (thrown-with-msg?
              Exception #"target already exists"
              (d/update-schema conn nil nil {:new/name :other/name})))
        (is (= before (d/schema conn))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-update-schema-validates-whole-operation-before-writing
  (let [dir    (u/tmp-dir (str "schema-preflight-" (UUID/randomUUID)))
        schema {:safe/name  {:db/valueType :db.type/string}
                :busy/value {}}
        conn   (d/create-conn dir schema)]
    (try
      (d/transact! conn [{:db/id 1 :busy/value 42}])
      (let [before   (d/schema conn)
            modified (i/last-modified (conn-store conn))]
        (is (thrown-with-msg?
              Exception #"Cannot delete attribute"
              (d/update-schema conn
                               {:safe/name {:db/doc "must roll back"}}
                               #{:busy/value})))
        (is (= before (d/schema conn)))
        (is (= modified (i/last-modified (conn-store conn)))))

      (is (thrown-with-msg?
            Exception #"patch and delete"
            (d/update-schema conn {:safe/name {:db/doc "ambiguous"}}
                             #{:safe/name})))
      (is (thrown-with-msg?
            Exception #"chains and cycles"
            (d/update-schema conn nil nil
                             {:safe/name :middle/name
                              :middle/name :final/name})))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-update-schema-commit-failure-rolls-back-migration
  (let [dir  (u/tmp-dir (str "schema-commit-failure-" (UUID/randomUUID)))
        conn (d/create-conn dir {:item/value {}})]
    (try
      (d/transact! conn [{:db/id 1 :item/value 42}])
      (let [before (d/schema conn)]
        (is (thrown-with-msg?
              Exception #"forced schema commit failure"
              (binding [cpp/*before-write-commit-fn*
                        (fn [{:keys [operation]}]
                          (when (= operation :close-transact-kv)
                            (throw (ex-info "forced schema commit failure"
                                            {:error ::forced-commit-failure}))))]
                (d/update-schema
                  conn {:item/value {:db/valueType :db.type/string}}))))
        (is (= before (d/schema conn)))
        (is (= 42 (:item/value (d/entity @conn 1)))))

      ;; The identical operation can be retried after the failed commit.
      (d/update-schema conn {:item/value {:db/valueType :db.type/string}})
      (is (= :db.type/string
             (get-in (d/schema conn) [:item/value :db/valueType])))
      (is (= "42" (:item/value (d/entity @conn 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-concurrent-schema-property-patches-compose
  (let [dir     (u/tmp-dir (str "schema-concurrent-" (UUID/randomUUID)))
        conn    (d/create-conn
                  dir {:item/value {:db/valueType :db.type/string}})
        patches [{:db/doc "Concurrent patches"}
                 {:db/cardinality :db.cardinality/one}
                 {:db/fulltext false}
                 {:db/isComponent false}
                 {:db/embedding false}
                 {:db/idocFormat :edn}
                 {:db.embedding/autoDomain false}]
        ready   (CountDownLatch. (count patches))
        start   (CountDownLatch. 1)
        workers (mapv
                  (fn [patch]
                    (future
                      (.countDown ready)
                      (.await start)
                      (d/update-schema conn {:item/value patch})))
                  patches)]
    (try
      (is (.await ready 10 TimeUnit/SECONDS))
      (.countDown start)
      (doseq [worker workers]
        (is (not= ::timeout (deref worker 10000 ::timeout))))
      (let [props (get (d/schema conn) :item/value)]
        (doseq [patch patches]
          (is (= patch (select-keys props (keys patch))))))
      (finally
        (.countDown start)
        (doseq [worker workers]
          (future-cancel worker))
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-live-idoc-schema-initializes-index-and-logs-dbis
  (let [dir  (u/tmp-dir (str "test-live-idoc-schema-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}
        conn (d/create-conn dir nil opts)]
    (try
      (d/update-schema conn
                       {:doc/idoc {:db/valueType :db.type/idoc
                                   :db/domain "profiles"}})
      (let [lmdb (.-lmdb ^Store (conn-store conn))
            txlog-count-before (count (kv/open-tx-log lmdb 1))]
        (d/transact! conn [{:db/id 1
                            :doc/idoc {:status "active"}}
                           {:db/id 2
                            :doc/idoc {:status "active"}}])
        (let [records            (vec (kv/open-tx-log lmdb 1))
              new-records        (drop txlog-count-before records)
              idoc-write-records (filter #(or (txlog-record-has-dbi?
                                                % "profiles/doc-ref")
                                               (txlog-record-has-dbi?
                                                % "profiles/doc-index")
                                               (txlog-record-has-dbi?
                                                % "profiles/path-dict"))
                                          new-records)
              record             (first idoc-write-records)]
          (is (= 1 (count idoc-write-records)))
          (is (txlog-record-has-dbi? record c/eav))
          (is (txlog-record-has-dbi? record "profiles/doc-ref"))
          (is (txlog-record-has-dbi? record "profiles/doc-index"))
          (is (txlog-record-has-dbi? record "profiles/path-dict"))))
      (is (= {:status "active"}
             (:doc/idoc (d/entity @conn 1))))
      (is (= #{1 2}
             (set (d/q '[:find [?e ...]
                         :in $ ?query
                         :where
                         [(idoc-match $ :doc/idoc ?query)
                          [[?e ?a ?v]]]]
                       @conn {:status "active"}))))
      (let [index (get (s/store-idoc-indices (conn-store conn)) "profiles")]
        (is (instance? SpillableMap (.-doc-refs ^IdocIndex index))))
      (let [lmdb (.-lmdb ^Store (conn-store conn))
            ops  (txlog-ops lmdb)]
        (is (some? (txlog-dbi-registration-op ops
                                               :put
                                               "profiles/doc-ref")))
        (is (some? (txlog-dbi-registration-op ops
                                               :put
                                               "profiles/doc-index")))
        (is (some? (txlog-dbi-registration-op ops
                                              :put
                                              "profiles/path-dict"))))
      (d/close conn)
      (let [conn2 (d/create-conn dir nil opts)]
        (try
          (is (= {:status "active"}
                 (:doc/idoc (d/entity @conn2 1))))
          (finally
            (d/close conn2))))
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

(deftest test-idoc-schema-update-inside-explicit-transaction
  (let [dir  (u/tmp-dir (str "schema-idoc-transaction-"
                             (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (try
      (d/with-transaction [tx conn]
        (d/update-schema tx
                         {:doc/idoc {:db/valueType :db.type/idoc
                                     :db/domain    "profiles"}}))
      (is (some? (get (s/store-idoc-indices (conn-store conn)) "profiles")))
      (d/transact! conn [{:db/id 1 :doc/idoc {:status "active"}}])
      (is (= {:status "active"}
             (:doc/idoc (d/entity @conn 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-idoc-selective-path-indexing-domain-options
  (let [dir    (u/tmp-dir (str "test-idoc-selective-path-indexing-"
                               (UUID/randomUUID)))
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        opts   {:idoc-domains
                {"profiles" {:indexed-paths [:status :profile]
                             :excluded-paths [[:profile :secret]]}}}
        conn   (d/create-conn dir schema opts)
        q      '[:find [?e ...]
                 :in $ ?query
                 :where
                 [(idoc-match $ :doc/idoc ?query) [[?e ?a ?v]]]]]
    (try
      (d/transact! conn [{:db/id 1
                          :doc/idoc {:status  "active"
                                     :profile {:age 30
                                               :secret "hidden"}
                                     :tags    ["a"]}}
                         {:db/id 2
                          :doc/idoc {:status  "inactive"
                                     :profile {:age 40
                                               :secret "hidden"}
                                     :tags    ["b"]}}])
      (is (= #{1} (set (d/q q @conn {:status "active"}))))
      (is (= #{1} (set (d/q q @conn {:profile {:age 30}}))))
      (is (empty? (d/q q @conn {:profile {:secret "hidden"}})))
      (is (empty? (d/q q @conn {:tags "a"})))
      (let [store    (conn-store conn)
            lmdb     (.-lmdb ^Store store)
            index    (get (s/store-idoc-indices store) "profiles")
            path-dbi (.-path-dict-dbi ^IdocIndex index)
            path-id  #(i/get-value lmdb path-dbi % :string :int)]
        (is (some? (path-id "/:status")))
        (is (some? (path-id "/:profile/:age")))
        (is (nil? (path-id "/:profile/:secret")))
        (is (nil? (path-id "/:tags"))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-idoc-selective-path-indexing-schema-options
  (let [dir    (u/tmp-dir (str "test-idoc-selective-schema-path-indexing-"
                               (UUID/randomUUID)))
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain "profiles"
                           :db.idoc/indexedPaths [:status]}}
        conn   (d/create-conn dir schema)
        q      '[:find [?e ...]
                 :in $ ?query
                 :where
                 [(idoc-match $ :doc/idoc ?query) [[?e ?a ?v]]]]]
    (try
      (d/transact! conn [{:db/id 1
                          :doc/idoc {:status "active"
                                     :profile {:age 30}}}])
      (is (= #{1} (set (d/q q @conn {:status "active"}))))
      (is (empty? (d/q q @conn {:profile {:age 30}})))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-giant-idoc-patch-updates-false-index
  (let [schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        conn   (d/create-conn nil schema
                              {:wal? false
                               :kv-opts {:inmemory? true :wal? false}})
        big    (apply str (repeat (+ c/+val-bytes-wo-hdr+ 100) "x"))
        q      '[:find [?e ...]
                 :in $ ?query
                 :where
                 [(idoc-match $ :doc/idoc ?query) [[?e ?a ?v]]]]
        q-attr '[:find [?a ...]
                 :in $ ?query
                 :where
                 [(idoc-match $ :doc/idoc ?query) [[?e ?a ?v]]]]
        q-doc  '[:find [?v ...]
                 :in $ ?query
                 :where
                 [(idoc-match $ :doc/idoc ?query) [[?e ?a ?v]]]]]
    (try
      (let [doc {:active false :kind :entry :bio big}]
        (d/transact! conn [{:db/id 1 :doc/idoc doc}])
        (let [index (get (s/store-idoc-indices (conn-store conn)) "profiles")
              ref   (-> ^SpillableMap (.-doc-refs ^IdocIndex index)
                        seq first val)]
          (is (= [:g 1] [(first ref) (nth ref 2)]))
          (is (int? (nth ref 3))))
        (is (= [1] (d/q q @conn {:active false})))
        (is (= [:doc/idoc] (d/q q-attr @conn {:active false})))
        (is (= [doc] (d/q q-doc @conn {:active false})))
        (is (= [doc]
               (d/q q-doc @conn {:active false :kind :entry}))))
      (d/transact! conn
                   [[:db.fn/patchIdoc 1 :doc/idoc
                     [[:set [:active] true]]]])
      (is (empty? (d/q q @conn {:active false})))
      (is (= [1] (d/q q @conn {:active true})))
      (d/transact! conn [[:db.fn/retractAttribute 1 :doc/idoc]])
      (is (empty? (d/q q @conn {:active true})))
      (finally
        (d/close conn)))))

(deftest test-giant-vector-projected-neighbors
  (let [dimensions 128
        value      (float-array
                     (map #(float (/ (double %) (double dimensions)))
                          (range dimensions)))
        conn       (d/create-conn
                     nil
                     {:embedding {:db/valueType :db.type/vec}}
                     {:wal?        false
                      :vector-opts {:dimensions dimensions
                                    :metric-type :cosine}
                      :kv-opts     {:inmemory? true :wal? false}})
        q-e        '[:find [?e ...]
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query {:top 1})
                      [[?e _ _]]]]
        q-a        '[:find [?a ...]
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query {:top 1})
                      [[_ ?a _]]]]
        q-v        '[:find [?v ...]
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query {:top 1})
                      [[_ _ ?v]]]]
        q-ed       '[:find ?e ?dist
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query
                                     {:top 1 :display :refs+dists})
                      [[?e _ _ ?dist]]]]
        q-all      '[:find ?e ?a ?v ?dist
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query
                                     {:top 1 :display :refs+dists})
                      [[?e ?a ?v ?dist]]]]]
    (try
      (d/transact! conn [{:db/id 1 :embedding value}])
      (let [store (conn-store conn)
            index (get (.-vector-indices ^Store store) "embedding")
            ref   (-> ^SpillableMap (.-vecs ^VectorIndex index)
                      seq first val)]
        (is (= [:g 1] [(first ref) (nth ref 2)]))
        (is (int? (nth ref 3)))
        (is (empty? (d/search-vec index value
                                  {:top 1 :vec-filter (constantly false)}))))
      (is (= [1] (d/q q-e @conn value)))
      (is (= [:embedding] (d/q q-a @conn value)))
      (is (= [(vec value)] (mapv vec (d/q q-v @conn value))))
      (let [[e dist] (first (d/q q-ed @conn value))]
        (is (= 1 e))
        (is (number? dist)))
      (let [[e a v dist] (first (d/q q-all @conn value))]
        (is (= 1 e))
        (is (= :embedding a))
        (is (= (vec value) (vec v)))
        (is (number? dist)))
      (d/transact! conn [[:db/retract 1 :embedding value]])
      (is (empty? (d/q q-e @conn value)))
      (finally
        (d/close conn)))))

(deftest test-giant-cardinality-many-vector-neighbors
  (let [dimensions 128
        value-1    (float-array dimensions)
        value-2    (float-array dimensions)
        _          (dotimes [i dimensions]
                     (aset-float value-1 i
                                 (float (/ (double (unchecked-inc-int i))
                                           (double dimensions))))
                     (aset-float value-2 i
                                 (float (/ (double (- dimensions i))
                                           (double dimensions)))))
        conn       (d/create-conn
                     nil
                     {:embedding {:db/valueType   :db.type/vec
                                  :db/cardinality :db.cardinality/many}}
                     {:wal?        false
                      :vector-opts {:dimensions dimensions
                                    :metric-type :cosine}
                      :kv-opts     {:inmemory? true :wal? false}})
        q-v        '[:find [?v ...]
                     :in $ ?query
                     :where
                     [(vec-neighbors $ :embedding ?query {:top 2})
                      [[_ _ ?v]]]]]
    (try
      (d/transact! conn [[:db/add 1 :embedding value-1]
                         [:db/add 1 :embedding value-2]])
      (let [store (conn-store conn)
            index (get (.-vector-indices ^Store store) "embedding")
            refs  (mapv val (seq (.-vecs ^VectorIndex index)))]
        (is (= 2 (:size (d/vector-index-info index))))
        (is (= 2 (count (set (map second refs)))))
        (is (every? #(and (= :g (first %))
                          (= 1 (nth % 2))
                          (int? (nth % 3)))
                    refs))
        (is (= #{(vec value-1) (vec value-2)}
               (set (map vec (d/q q-v @conn value-1)))))
        (d/transact! conn [[:db/retract 1 :embedding value-1]])
        (is (= 1 (:size (d/vector-index-info index))))
        (is (= #{(vec value-2)}
               (set (map vec (d/q q-v @conn value-1))))))
      (finally
        (d/close conn)))))

(deftest test-giant-cardinality-many-fulltext-projections
  (let [suffix     (apply str (repeat 700 "x"))
        value-1    (str "alpha first " suffix)
        value-2    (str "alpha second " suffix)
        conn       (d/create-conn
                     nil
                     {:body {:db/valueType           :db.type/string
                             :db/cardinality         :db.cardinality/many
                             :db/fulltext            true
                             :db.fulltext/autoDomain true}}
                     {:wal?           false
                      :kv-opts        {:inmemory? true :wal? false}
                      :search-domains {"body" {:index-position? true
                                                :include-text?   true}}})
        q-e        '[:find [?e ...]
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query) [[?e]]]]
        q-a        '[:find [?a ...]
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query) [[_ ?a]]]]
        q-eav      '[:find ?e ?a ?v
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query) [[?e ?a ?v]]]]
        q-score    '[:find [?score ...]
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query {:display :refs+scores})
                      [[_ _ _ ?score]]]]
        q-text     '[:find [?text ...]
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query {:display :texts})
                      [[_ _ _ ?text]]]]
        q-offsets  '[:find [?offsets ...]
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query {:display :offsets})
                      [[_ _ _ ?offsets]]]]
        q-text+off '[:find ?text ?offsets
                     :in $ ?query
                     :where
                     [(fulltext $ :body ?query {:display :texts+offsets})
                      [[_ _ _ ?text ?offsets]]]]]
    (try
      (d/transact! conn [[:db/add 1 :body value-1]
                         [:db/add 1 :body value-2]])
      (let [engine (conn-search-engine conn "body")
            refs   (mapv val
                         (seq (.-docs ^datalevin.search.SearchEngine engine)))]
        (is (= 2 (count refs)))
        (is (= 2 (count (set (map second refs)))))
        (is (every? #(and (= :g (first %))
                          (= 1 (nth % 2))
                          (int? (nth % 3)))
                    refs)))
      (is (= [1] (d/q q-e @conn "alpha")))
      (is (= [:body] (d/q q-a @conn "alpha")))
      (is (= #{[1 :body value-1] [1 :body value-2]}
             (set (d/q q-eav @conn "alpha"))))
      (is (every? number? (d/q q-score @conn "alpha")))
      (is (= #{value-1 value-2} (set (d/q q-text @conn "alpha"))))
      (doseq [offsets (d/q q-offsets @conn "alpha")]
        (is (= [0] (get (into {} offsets) "alpha"))))
      (let [results (d/q q-text+off @conn "alpha")]
        (is (= #{value-1 value-2} (set (map first results))))
        (doseq [[_ offsets] results]
          (is (= [0] (get (into {} offsets) "alpha")))))
      (d/transact! conn [[:db/retract 1 :body value-1]])
      (is (= #{[1 :body value-2]} (set (d/q q-eav @conn "alpha"))))
      (finally
        (d/close conn)))))

(deftest test-legacy-giant-fulltext-reference
  (let [dir    (u/tmp-dir (str "test-legacy-giant-fulltext-"
                               (UUID/randomUUID)))
        value  (str "legacy searchable text "
                    (apply str (repeat 700 "x")))
        value' (str "replacement searchable text "
                    (apply str (repeat 700 "y")))
        schema {:body {:db/valueType           :db.type/string
                       :db/fulltext            true
                       :db.fulltext/autoDomain true}}
        opts   {:wal? false :kv-opts {:wal? false}}
        query  '[:find [?e ...]
                 :in $ ?query
                 :where
                 [(fulltext $ :body ?query) [[?e]]]]]
    (try
      (let [conn (d/create-conn dir schema opts)]
        (try
          (d/transact! conn [{:db/id 1 :body value}])
          (let [engine (conn-search-engine conn "body")
                ref    (-> (.-docs ^datalevin.search.SearchEngine engine)
                           seq first val)
                legacy-ref [:g (second ref)]]
            (is (= 4 (count ref)))
            (i/remove-doc engine ref)
            (i/add-doc engine legacy-ref value false)
            (is (= [legacy-ref]
                   (vec (i/search engine "searchable" {:top 1})))))
          (finally
            (d/close conn))))
      (let [conn (d/create-conn dir nil opts)]
        (try
          (is (= [1] (d/q query @conn "searchable")))
          (d/transact! conn [{:db/id 1 :body value'}])
          (is (empty? (d/q query @conn "legacy")))
          (is (= [1] (d/q query @conn "replacement")))
          (d/transact! conn [[:db/retract 1 :body value']])
          (is (empty? (d/q query @conn "replacement")))
          (is (zero? (i/doc-count (conn-search-engine conn "body"))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-idoc-and-fulltext-fuzz-in-same-tx
  (doseq [seed [7 29 113]]
    (let [dir    (u/tmp-dir (str "test-idoc-fulltext-fuzz-"
                                 seed "-"
                                 (UUID/randomUUID)))
          schema (idoc-fulltext-schema)
          opts   (idoc-fulltext-opts)
          conn   (d/create-conn dir schema opts)
          rng    (Random. seed)]
      (try
        (let [final-oracle
              (loop [step   0
                     oracle {}]
                (if (< step 16)
                  (let [[actions oracle'] (idoc-fulltext-fuzz-actions
                                            rng step oracle)
                        tx-data           (mapcat :tx actions)
                        status            (pick rng
                                                idoc-fulltext-fuzz-statuses)
                        token             (pick rng
                                                idoc-fulltext-fuzz-tokens)]
                    (try
                      (d/transact! conn tx-data)
                      (verify-idoc-fulltext-oracle! @conn oracle' seed step)
                      (verify-idoc-fulltext-combined!
                        @conn oracle' seed step status token)
                      (catch Throwable t
                        (throw
                          (ex-info "idoc/fulltext fuzz transaction failed"
                                   {:seed    seed
                                    :step    step
                                    :actions actions}
                                   t))))
                    (recur (inc step) oracle'))
                  oracle))]
          (d/close conn)
          (let [conn2 (d/create-conn dir nil opts)]
            (try
              (verify-idoc-fulltext-oracle! @conn2 final-oracle seed :reopen)
              (doseq [status idoc-fulltext-fuzz-statuses
                      token  idoc-fulltext-fuzz-tokens]
                (verify-idoc-fulltext-combined!
                  @conn2 final-oracle seed :reopen status token))
              (finally
                (d/close conn2)))))
        (finally
          (when-not (d/closed? conn)
            (d/close conn))
          (u/delete-files dir))))))

(deftest test-idoc-and-fulltext-property-in-same-tx
  (let [result (tc/quick-check
                 40
                 (prop/for-all [scenario idoc-fulltext-property-scenario-gen]
                   (run-idoc-fulltext-property-scenario! scenario))
                 {:seed     375
                  :max-size 40})]
    (is (:pass? result)
        (pr-str (select-keys result
                             [:seed :num-tests :fail :result :shrunk])))))

(deftest test-search-query-term-info-uses-off-heap-bitmap
  (let [dir             (u/tmp-dir (str "test-search-offheap-term-info-"
                                        (UUID/randomUUID)))
        db              (d/open-kv dir)
        engine          (d/new-search-engine db {:index-position? true})
        query-term-info (deref #'search/query-term-info)]
    (try
      (dotimes [i 8]
        (d/add-doc engine (inc i) (str "alpha beta " i)))
      (is (= #{1 2 3 4 5 6 7 8}
             (set (d/search engine "alpha" {:top 10}))))
      (let [[_ _ sl] (query-term-info engine "alpha")
            indices  (.-indices ^SparseIntArrayList sl)]
        (is (instance? ImmutableRoaringBitmap indices))
        (is (= 8 (sl/size sl)))
        (is (= 1 (sl/get sl 1)))
        (is (= 1 (sl/get sl 8))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(deftest test-batched-fulltext-secondary-index
  (let [dir    (u/tmp-dir (str "test-batched-fulltext-secondary-index-"
                               (UUID/randomUUID)))
        n      1025
        schema {:doc/text {:db/valueType        :db.type/string
                           :db/fulltext          true
                           :db.fulltext/domains ["docs"]}}
        opts   {:wal? false
                :kv-opts {:wal? false}
                :search-domains
                {"docs" {:index-position? true
                         :include-text?   true}}}
        text   (fn [i] (str "alpha beta document" i))
        conn   (d/create-conn dir schema opts)]
    (try
      (d/transact! conn
                   (mapv (fn [i]
                           {:db/id i :doc/text (text i)})
                         (range 1 (inc n))))
      (let [engine (conn-search-engine conn "docs")]
        (is (= n (i/doc-count engine)))
        (is (= n (count (i/search engine "alpha" {:top n}))))
        (is (= n (count (i/search engine {:phrase "alpha beta"}
                                  {:top n}))))
        (is (= "alpha beta document1"
               (-> (i/search engine "document1" {:display :texts})
                   first
                   peek))))
      (d/transact! conn
                   (mapv (fn [i]
                           {:db/id i
                            :doc/text (str "gamma delta replacement" i)})
                         (range 1 (inc n))))
      (let [engine (conn-search-engine conn "docs")]
        (is (empty? (i/search engine "alpha" {:top n})))
        (is (= n (count (i/search engine "gamma" {:top n})))))
      (d/transact! conn [[:db.fn/retractAttribute 2 :doc/text]
                         [:db.fn/retractAttribute 3 :doc/text]])
      (d/close conn)
      (let [conn2 (d/create-conn dir nil opts)]
        (try
          (let [engine (conn-search-engine conn2 "docs")]
            (is (= (- n 2) (i/doc-count engine)))
            (is (empty? (i/search engine "alpha" {:top n})))
            (is (= (- n 2)
                   (count (i/search engine "gamma" {:top n}))))
            (is (= 1 (count (i/search engine "replacement1"))))
            (is (empty? (i/search engine "replacement2"))))
          (finally
            (d/close conn2))))
      (finally
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

(deftest test-batched-fulltext-delete-recomputes-max-weight
  (let [schema {:doc/text {:db/valueType :db.type/string
                           :db/fulltext   true}}
        opts   {:wal? false
                :kv-opts {:inmemory? true :wal? false}}
        heavy  (str (str/join " " (repeat 10000 "alpha"))
                    " bravo charlie delta echo foxtrot golf hotel india juliet")
        conn   (d/create-conn nil schema opts)]
    (try
      (d/transact! conn [{:db/id 1 :doc/text heavy}
                         {:db/id 2 :doc/text "alpha bravo charlie"}
                         {:db/id 3 :doc/text "alpha bravo charlie delta"}])
      (let [engine    (conn-search-engine conn c/default-domain)
            term-info #((deref #'search/query-term-info) engine "alpha")]
        (d/transact! conn [[:db.fn/retractAttribute 1 :doc/text]])
        (let [[_ max-weight postings] (term-info)]
          (is (< (Math/abs (- (double max-weight) (/ 1.0 3.0))) 1.0e-6))
          (is (= 2 (sl/size postings))))
        (d/transact! conn [{:db/id 1 :doc/text heavy}])
        (d/transact! conn [[:db.fn/retractAttribute 1 :doc/text]
                           [:db.fn/retractAttribute 3 :doc/text]])
        (let [[_ max-weight postings] (term-info)]
          (is (< (Math/abs (- (double max-weight) (/ 1.0 3.0))) 1.0e-6))
          (is (= 1 (sl/size postings)))
          (is (= 2 (-> (i/search engine "alpha" {:top 1}) ffirst)))))
      (finally
        (d/close conn)))))

(deftest test-search-pagination
  (let [dir    (u/tmp-dir (str "test-search-pagination-"
                               (UUID/randomUUID)))
        db     (d/open-kv dir)
        engine (d/new-search-engine db {:index-position? true})]
    (try
      (dotimes [i 8]
        (d/add-doc engine (inc i) (str "alpha beta " i)))
      (let [all-results (vec (d/search engine "alpha" {:top 8}))
            window      (vec (d/search engine "alpha" {:top 5}))
            page        (vec (d/search engine "alpha"
                                       {:limit 3 :offset 2}))
            one-window-page
            (vec (d/search engine "alpha"
                           {:limit              3
                            :offset             2
                            :paging-cache-pages 1}))
            score-page  (vec (d/search engine "alpha"
                                       {:display :refs+scores
                                        :limit   3
                                        :offset  2}))
            score-window
            (vec (d/search engine "alpha"
                           {:display :refs+scores
                            :top     8}))]
        (is (= 8 (count all-results)))
        (is (= (subvec all-results 2 5) page))
        (is (= (subvec window 2 5) one-window-page))
        (is (= (subvec score-window 2 5) score-page))
        (is (empty? (d/search engine "alpha" {:limit 0 :offset 2}))))
      (d/search engine "alpha" {:limit 3})
      (d/add-doc engine 9 "alpha alpha alpha alpha")
      (is (= 9 (first (d/search engine "alpha" {:limit 3}))))
      (finally
        (d/close-kv db)
        (u/delete-files dir)))))

(defn- parallel-add-doc-errors
  [engine docs]
  (let [start   (promise)
        workers (doall
                  (for [[doc-ref doc-text] docs]
                    (future
                      @start
                      (try
                        (d/add-doc engine doc-ref doc-text)
                        nil
                        (catch Throwable t
                          {:doc-ref doc-ref
                           :type    (class t)
                           :message (.getMessage t)})))))]
    (deliver start true)
    (doall (remove nil? (map deref workers)))))

(deftest test-search-add-doc-parallel-does-not-throw
  (testing "distinct document refs"
    (let [dir    (u/tmp-dir (str "test-search-parallel-add-docs-"
                                 (UUID/randomUUID)))
          db     (d/open-kv dir)
          engine (d/new-search-engine db {:index-position? true})
          docs   (mapv (fn [i] [i (str "alpha beta " i)]) (range 64))]
      (try
        (let [errors (parallel-add-doc-errors engine docs)]
          (is (empty? errors) (pr-str errors))
          (is (= 64 (d/doc-count engine)))
          (is (= (set (map first docs))
                 (set (d/search engine "alpha" {:top 100})))))
        (finally
          (d/close-kv db)
          (u/delete-files dir)))))
  (testing "same document ref replacement"
    (let [dir    (u/tmp-dir (str "test-search-parallel-replace-doc-"
                                 (UUID/randomUUID)))
          db     (d/open-kv dir)
          engine (d/new-search-engine db {:index-position? true})
          docs   (mapv (fn [i] [:shared (str "alpha beta " i)])
                       (range 32))]
      (try
        (let [errors (parallel-add-doc-errors engine docs)]
          (is (empty? errors) (pr-str errors))
          (is (= 1 (d/doc-count engine)))
          (is (= [:shared] (d/search engine "alpha" {:top 10}))))
        (finally
          (d/close-kv db)
          (u/delete-files dir))))))

(deftest test-idoc-match-concurrent-read-write-sidecar-state
  (let [dir    (u/tmp-dir (str "test-idoc-concurrent-sidecar-"
                               (UUID/randomUUID)))
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        conn   (d/create-conn dir schema)
        stop?  (promise)]
    (try
      (d/transact! conn
                   (mapv (fn [i]
                           {:db/id    i
                            :doc/idoc {:status "active" :n i}})
                         (range 1 21)))
      (let [query     '[:find [?e ...]
                        :in $ ?query
                        :where
                        [(idoc-match $ :doc/idoc ?query)
                         [[?e ?a ?v]]]]
            errors    (atom [])
            record!   (fn [^Throwable t]
                        (swap! errors conj t)
                        (deliver stop? true))
            read-once #(do (d/q query @conn {}) nil)
            readers   (doall
                        (for [_ (range 4)]
                          (future
                            (try
                              (while (not (realized? stop?))
                                (read-once))
                              :done
                              (catch Throwable t
                                (record! t)
                                :error)))))
            writer    (future
                        (try
                          (dotimes [i 40]
                            (d/transact!
                              conn
                              [{:db/id    1
                                :doc/idoc {:status (if (even? i)
                                                     "active"
                                                     "inactive")
                                           :n      i}}])
                            (let [eid (+ 1000 i)]
                              (d/transact!
                                conn
                                [{:db/id    eid
                                  :doc/idoc {:status "active" :n i}}])
                              (when (pos? i)
                                (d/transact!
                                  conn
                                  [[:db/retractEntity (dec eid)]]))))
                          :done
                          (catch Throwable t
                            (record! t)
                            :error)))]
        (let [writer-result (deref writer 30000 ::timeout)]
          (deliver stop? true)
          (when (= ::timeout writer-result)
            (future-cancel writer))
          (let [reader-results (doall
                                 (map #(deref % 30000 ::timeout) readers))]
            (doseq [[reader result] (map vector readers reader-results)
                    :when (= ::timeout result)]
              (future-cancel reader))
            (is (not= ::timeout writer-result))
            (is (not-any? #{::timeout} reader-results))
            (is (empty? @errors)
                (pr-str (mapv str @errors))))))
      (finally
        (deliver stop? true)
        (when-not (d/closed? conn)
          (d/close conn))
        (u/delete-files dir)))))

(deftest test-idoc-match-nil-candidate-subexpressions
  (let [dir    (u/tmp-dir (str "test-idoc-nil-candidates-"
                               (UUID/randomUUID)))
        schema {:doc/idoc {:db/valueType :db.type/idoc
                           :db/domain    "profiles"}}
        conn   (d/create-conn dir schema)]
    (try
      (d/transact! conn
                   [{:db/id    1
                     :doc/idoc {:status  "active"
                                :deleted false
                                :a       1
                                :b       {:x 1}}}
                    {:db/id    2
                     :doc/idoc {:status  "active"
                                :deleted true
                                :a       2}}
                    {:db/id    3
                     :doc/idoc {:status  "inactive"
                                :deleted false
                                :a       1}}])
      (let [match-eids (fn [query]
                         (set (d/q '[:find [?e ...]
                                      :in $ ?query
                                      :where
                                      [(idoc-match $ :doc/idoc ?query)
                                       [[?e ?a ?v]]]]
                                    @conn
                                    query)))]
        (is (= #{1}
               (match-eids {:status  "active"
                            :deleted [:not true]})))
        (is (= #{1 2}
               (match-eids [:or {:a 2}
                            [:not {:status "inactive"}]])))
        (is (= #{1}
               (match-eids {:a 1
                            :b {}})))
        (is (= #{1 2}
               (match-eids {:status "active"
                            :empty  [:and]})))
        (is (= #{}
               (match-eids {:status "active"
                            :empty  [:or]}))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-idoc-match-caches-are-bounded
  (binding [c/idoc-pattern-cache-size 2
            c/idoc-range-cache-size   2]
    (let [dir    (u/tmp-dir (str "test-idoc-cache-bounds-"
                                 (UUID/randomUUID)))
          schema {:doc/idoc {:db/valueType :db.type/idoc
                             :db/domain    "profiles"}}
          conn   (d/create-conn dir schema)]
      (try
        (d/transact!
          conn
          [{:db/id 1
            :doc/idoc (into {}
                            (for [i (range 8)]
                              [(keyword (str "facts" i)) {:v i}]))}])
        (let [match-eids (fn [query]
                           (set (d/q '[:find [?e ...]
                                        :in $ ?query
                                        :where
                                        [(idoc-match $ :doc/idoc ?query)
                                         [[?e ?a ?v]]]]
                                      @conn
                                      query)))]
          (doseq [i (range 8)]
            (is (= #{1}
                   (match-eids
                     (list '>= [(keyword (str "facts" i)) :?] 0))))))
        (let [index (get (s/store-idoc-indices (conn-store conn)) "profiles")]
          (is (<= (count (.keys ^LRUCache (.-pattern-cache ^IdocIndex index)))
                  c/idoc-pattern-cache-size))
          (is (<= (count (.keys ^LRUCache (.-range-cache ^IdocIndex index)))
                  c/idoc-range-cache-size)))
        (finally
          (d/close conn)
          (u/delete-files dir))))))

(deftest test-ways-to-create-conn-1
  (let [dir  (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn (d/create-conn dir)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= c/implicit-schema (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-source-schema-ignores-aids
  (let [dir  (u/tmp-dir (str "source-schema-aid-" (UUID/randomUUID)))
        conn (d/create-conn dir
                            {:source/one {:db/valueType :db.type/string
                                          :db/aid       0}
                             :source/two {:db/aid 0}})]
    (try
      (let [schema (d/schema conn)
            aid-1  (get-in schema [:source/one :db/aid])
            aid-2  (get-in schema [:source/two :db/aid])]
        (is (not= 0 aid-1))
        (is (not= 0 aid-2))
        (is (not= aid-1 aid-2))
        (is (= :db.type/string
               (get-in schema [:source/one :db/valueType]))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest test-ways-to-create-conn-2
  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (is (= #{} (set (d/datoms @conn :eav))))
    (is (= (db/-schema @conn) (merge schema c/implicit-schema)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-ways-to-create-conn-3
  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-datoms datoms dir schema)]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (d/init-db datoms dir))]
    (is (thrown-with-msg? Exception
                          #"init-db expects list of Datoms, got "
                          (d/init-db [[:add -1 :name "Ivan"]
                                      {:add -1 :age 35}])))
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir))

  (let [schema { :aka { :db/cardinality :db.cardinality/many
                        :db/aid         (count c/implicit-schema)}}
        datoms #{(d/datom 1 :age  17)
                 (d/datom 1 :name "Ivan")
                 (d/datom 1 :aka "danger")
                 (d/datom 1 :aka "fun")}
        dir    (u/tmp-dir (str "test-" (UUID/randomUUID)))
        conn   (d/conn-from-db (-> (d/empty-db dir schema)
                                   (d/fill-db datoms)))]
    (is (= datoms (set (d/datoms @conn :eav))))
    (is (= (d/schema conn) (db/-schema @conn)))
    (d/close conn)
    (u/delete-files dir)))

(deftest test-recreate-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "recreate-conn-test-" (UUID/randomUUID)))
        conn   (d/create-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/create-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-open-kv-enables-virtual-thread-safe-reader-slots
  (let [dir (u/tmp-dir (str "open-kv-flags-test-" (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir)]
        (try
          (is (= c/default-env-flags (d/get-env-flags db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-without-value-compression
  (let [dir (u/tmp-dir (str "with-tx-kv-no-compress-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (let [tx (i/open-transact-kv db)]
            (try
              (is (= :transacted
                     (d/transact-kv tx [[:put "a" :k :v]])))
              (is (= :v (d/get-value tx "a" :k)))
              (is (nil? (d/get-value db "a" :k)))
              (finally
                (i/close-transact-kv db))))
          (is (= :v (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-aborts-on-exception
  (let [dir (u/tmp-dir (str "with-tx-kv-abort-on-error-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"boom"
               (d/with-transaction-kv [tx db]
                 (d/transact-kv tx [[:put "a" :k :v]])
                 (throw (ex-info "boom" {})))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :after :ok]])))
          (is (= :ok (d/get-value db "a" :after)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-times-out
  (let [dir (u/tmp-dir (str "with-tx-kv-timeout-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"Explicit transaction timed out"
               (d/with-transaction-kv [tx db {:timeout-ms 50}]
                 (d/transact-kv tx [[:put "a" :k :v]])
                 (Thread/sleep 500))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :after :ok]])))
          (is (= :ok (d/get-value db "a" :after)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-explicit-transaction-timeout-default
  (let [dir      (u/tmp-dir (str "explicit-tx-timeout-default-test-"
                                 (UUID/randomUUID)))
        previous (d/explicit-transaction-timeout)]
    (try
      (let [db (d/open-kv dir {:wal? false})]
        (try
          (d/open-dbi db "a")
          (d/set-explicit-transaction-timeout! 50)
          (is (= 50 (d/explicit-transaction-timeout)))
          (is (thrown-with-msg?
               Exception
               #"Explicit transaction timed out"
               (d/with-transaction-kv [tx db]
                 (d/transact-kv tx [[:put "a" :k :v]])
                 (Thread/sleep 500))))
          (is (nil? (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (d/set-explicit-transaction-timeout! previous)
        (u/delete-files dir)))))

(deftest test-with-transaction-kv-evaluates-db-once
  (let [dir (u/tmp-dir (str "with-tx-kv-eval-once-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db    (d/open-kv dir {:wal? false})
            calls (atom 0)
            get-db
            (fn []
              (swap! calls inc)
              db)]
        (try
          (d/open-dbi db "a")
          (is (= 1
                 (d/with-transaction-kv [tx (get-db)]
                   (d/transact-kv tx [[:put "a" :k 1]])
                   (d/get-value tx "a" :k))))
          (is (= 1 @calls))
          (is (= 1 (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-aborts-on-exception
  (let [dir    (u/tmp-dir (str "with-tx-abort-on-error-test-"
                               (UUID/randomUUID)))
        schema {:name {:db/valueType :db.type/string}}]
    (try
      (let [conn (d/create-conn dir schema {:wal? false})]
        (try
          (is (thrown-with-msg?
               Exception
               #"boom"
               (d/with-transaction [tx conn]
                 (d/transact! tx [{:db/id 1 :name "partial"}])
                 (throw (ex-info "boom" {})))))
          (is (empty?
               (d/q '[:find [?e ...]
                      :where [?e :name "partial"]]
                    @conn)))
          (d/transact! conn [{:db/id 2 :name "after"}])
          (is (= #{2}
                 (set (d/q '[:find [?e ...]
                             :where [?e :name "after"]]
                           @conn))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-times-out
  (let [dir    (u/tmp-dir (str "with-tx-timeout-test-"
                               (UUID/randomUUID)))
        schema {:name {:db/valueType :db.type/string}}]
    (try
      (let [conn (d/create-conn dir schema {:wal? false})]
        (try
          (is (thrown-with-msg?
               Exception
               #"Explicit transaction timed out"
               (d/with-transaction [tx conn {:timeout-ms 50}]
                 (d/transact! tx [{:db/id 1 :name "partial"}])
                 (Thread/sleep 500))))
          (is (empty?
               (d/q '[:find [?e ...]
                      :where [?e :name "partial"]]
                    @conn)))
          (d/transact! conn [{:db/id 2 :name "after"}])
          (is (= #{2}
                 (set (d/q '[:find [?e ...]
                             :where [?e :name "after"]]
                           @conn))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-transaction-evaluates-conn-once
  (let [dir    (u/tmp-dir (str "with-tx-eval-once-test-"
                               (UUID/randomUUID)))
        schema {:counter {:db/valueType :db.type/long}}]
    (try
      (let [conn  (d/create-conn dir schema {:wal? false})
            calls (atom 0)
            get-conn
            (fn []
              (swap! calls inc)
              conn)]
        (try
          (is (= 1
                 (d/with-transaction [tx (get-conn)]
                   (d/transact! tx [{:db/id 1 :counter 1}])
                   (d/q '[:find ?c .
                          :where [1 :counter ?c]]
                        @tx))))
          (is (= 1 @calls))
          (is (= 1
                 (d/q '[:find ?c .
                        :where [1 :counter ?c]]
                      @conn)))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-open-kv-rejects-second-local-handle-in-process
  (let [dir (u/tmp-dir (str "open-kv-duplicate-handle-test-"
                            (UUID/randomUUID)))]
    (try
      (let [db1 (d/open-kv dir)]
        (try
          (d/open-dbi db1 "a")
          (d/transact-kv db1 [[:put "a" :k :v]])
          (let [result (try
                         (let [db2 (d/open-kv dir)]
                           (try
                             {:status :opened}
                             (finally
                               (d/close-kv db2))))
                         (catch Exception e
                           {:status  :error
                            :message (ex-message e)}))]
            (is (= :error (:status result)) result)
            (is (re-find #"Please do not open multiple LMDB connections"
                         (:message result)))
            (is (= :v
                   (d/get-value db1 "a" :k))))
          (finally
            (d/close-kv db1))))
      (let [db2 (d/open-kv dir)]
        (try
          (d/open-dbi db2 "a")
          (is (= :v
                 (d/get-value db2 "a" :k)))
          (finally
            (d/close-kv db2))))
      (finally
        (u/delete-files dir)))))

(deftest test-get-conn
  (let [schema {:name          {:db/valueType :db.type/string}
                :dt/updated-at {:db/valueType :db.type/instant}}
        dir    (u/tmp-dir (str "get-conn-test-" (UUID/randomUUID)))
        conn   (d/get-conn dir schema)]
    (d/transact! conn [{:db/id         -1
                        :name          "Namebo"
                        :dt/updated-at (Date.)}])
    (d/close conn)

    (let [conn2 (d/get-conn dir schema)]
      (d/transact! conn2 [{:db/id         -2
                           :name          "Another name"
                           :dt/updated-at (Date.)}])
      (is (= 4 (count (d/datoms @conn2 :eav))))
      (d/close conn2))
    (u/delete-files dir)))

(deftest test-get-conn-existing-store-opens-lmdb-once
  (let [schema {:name {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "get-conn-open-once-test-" (UUID/randomUUID)))]
    (try
      (let [conn (d/get-conn dir schema)]
        (try
          (d/transact! conn [{:db/id -1 :name "Namebo"}])
          (finally
            (d/close conn))))
      (let [open-count (atom 0)
            orig-open  lmdb/open-kv]
        (with-redefs [lmdb/open-kv (fn
                                     ([dir]
                                      (swap! open-count inc)
                                      (orig-open dir))
                                     ([dir opts]
                                      (swap! open-count inc)
                                      (orig-open dir opts)))]
          (let [conn (d/get-conn dir schema)]
            (try
              (is (= 1 @open-count))
              (is (= 1 (d/q '[:find (count ?e) .
                              :where
                              [?e :name]]
                            @conn)))
              (finally
                (d/close conn))))))
      (finally
        (u/delete-files dir)))))

(deftest test-non-wal-store-ignores-stale-txlog-directory
  (let [schema {:name {:db/valueType :db.type/string}}
        dir    (u/tmp-dir (str "non-wal-stale-txlog-test-"
                               (UUID/randomUUID)))
        opts   {:wal? false :kv-opts {:wal? false}}]
    (try
      (let [conn (d/get-conn dir schema opts)]
        (d/transact! conn [{:db/id 1 :name "Ada"}])
        (d/close conn))
      (u/create-dirs (str dir u/+separator+ "txlog"))
      (let [conn (d/get-conn dir schema)]
        (try
          (is (false? (:wal? (d/opts conn))))
          (is (= #{["Ada"]}
                 (d/q '[:find ?name :where [_ :name ?name]] @conn)))
          (is (not (u/file-exists
                     (str dir u/+separator+ "snapshots"))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest test-with-conn
  (let [dir (u/tmp-dir (str "with-conn-test-" (UUID/randomUUID)))]
    (d/with-conn [conn dir]
      (d/transact! conn [{:db/id      -1
                          :name       "something"
                          :updated-at (Date.)}])
      (is (= 2 (count (d/datoms @conn :eav)))))
    (u/delete-files dir)))

(deftest test-with-kv
  (let [dir       (u/tmp-dir (str "with-kv-test-" (UUID/randomUUID)))
        db-ref    (atom nil)
        error-ref (atom nil)]
    (try
      (is (= :done
             (d/with-kv [db dir]
               (reset! db-ref db)
               (d/open-dbi db "state")
               (d/transact-kv db "state" [[:put "k" "v"]] :string :string)
               (is (= "v" (d/get-value db "state" "k" :string :string true)))
               :done)))
      (is (d/closed-kv? @db-ref))
      (d/with-kv [db dir]
        (d/open-dbi db "state")
        (is (= "v" (d/get-value db "state" "k" :string :string true))))
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"boom"
            (d/with-kv [db dir]
              (reset! error-ref db)
              (throw (ex-info "boom" {})))))
      (is (d/closed-kv? @error-ref))
      (finally
        (u/delete-files dir)))))

(deftest test-relaxed-transact-uses-queued-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :relaxed
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 32]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 32 (count @paths)))
      (is (every? #{:queued-relaxed} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-strict} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-extra-transact-prefers-direct-path-when-idle
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :extra
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (dotimes [i 16]
          (d/transact! conn [{:db/id i :k i}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-extra} @paths))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-strict-transact-async-no-stall
  (let [n    256
        conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})]
    (try
      (let [futs    (doall
                      (for [i (range n)]
                        (d/transact-async conn [{:db/id i :k i}])))
            results (doall (map #(deref % 10000 ::timeout) futs))]
        (is (not-any? #{::timeout} results))
        (is (= n
               (d/q '[:find (count ?e) .
                      :where [?e :k]]
                    (d/db conn)))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(deftest test-transact-async-callback-exception-does-not-stall-result
  (let [conn (d/create-conn nil
                            {:k {:db/valueType :db.type/long}}
                            {:wal? true
                             :wal-durability-profile :strict
                             :kv-opts {:inmemory? true}})
        fut  (d/transact-async conn
                               [{:db/id 1 :k 1}]
                               nil
                               (fn [_]
                                 (throw (ex-info "callback failed" {}))))]
    (try
      (is (not= ::timeout (deref fut 2000 ::timeout)))
      (is (= 1
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (dc/shutdown-transact-async-executor!)
        (d/close conn)))))

(defn- wait-until
  [pred timeout-ms]
  (let [^long timeout-ms timeout-ms]
    (loop [elapsed 0]
      (cond
        (pred) true
        (>= ^long elapsed timeout-ms) false
        :else
        (do
          (Thread/sleep 25)
          (recur (+ elapsed 25)))))))

(deftest test-strict-with-transaction-transact-uses-direct-path
  (let [conn  (d/create-conn nil
                             {:k {:db/valueType :db.type/long}}
                             {:wal? true
                              :wal-durability-profile :strict
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [dc/*txlog-sync-path-observer*
                (fn [path] (swap! paths conj path))]
        (d/with-transaction [cn conn]
          (dotimes [i 8]
            (d/transact! cn [{:db/id (- (inc i)) :k i}]))))
      (is (= 8 (count @paths)))
      (is (every? #{:direct-no-wal} @paths))
      (is (= 8
             (d/q '[:find (count ?e) .
                    :where [?e :k]]
                  (d/db conn))))
      (finally
        (d/close conn)))))

(deftest test-wal-rejects-commit-before-txlog-append
  (let [dir  (u/tmp-dir (str "wal-cross-process-recovery-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"forced commit failure"
               (binding [cpp/*before-write-commit-fn*
                         (fn [ctx]
                           (when (= (:operation ctx) :close-transact-kv)
                             (throw (ex-info "forced commit failure"
                                             {:type ::forced-commit-failure}))))]
                 (d/transact-kv db [[:put "a" :k :v]]))))
          (finally
            (d/close-kv db))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (nil? (d/get-value db "a" :k)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= 1
                 (get-in (kv/read-commit-marker db) [:current :applied-lsn])))
          (is (= 1
                 (:last-applied-lsn (kv/txlog-watermarks db))))
          (is (:ok? (kv/verify-commit-marker! db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-explicit-write-transaction-rejects-commit-before-txlog-append
  (let [dir  (u/tmp-dir (str "wal-explicit-write-transaction-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (let [wdb (i/open-transact-kv db)]
            (is (= :transacted
                   (d/transact-kv wdb [[:put "a" :k :v]])))
            (is (thrown-with-msg?
                 Exception
                 #"forced commit failure"
                 (binding [cpp/*before-write-commit-fn*
                           (fn [ctx]
                             (when (= (:operation ctx) :close-transact-kv)
                               (throw (ex-info "forced commit failure"
                                               {:type ::forced-commit-failure}))))]
                   (i/close-transact-kv db)))))
          (is (nil? (d/get-value db "a" :k)))
          (is (= [1]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (is (= [1 2]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-runs-after-local-commit
  (let [dir  (u/tmp-dir (str "wal-after-append-hook-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}
        seen (atom nil)]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (binding [kv/*after-txlog-append-fn*
                    (fn [{:keys [txlog-lsn] :as ctx}]
                      (reset! seen
                              {:ctx ctx
                               :value (d/get-value db "a" :k)
                               :watermarks (kv/txlog-watermarks db)
                               :txlog-lsn txlog-lsn}))]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= :v (:value @seen)))
          (is (= 2 (long (:txlog-lsn @seen))))
          (is (= 2
                 (long (get-in @seen [:watermarks :last-applied-lsn]))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-runs-for-explicit-transaction-close
  (let [dir  (u/tmp-dir (str "wal-after-append-explicit-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}
        seen (atom nil)]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (let [wdb (i/open-transact-kv db)]
            (is (= :transacted
                   (d/transact-kv wdb [[:put "a" :k :v]])))
            (binding [kv/*after-txlog-append-fn*
                      (fn [{:keys [txlog-lsn] :as ctx}]
                        (reset! seen
                                {:ctx ctx
                                 :value (d/get-value db "a" :k)
                                 :watermarks (kv/txlog-watermarks db)
                                 :txlog-lsn txlog-lsn}))]
              (is (= :committed
                     (i/close-transact-kv db)))))
          (is (= :v (:value @seen)))
          (is (= 2 (long (:txlog-lsn @seen))))
          (is (= 2
                 (long (get-in @seen [:watermarks :last-applied-lsn]))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-after-append-hook-failure-is-post-commit
  (let [dir  (u/tmp-dir (str "wal-after-append-hook-failure-test-"
                             (UUID/randomUUID)))
        opts {:wal? true
              :wal-commit-marker? true
              :snapshot-bootstrap-force? false
              :wal-durability-profile :strict}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (thrown-with-msg?
               Exception
               #"forced publish failure"
               (binding [kv/*after-txlog-append-fn*
                         (fn [_]
                           (throw (ex-info "forced publish failure"
                                           {:type ::forced-publish-failure})))]
                 (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= :v
                 (d/get-value db "a" :k)))
          (is (= 2
                 (long (:last-applied-lsn (kv/txlog-watermarks db)))))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (is (= [:v :v2]
                 [(d/get-value db "a" :k)
                  (d/get-value db "a" :k2)]))
          (is (= [1 2 3]
                 (mapv :lsn (kv/open-tx-log db 1))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-failure-still-allows-reopen
  (let [dir                    (u/tmp-dir (str "wal-process-lock-failure-test-"
                                               (UUID/randomUUID)))
        bootstrap-disabled-opts {:wal? true
                                 :snapshot-bootstrap-force? false}
        bootstrap-opts          {:wal? true
                                 :snapshot-bootstrap-force? true}]
    (try
      (let [db (d/open-kv dir bootstrap-disabled-opts)]
        (try
          (d/open-dbi db "bootstrap")
          (d/transact-kv db [[:put "bootstrap" :k :v]])
          (finally
            (d/close-kv db))))
      (let [{:keys [db error]}
            (binding [kv/*wal-snapshot-copy-failpoint*
                      (fn [_]
                        (throw (ex-info "forced snapshot bootstrap failure"
                                        {:type ::snapshot-bootstrap-failed})))]
              (try
                {:db (d/open-kv dir bootstrap-opts)}
                (catch Exception e
                  {:error e})))]
        (when db
          (d/close-kv db))
        (is (instance? clojure.lang.ExceptionInfo error))
        (is (re-find #"forced snapshot bootstrap failure"
                     (.getMessage ^Exception error))))
      (let [db (d/open-kv dir bootstrap-opts)]
        (try
          (is (not (d/closed-kv? db)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-fallback-restores-snapshot-with-clean-txlog-runtime
  (let [dir      (u/tmp-dir (str "wal-snapshot-fallback-test-"
                                 (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts     {:wal? true
                  :snapshot-bootstrap-force? false}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (d/create-snapshot! db)
          (is (seq (d/list-snapshots db)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [segment-path (->> (or (u/list-files txlog-dir) [])
                              (map #(.getPath ^java.io.File %))
                              (filter #(str/ends-with? % ".wal"))
                              sort
                              first)]
        (is (string? segment-path))
        (with-open [raf (java.io.RandomAccessFile. ^String segment-path "rw")]
          (.seek raf 0)
          (.write raf (byte-array [0 0 0 0]))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (nil? (d/get-value db "a" :k2)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k3 :v3]])))
          (is (= :v3
                 (d/get-value db "a" :k3)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-restores-live-created-dbi-from-snapshot-tail
  (let [dir  (u/tmp-dir (str "wal-live-dbi-snapshot-tail-test-"
                             (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts {:wal? true
              :wal-durability-profile :strict
              :snapshot-bootstrap-force? false}]
    (try
      (let [snapshot (atom nil)]
        (let [db (d/open-kv dir opts)]
          (try
            (reset! snapshot (:snapshot (d/create-snapshot! db)))
            (d/open-dbi db "live" {:key-size 64})
            (is (= :transacted
                   (d/transact-kv db [[:put "live" :k :v]])))
            (finally
              (d/close-kv db))))
        (copy-snapshot-files-to-env! (:path @snapshot) dir)
        (let [applied-lsn (long (:applied-lsn @snapshot))]
          (txlog/write-meta-file!
           (txlog/meta-path txlog-dir)
           {:last-committed-lsn applied-lsn
            :last-durable-lsn applied-lsn
            :last-applied-lsn applied-lsn
            :segment-id 1
            :segment-offset 0
            :updated-ms (System/currentTimeMillis)}))
        (let [db (d/open-kv dir opts)]
          (try
            (is (map? (i/dbi-opts db "live")))
            (is (= :v
                   (d/get-value db "live" :k)))
            (finally
              (d/close-kv db)))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-snapshot-fallback-failure-keeps-original-lmdb-files
  (let [dir       (u/tmp-dir (str "wal-snapshot-fallback-safe-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        data-path (str dir u/+separator+ c/data-file-name)
        opts      {:wal? true
                   :snapshot-bootstrap-force? false}]
    (try
      (let [snapshot-path (atom nil)]
        (let [db (d/open-kv dir opts)]
          (try
            (d/open-dbi db "a")
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k1 :v1]])))
            (d/create-snapshot! db)
            (reset! snapshot-path (:path (first (d/list-snapshots db))))
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k2 :v2]])))
            (finally
              (d/close-kv db))))
        (let [before (java.nio.file.Files/readAllBytes
                      (.toPath (java.io.File. data-path)))
              snapshot-data-path (str @snapshot-path u/+separator+
                                      c/data-file-name)
              segment-path (first-wal-segment-path txlog-dir)]
          (is (string? segment-path))
          (with-open [raf (java.io.RandomAccessFile.
                           ^String snapshot-data-path
                           "rw")]
            (.setLength raf 0)
            (.write raf (.getBytes "not-an-lmdb-snapshot" "UTF-8")))
          (with-open [raf (java.io.RandomAccessFile.
                           ^String segment-path
                           "rw")]
            (.seek raf 0)
            (.write raf (byte-array [0 0 0 0])))
          (is (thrown-with-msg?
               Exception
               #"Txn-log recovery failed after snapshot restore attempts"
               (d/open-kv dir opts)))
          (is (Arrays/equals
               before
               (java.nio.file.Files/readAllBytes
                (.toPath (java.io.File. data-path)))))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-refresh-shared-state-skips-meta-read-by-default
  (let [dir   (u/tmp-dir (str "wal-refresh-fast-path-test-"
                              (UUID/randomUUID)))
        state (atom nil)]
    (try
      (reset! state
              (:state (txlog/init-runtime-state {:dir dir
                                                 :wal-shared? false}
                                                {})))
      (txlog/write-meta-file!
       (:meta-path @state)
       {:last-committed-lsn 0
        :last-durable-lsn 0
        :last-applied-lsn 0
        :segment-id 1
        :segment-offset 0
        :updated-ms (System/currentTimeMillis)}
       {:sync-mode :none})
      (let [refreshed (txlog/refresh-shared-state! @state)
            watermarks (#'datalevin.txlog/refresh-shared-watermarks!
                        @state)]
        (is (= -1 (long @(:meta-revision @state))))
        (is (= 1 (long (:segment-id refreshed))))
        (is (= 0 (long (:last-committed-lsn refreshed))))
        (is (= 0 (long (:last-committed-lsn watermarks)))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-refresh-shared-state-reads-meta-when-shared
  (let [dir   (u/tmp-dir (str "wal-refresh-shared-test-"
                              (UUID/randomUUID)))
        state (atom nil)]
    (try
      (reset! state
              (:state (txlog/init-runtime-state {:dir dir
                                                 :wal-shared? true}
                                                {})))
      (txlog/write-meta-file!
       (:meta-path @state)
       {:last-committed-lsn 0
        :last-durable-lsn 0
        :last-applied-lsn 0
        :segment-id 1
        :segment-offset 0
        :updated-ms (System/currentTimeMillis)}
       {:sync-mode :none})
      (is (= 0 (long (:revision (txlog/refresh-shared-state! @state)))))
      (is (= 0 (long @(:meta-revision @state))))
      (vreset! (:meta-revision @state) -1)
      (is (= 0 (long (:revision
                      (#'datalevin.txlog/refresh-shared-watermarks!
                       @state)))))
      (is (= 0 (long @(:meta-revision @state))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-refresh-shared-state-clamps-stale-meta-segment-offset
  (let [dir       (u/tmp-dir (str "wal-stale-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true
                   :wal-shared? true}
        segment-path
        (fn []
          (->> (or (u/list-files txlog-dir) [])
               (map #(.getPath ^java.io.File %))
               (filter #(str/ends-with? % ".wal"))
               sort
               last))]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (is (string? path))
        ;; Leave the WAL meta file intact but wipe the active segment bytes to
        ;; simulate recovery paths where metadata survives while the segment
        ;; tail is rebuilt from scratch.
        (with-open [raf (java.io.RandomAccessFile. ^String path "rw")]
          (.setLength raf 0)))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v1
                 (d/get-value db "a" :k1)))
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k2 :v2]])))
          (finally
            (d/close-kv db))))
      (let [path (segment-path)]
        (with-open [raf (java.io.RandomAccessFile. ^String path "r")]
          (let [magic (byte-array 4)]
            (is (= 4 (.read raf magic)))
            (is (= [0x44 0x4c 0x57 0x4c]
                   (mapv (fn [b] (bit-and 0xff (int b))) magic))))))
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :v2
                 (d/get-value db "a" :k2)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-open-repairs-closed-preallocated-segment-tail
  (let [dir       (u/tmp-dir (str "wal-closed-prealloc-tail-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        state     (atom nil)]
    (try
      (u/create-dirs txlog-dir)
      (let [closed-path (txlog/segment-path txlog-dir 1)
            active-path (txlog/segment-path txlog-dir 2)]
        (txlog/prepare-segment! closed-path 4096)
        (with-open [^java.nio.channels.FileChannel ch
                    (txlog/open-segment-channel closed-path)]
          (txlog/append-record-at!
           ch
           0
           (txlog/encode-commit-row-payload 1 1 [])))
        (with-open [^java.nio.channels.FileChannel _
                    (txlog/open-segment-channel active-path)])
        (is (:preallocated-tail?
             (txlog/scan-segment closed-path
                                 {:allow-preallocated-tail? true
                                  :collect-records?         false})))
        (is (thrown-with-msg?
             Exception
             #"Txn-log segment corruption"
             (txlog/scan-segment closed-path {:collect-records? false})))
        (reset! state
                (:state (txlog/init-runtime-state {:dir dir} {})))
        (is (= 2 (long @(:segment-id @state))))
        (is (= 2 (long @(:next-lsn @state))))
        (let [file (java.io.File. ^String closed-path)
              scan (txlog/scan-segment closed-path
                                       {:collect-records? false})]
          (is (false? (:partial-tail? scan)))
          (is (= (long (:valid-end scan))
                 (long (.length file))))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-open-truncates-torn-active-tail-checksum-mismatch
  (let [dir       (u/tmp-dir (str "wal-torn-active-tail-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        state     (atom nil)]
    (try
      (u/create-dirs txlog-dir)
      (let [active-path (txlog/segment-path txlog-dir 1)]
        (txlog/prepare-segment! active-path 4096)
        (let [valid-payload (txlog/encode-commit-row-payload 1 1 [])
              bad-payload   (txlog/encode-commit-row-payload 2 2 [])
              bad-record    (txlog/encode-record bad-payload)
              header-len    (alength ^bytes (txlog/encode-record
                                              (byte-array 0)))
              body-fragment (max 1 (min 8
                                         (dec (alength ^bytes bad-payload))))
              torn-len      (+ header-len body-fragment)
              valid-end
              (with-open [^java.nio.channels.FileChannel ch
                          (txlog/open-segment-channel active-path)]
                (let [{:keys [size]} (txlog/append-record-at!
                                      ch
                                      0
                                      valid-payload)
                      tail-offset (long size)
                      ^ByteBuffer torn-bf (ByteBuffer/wrap
                                           ^bytes bad-record
                                           0
                                           torn-len)]
                  (.position ch tail-offset)
                  (while (.hasRemaining torn-bf)
                    (.write ch torn-bf))
                  tail-offset))]
          (let [scan (txlog/scan-segment
                      active-path
                      {:allow-preallocated-tail? true
                       :collect-records?         false})]
            (is (:partial-tail? scan))
            (is (not (:preallocated-tail? scan)))
            (is (:checksum-mismatch-tail? scan))
            (is (= valid-end (long (:valid-end scan)))))
          (is (thrown-with-msg?
               Exception
               #"Txn-log segment corruption"
               (txlog/scan-segment active-path {:collect-records? false})))
          (reset! state
                  (:state (txlog/init-runtime-state {:dir dir} {})))
          (is (= 2 (long @(:next-lsn @state))))
          (is (= valid-end (long @(:segment-offset @state))))
          (let [file (java.io.File. ^String active-path)
                scan (txlog/scan-segment active-path
                                         {:collect-records? false})]
            (is (= valid-end (long (.length file))))
            (is (false? (:partial-tail? scan)))
            (is (= valid-end (long (:valid-end scan)))))))
      (finally
        (when-let [state @state]
          (when-let [ch @(:segment-channel state)]
            (.close ^java.io.Closeable ch)))
        (u/delete-files dir)))))

(deftest test-wal-commit-meta-segment-offset-matches-segment-end
  (let [dir       (u/tmp-dir (str "wal-commit-meta-offset-test-"
                                  (UUID/randomUUID)))
        txlog-dir (str dir u/+separator+ "txlog")
        opts      {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k1 :v1]])))
          (finally
            (d/close-kv db))))
      (let [{:keys [file]} (last (txlog/segment-files txlog-dir))
            meta-state (get-in (txlog/read-meta-file (txlog/meta-path txlog-dir))
                               [:current])
            scan (txlog/scan-segment (.getPath ^java.io.File file)
                                     {:allow-preallocated-tail? true})
            end-offset (txlog/segment-end-offset scan)]
        (is (map? meta-state))
        (is (= (long end-offset)
               (long (:segment-offset meta-state)))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-aligns-runtime-cursor-to-persisted-payload-floor
  (let [dir  (u/tmp-dir (str "wal-replay-align-floor-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          ;; Build a local WAL tail that ends at LSN 14, then simulate a
          ;; snapshot-installed follower whose persisted payload floor has
          ;; already advanced to LSN 15 before the runtime txlog cursor is
          ;; realigned.
          (dotimes [i 13]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          (let [state (txlog/state db)]
            (is (some? state))
            (is (= 15 (long @(:next-lsn state))))
            (is (= :transacted
                   (kv/transact-kv-without-txlog!
                    db
                    [[:put c/kv-info c/wal-local-payload-lsn
                      15 :keyword :data]])))
            (let [res (kv/mirror-replayed-txlog-record!
                       db
                       {:lsn 16
                        :ha-term 7
                        :rows [[:put "a" :replayed :ok]]})]
              (is (= 16 (long (:lsn res))))
              (is (not (:skipped? res)))
              (is (= 17 (long @(:next-lsn state))))
              (is (= :ok
                     (d/get-value db "a" :replayed)))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-skips-record-covered-by-persisted-payload-floor
  (let [dir  (u/tmp-dir (str "wal-replay-skip-payload-floor-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (dotimes [i 13]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" i i]]))))
          (let [state (txlog/state db)]
            (is (some? state))
            (is (= 15 (long @(:next-lsn state))))
            (is (= :transacted
                   (kv/transact-kv-without-txlog!
                    db
                    [[:put c/kv-info c/wal-local-payload-lsn
                      15 :keyword :data]])))
            (#'kv/align-runtime-txlog-payload-floor! db)
            (is (= 16 (long @(:next-lsn state))))
            (is (empty? (kv/open-tx-log db 15 15)))
            (let [res (kv/mirror-replayed-txlog-record!
                       db
                       {:lsn 15
                        :ha-term 7
                        :rows [[:put "a" :covered :ok]]})]
              (is (= 15 (long (:lsn res))))
              (is (:skipped? res))
              (is (nil? (d/get-value db "a" :covered)))
              (is (= 16 (long @(:next-lsn state))))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-materializes-skipped-unapplied-tail
  (let [dir  (u/tmp-dir (str "wal-replay-unapplied-tail-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (d/transact-kv db [[:put "a" :k :expected]])))
          (let [record (last (kv/open-tx-log db 1))
                lsn    (long (:lsn record))]
            ;; Model a crash after WAL append but before its LMDB payload was
            ;; fully reflected: the local cursor contains the record, while
            ;; the persisted materialization floor still trails it.
            (is (= :transacted
                   (kv/transact-kv-without-txlog!
                    db
                    [[:put "a" :k :stale]
                     [:put c/kv-info c/wal-local-payload-lsn
                      (dec lsn) :keyword :data]])))
            (let [res (kv/mirror-replayed-txlog-record!
                       db record nil {:replay-skipped? true})]
              (is (:skipped? res))
              (is (:replayed? res))
              (is (= :expected (d/get-value db "a" :k)))
              (is (= lsn
                     (long (i/get-value db
                                        c/kv-info
                                        c/wal-local-payload-lsn
                                        :keyword
                                        :data))))))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-direct-replay-preserves-higher-payload-floor
  (let [dir  (u/tmp-dir (str "wal-direct-replay-payload-floor-test-"
                             (UUID/randomUUID)))
        opts {:wal? true}]
    (try
      (let [db (d/open-kv dir opts)]
        (try
          (d/open-dbi db "a")
          (is (= :transacted
                 (kv/transact-kv-without-txlog!
                  db
                  [[:put c/kv-info c/wal-local-payload-lsn
                    15 :keyword :data]])))
          (kv/replay-txlog-rows! db [[:put "a" :covered :ok]] 14)
          (is (= 15
                 (i/get-value db
                              c/kv-info
                              c/wal-local-payload-lsn
                              :keyword
                              :data)))
          (is (= :ok
                 (d/get-value db "a" :covered)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))

(deftest test-wal-replay-rejects-divergent-local-skip
  (let [source-dir (u/tmp-dir (str "wal-replay-divergent-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "wal-replay-divergent-target-"
                                   (UUID/randomUUID)))
        opts       {:wal? true}
        source-db  (atom nil)
        target-db  (atom nil)]
    (try
      (let [source (d/open-kv source-dir opts)
            target (d/open-kv target-dir opts)]
        (reset! source-db source)
        (reset! target-db target)
        (kv/mirror-replayed-txlog-record!
         target
         {:lsn 1
          :ha-term 1
          :rows [[:put c/kv-info [:replay-divergent :k] :rogue
                  :data :data]]})
        (let [local-record (first (kv/open-tx-log target 1))]
          (is (= {:lsn 1 :skipped? true}
                 (select-keys
                  (kv/mirror-replayed-txlog-record! target local-record)
                  [:lsn :skipped?]))))
        (kv/mirror-replayed-txlog-record!
         source
         {:lsn 1
          :ha-term 2
          :rows [[:put c/kv-info [:replay-divergent :k] :canonical
                  :data :data]]})
        (let [incoming (first (kv/open-tx-log source 1))
              err      (try
                         (kv/mirror-replayed-txlog-record! target incoming)
                         nil
                         (catch clojure.lang.ExceptionInfo e
                           e))
              data     (ex-data err)]
          (is (some? err))
          (is (= :txlog/ha-replay-divergent-local-record (:type data)))
          (is (= :ha/txlog-divergent-local-record (:error data)))
          (is (= 1 (long (:record-lsn data))))
          (is (= 2 (get-in data [:incoming-record :ha-term])))
          (is (= 1 (get-in data [:local-record :ha-term])))))
      (finally
        (when-let [db @source-db]
          (d/close-kv db))
        (when-let [db @target-db]
          (d/close-kv db))
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest test-wal-replay-preserves-lookup-ref-cas
  (let [source-dir (u/tmp-dir (str "wal-replay-cas-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "wal-replay-cas-target-"
                                   (UUID/randomUUID)))
        schema     {:identity/key   {:db/valueType :db.type/string
                                      :db/unique :db.unique/identity}
                    :identity/value {:db/valueType :db.type/long}}
        opts       {:wal? true
                    :wal-durability-profile :strict}]
    (try
      (let [source-conn (d/create-conn source-dir schema opts)
            target-conn (d/create-conn target-dir schema opts)]
        (try
          (d/with-transaction [tx source-conn]
            (d/transact! tx
                         [{:db/id -1
                           :identity/key "identity-1"
                           :identity/value 0}]))
          (d/with-transaction [tx source-conn]
            (d/transact! tx
                         [[:db/cas [:identity/key "identity-1"]
                           :identity/value 0 1]
                          [:db/cas [:identity/key "identity-1"]
                           :identity/value 1 2]]))
          (let [source-kv (.-lmdb ^Store (conn-store source-conn))
                target-kv (.-lmdb ^Store (conn-store target-conn))
                records   (kv/open-tx-log source-kv 1)]
            (doseq [record records]
              (kv/mirror-replayed-txlog-record! target-kv record))
            (d/close target-conn)
            (let [target-conn' (d/create-conn target-dir schema opts)]
              (try
                (is (= 2 (:identity/value
                          (d/entity @target-conn'
                                    [:identity/key "identity-1"]))))
                (finally
                  (d/close target-conn')))))
          (finally
            (d/close source-conn)
            (when-not (d/closed? target-conn)
              (d/close target-conn)))))
      (finally
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest test-wal-ha-replay-serializes-cardinality-one-cleanup
  (let [source-dir (u/tmp-dir (str "wal-replay-serialized-cleanup-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "wal-replay-serialized-cleanup-target-"
                                   (UUID/randomUUID)))
        schema     {:bank/id      {:db/valueType :db.type/long
                                   :db/unique :db.unique/identity}
                    :bank/balance {:db/valueType :db.type/long}}
        opts       {:wal? true
                    :wal-durability-profile :strict}
        initial    [{:db/id "account-0" :bank/id 0 :bank/balance 100}
                    {:db/id "account-1" :bank/id 1 :bank/balance 100}
                    {:db/id "account-2" :bank/id 2 :bank/balance 100}]]
    (try
      (let [source-conn (d/create-conn source-dir schema opts)
            target-conn (d/create-conn target-dir schema opts)]
        (try
          (d/transact! source-conn initial)
          (d/transact! target-conn initial)
          ;; Consecutive transfers share account 1. If both replay callbacks
          ;; derive cleanup rows before record 1 is materialized, record 2
          ;; deletes balance 100 instead of 105 and leaves a stale duplicate.
          (d/transact! source-conn
                       [{:db/id [:bank/id 0] :bank/balance 95}
                        {:db/id [:bank/id 1] :bank/balance 105}])
          (d/transact! source-conn
                       [{:db/id [:bank/id 1] :bank/balance 101}
                        {:db/id [:bank/id 2] :bank/balance 104}])
          (let [source-kv   (.-lmdb ^Store (conn-store source-conn))
                target-store (conn-store target-conn)
                target-kv   (.-lmdb ^Store target-store)
                next-lsn    (long @(:next-lsn (txlog/state target-kv)))
                records     (vec
                             (drop-while #(< (long (:lsn %)) next-lsn)
                                         (kv/open-tx-log source-kv 1)))
                cleanup-var (ns-resolve
                             'datalevin.ha.replication
                             'ha-cardinality-one-eav-cleanup-rows)
                cleanup-fn  @cleanup-var
                second-entered (promise)
                second-future (promise)
                second-during-first (atom nil)
                cleanup     (fn [record]
                              (cleanup-fn
                               target-store
                               target-kv
                               (vec (or (:rows record) (:ops record)))))]
            (is (= 2 (count records)))
            (let [record-1 (first records)
                  record-2 (second records)
                  result-1
                  (kv/mirror-replayed-txlog-record!
                   target-kv
                   record-1
                   (fn []
                     (let [f2
                           (future
                             (try
                               (kv/mirror-replayed-txlog-record!
                                target-kv
                                record-2
                                (fn []
                                  (deliver second-entered true)
                                  (cleanup record-2)))
                               (catch Throwable e
                                 e)))]
                       (deliver second-future f2)
                       ;; The second cleanup supplier must not run while the
                       ;; first replay owns the serialized WAL/LMDB apply lock.
                       (reset! second-during-first
                               (deref second-entered 300 ::timeout))
                       (cleanup record-1))))
                  f2 (deref second-future 5000 ::timeout)
                  result-2 (if (= ::timeout f2)
                             ::timeout
                             (deref f2 10000 ::timeout))]
              (is (= ::timeout @second-during-first))
              (is (map? result-1))
              (is (map? result-2)
                  (when (instance? Throwable result-2)
                    (ex-message result-2))))
            (s/sync-max-tx-floor!
             target-store
             (long (i/get-value target-kv c/meta :max-tx :attr :long)))
            (db/refresh-cache target-store)
            (let [target-db (db/new-db target-store)
                  balances (mapv
                            (fn [account-id]
                              (:bank/balance
                               (d/entity target-db [:bank/id account-id])))
                            [0 1 2])
                  account-1-eid (d/entid target-db [:bank/id 1])
                  account-1-eav (i/get-list target-kv
                                            c/eav
                                            account-1-eid
                                            :id
                                            :avg)]
              (is (= [95 101 104] balances))
              (is (= 2 (count account-1-eav)))))
          (finally
            (d/close source-conn)
            (when-not (d/closed? target-conn)
              (d/close target-conn)))))
      (finally
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest test-wal-replay-preserves-repeated-cardinality-one-giant-updates
  (let [source-dir (u/tmp-dir (str "wal-replay-giant-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "wal-replay-giant-target-"
                                   (UUID/randomUUID)))
        schema     {:giant/key     {:db/valueType :db.type/long
                                    :db/unique :db.unique/identity}
                    :giant/version {:db/valueType :db.type/long}
                    :giant/payload {:db/valueType :db.type/string}}
        opts       {:wal? true
                    :wal-durability-profile :strict}
        payload    (fn [version]
                     (apply str (take 12000 (cycle (str "payload-" version)))))]
    (try
      (let [source-conn (d/create-conn source-dir schema opts)
            target-conn (d/create-conn target-dir schema opts)]
        (try
          (doseq [version [29 13 9]]
            (d/transact! source-conn
                         [{:db/id "giant-2"
                           :giant/key 2
                           :giant/version version
                           :giant/payload (payload version)}]))
          ;; Model a follower snapshot at version 29, then stream the two
          ;; subsequent raw WAL records through the real HA replay path.
          (d/transact! target-conn
                       [{:db/id "giant-2"
                         :giant/key 2
                         :giant/version 29
                         :giant/payload (payload 29)}])
          (let [source-kv (.-lmdb ^Store (conn-store source-conn))
                target-store (conn-store target-conn)
                target-kv (.-lmdb ^Store target-store)
                next-lsn (long @(:next-lsn (txlog/state target-kv)))
                records (drop-while #(< (long (:lsn %)) next-lsn)
                                    (kv/open-tx-log source-kv 1))]
            (is (= 2 (count records)))
            (reduce repl/apply-ha-follower-txlog-record!
                    {:store target-store}
                    records)
            (db/refresh-cache target-store)
            (let [entity (d/entity (db/new-db target-store)
                                   [:giant/key 2])]
              (is (= 9 (:giant/version entity)))
              (is (= (payload 9) (:giant/payload entity)))))
          (finally
            (d/close source-conn)
            (when-not (d/closed? target-conn)
              (d/close target-conn)))))
      (finally
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest test-wal-one-shot-write-uses-explicit-lmdb-write-transaction
  (let [dir  (u/tmp-dir (str "wal-one-shot-write-test-" (UUID/randomUUID)))
        ops* (atom [])]
    (try
      (let [db (d/open-kv dir {:wal? true})]
        (try
          (d/open-dbi db "a")
          (binding [cpp/*before-write-commit-fn*
                    (fn [{:keys [operation]}]
                      (swap! ops* conj operation))]
            (is (= :transacted
                   (d/transact-kv db [[:put "a" :k :v]]))))
          (is (= [:close-transact-kv] @ops*))
          (is (= :v
                 (d/get-value db "a" :k)))
          (finally
            (d/close-kv db))))
      (finally
        (u/delete-files dir)))))
