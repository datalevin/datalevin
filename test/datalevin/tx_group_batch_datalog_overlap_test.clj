(ns datalevin.tx-group-batch-datalog-overlap-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.binding.cpp :as cpp]
            [datalevin.core :as d]
            [datalevin.constants :as c]
            [datalevin.datom :as datom]
            [datalevin.conn :as conn-api]
            [datalevin.interface :as i]
            [datalevin.index :as idx]
            [datalevin.kv :as kv]
            [datalevin.db.tx.common :as txcommon]
            [datalevin.lmdb :as l]
            [datalevin.storage :as s]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.batch.rmw :as rmw]
            [datalevin.tx-group.phase :as phase]
            [datalevin.txlog :as wal]
            [datalevin.util :as u])
  (:import [clojure.lang Var]
           [java.io IOException]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private opts
  {:wal? true :wal-durability-profile :strict :wal-segment-prealloc? false
   :snapshot-scheduler? false :background-sampling? false})

(def ^:dynamic *scalar-caller* :root)

(defn- with-conn [opts f]
  (let [dir (u/tmp-dir (str "datalog-native-overlap-" (random-uuid)))
        conn (d/create-conn dir {:value {:db/valueType :db.type/long}} opts)]
    (try (f conn dir)
         (finally (d/close conn) (u/delete-files dir)))))

(defn- collected! [conn ops]
  (let [collector (get-in @(i/kv-info (d/datalog-kv conn)) [:independent-control :collector])
        queue (.-ready ^datalevin.tx_group.batch.Collector collector)
        entered (promise) release (promise) jobs (atom [])
        previous (phase/current-observer)
        unobserve (phase/observe!
                    (fn [event b]
                      (when (and (= :batch-sealed event)
                                 (identical? collector (batch/batch-collector b))
                                 (not (realized? entered)))
                        (deliver entered true)
                        (assert (deref release 10000 false)))
                      (when previous (previous event b))))]
    (try
      (doseq [[idx op] (map-indexed vector ops)]
        (swap! jobs conj (future (try (op) (catch Throwable t t))))
        (when (zero? (long idx)) (is (deref entered 10000 false)))
        (is (loop [attempt 0]
              (cond (= (dec (count @jobs)) (.size ^java.util.Queue queue)) true
                    (= attempt 1000) false
                    :else (do (Thread/sleep 5) (recur (inc attempt)))))))
      (deliver release true)
      (mapv #(deref % 10000 ::timeout) @jobs)
      (finally (deliver release true)
               (doseq [job @jobs] (deref job 10000 nil))
               (unobserve)))))

(deftest resolution-cursor-decodes-before-release-and-closes-on-commit-or-abort
  (with-conn opts
    (fn [conn _]
      (d/update-schema conn {:text {:db/valueType :db.type/string}
                             :absent {:db/valueType :db.type/string}})
      (let [giant (apply str (repeat 2000 "shared-prefix-"))]
        (d/transact! conn [{:db/id 1 :value 10 :text "shared-prefix-small"}
                          {:db/id 2 :value 20 :text giant}])
        (let [raw (kv/raw-lmdb (d/datalog-kv conn))
              schema (d/schema conn)
              aid #(get-in schema [% :db/aid])]
          (doseq [abort? [false true]]
            (let [cursor (atom nil)
                  owner (atom nil)
                  run! #(cpp/apply-native-once!
                          raw
                          (fn [view]
                            (let [rtx @(l/write-txn view)
                                  dbi (i/get-dbi view c/eav false)
                                  read! (fn [e a]
                                          (cpp/read-resolution-value
                                            view view e (aid a) idx/avg-buffer->v))]
                              (reset! owner rtx)
                              (is (= "shared-prefix-small" (read! 1 :text)))
                              (reset! cursor (cpp/resolution-cursor! rtx dbi))
                              (is (= giant (read! 2 :text)))
                              (is (= 10 (read! 1 :value)))
                              (is (nil? (read! 1 :absent)))
                              (is (nil? (read! 999 :value)))
                              (is (= "shared-prefix-small" (read! 1 :text)))
                              (is (identical? @cursor (cpp/resolution-cursor! rtx dbi)))
                              (when abort? (throw (ex-info "abort lookup" {})))))
                          nil)]
              (if abort?
                (is (thrown-with-msg? Exception #"abort lookup" (run!)))
                (run!))
              (let [field (.getDeclaredField datalevin.binding.cpp.Rtx
                                              "resolution_cursor")]
                (.setAccessible field true)
                (is (nil? (.get field @owner)))))))))))

(deftest resolved-members-reuse-callbacks-across-a-generic-body
  (with-conn
    opts
    (fn [conn _]
      (let [kv (d/datalog-kv conn)
            control (:independent-control @(i/kv-info kv))
            seen (atom [])
            op (fn [which prepared? n]
                 #((:internal-body! control)
                   (fn [view]
                     (swap! seen conj (assoc (select-keys (meta view)
                                                          [:native-row-capture
                                                           :native-batch-abort!
                                                           :native-storage-staged!])
                                             :request-context (l/request-context view)))
                     (d/transact-kv view "probe" [[:put n n]] :long :long)
                     which)
                   {:datalog-conn conn :datalog-prepare? prepared? :which which}))]
        (d/open-dbi kv "probe")
        (binding [rmw/*reuse-datalog-member-setup?* true]
          (is (= [:first :generic :last]
                 (collected! conn [(op :first true 1) (op :generic false 2)
                                   (op :last true 3)]))))
        (let [[first generic last] @seen]
          (doseq [callback [:native-row-capture :native-batch-abort!
                           :native-storage-staged!]]
            (is (identical? (callback first) (callback last)))
            (is (not (identical? (callback first) (callback generic)))))
          (is (= [:first :generic :last]
                 (mapv #(get-in % [:request-context :which]) @seen))))
        (doseq [n [1 2 3]] (is (= n (d/get-value kv "probe" n :long :long))))))))

(deftest scalar-callers-retain-bindings-and-restore-their-frames
  (doseq [fail? [false true]]
    (with-conn
      opts
      (fn [conn _]
        (d/transact! conn [{:db/id 1 :value 0}])
        (let [seen (atom [])
              callers (atom {})
              outer-frame (Var/getThreadBindingFrame)
              ops (mapv
                    (fn [value]
                      #(binding [*scalar-caller* :outer]
                         (binding [*scalar-caller* value
                                   conn-api/*local-wal-tx-path-observer*
                                   (fn [path]
                                     (is (= :scalar-update path))
                                     (swap! seen conj {:request value
                                                       :binding *scalar-caller*
                                                       :owner (Thread/currentThread)})
                                     (when (and fail? (= 4 value))
                                       (throw (ex-info "Scalar binding failure" {}))))]
                           (let [frame (Var/getThreadBindingFrame)]
                             (swap! callers assoc value (Thread/currentThread))
                             (try
                               (d/transact! conn [[:db/add 1 :value value]]
                                            {:request value})
                               (finally
                                 (is (identical? frame (Var/getThreadBindingFrame)))
                                 (is (= value *scalar-caller*))
                                 (is (nil? s/*write-group*))
                                 (is (nil? txcommon/*batch-prepare*))))))))
                    (range 1 5))
              results (collected! conn ops)]
          (is (= [1 2 3 4] (mapv :request @seen)))
          (is (= [1 2 3 4] (mapv :binding @seen)))
          (is (some #(identical? (:owner %) (get @callers (:request %))) @seen))
          (is (some #(not (identical? (:owner %) (get @callers (:request %)))) @seen))
          (is (identical? outer-frame (Var/getThreadBindingFrame)))
          (is (= :root *scalar-caller*))
          (if fail?
            (do (is (every? #(instance? Throwable %) results))
                (is (= 0 (:value (d/entity @conn 1)))))
            (do (is (every? map? results))
                (is (= 4 (:value (d/entity @conn 1)))))))))))

(deftest preparation-context-is-batch-owned-and-bindings-do-not-escape
  (with-conn
    opts
    (fn [conn _]
      (let [seen (atom [])
            outer txcommon/*batch-prepare*
            ops (mapv (fn [value]
                        #(let [report (d/transact! conn
                                                  [[:db.fn/call
                                                    (fn [_]
                                                      (swap! seen conj txcommon/*batch-prepare*)
                                                      [[:db/add 1 :value value]])]])]
                           (is (identical? outer txcommon/*batch-prepare*))
                           (is (nil? s/*write-group*))
                           report))
                      [1 2])]
        (is (every? map? (collected! conn ops)))
        (is (= 2 (count @seen)))
        (is (satisfies? txcommon/BatchPreparation (first @seen)))
        (is (identical? (first @seen) (second @seen)))
        (is (identical? outer txcommon/*batch-prepare*))
        (is (nil? s/*write-group*))
        (is (= 2 (:value (d/entity @conn 1))))))))

(deftest scalar-pending-index-materializes-before-general-resolution
  (with-conn
    opts
    (fn [conn _]
      (d/update-schema conn {:enabled {:db/valueType :db.type/boolean}
                             :tags {:db/cardinality :db.cardinality/many}})
      (d/transact! conn [{:db/id 1 :value 0 :enabled false :tags #{"old"}}])
      (let [control (:independent-control @(i/kv-info (d/datalog-kv conn)))
            scalar-stages (atom [])
            reports
            (binding [conn-api/*local-wal-tx-path-observer*
                      (fn [_]
                        (when (txcommon/scalar-pending-index)
                          (let [context @(:datalog-context control)
                                tx-db (or @(:current context) @conn)]
                            (swap! scalar-stages conj
                                   [(empty? (:eavt tx-db)) (empty? (:avet tx-db))]))))]
              (collected! conn
                          [#(d/transact! conn [[:db/add 1 :value 1]])
                           #(d/transact! conn [[:db/add 1 :value 2]])
                           #(d/transact! conn [[:db/add 1 :enabled true]])
                           #(d/transact! conn [[:db.fn/cas 1 :value 2 3]
                                              [:db/add 1 :tags "new"]])
                           #(d/transact! conn [[:db/add 1 :value 4]])]))]
        (is (every? map? reports) (pr-str reports))
        (is (seq @scalar-stages))
        (is (every? #(= [true true] %) @scalar-stages))
        (is (= [[[0 false] [1 true]] [[1 false] [2 true]]
                [[2 false] [3 true]] [[3 false] [4 true]]]
               (mapv (fn [report]
                       (mapv (juxt :v datom/datom-added)
                             (filter #(= :value (:a %)) (:tx-data report))))
                     [(reports 0) (reports 1) (reports 3) (reports 4)])))
        (is (= {:value 4 :enabled true :tags ["new" "old"]}
               (d/pull @conn [:value :enabled :tags] 1)))))))

(deftest scalar-pending-projection-mixes-native-values-and-false
  (with-conn
    opts
    (fn [conn _]
      (let [attrs (mapv #(keyword "pending" (str "field" %)) (range 10))
            flag (attrs 1)
            types (assoc (zipmap attrs (repeat {:db/valueType :db.type/long}))
                         flag {:db/valueType :db.type/boolean})
            old (assoc (zipmap attrs (repeat 0)) flag true)
            target (assoc (zipmap attrs (repeat 20)) flag false)
            update-all (mapv (fn [attr] [:db/add 1 attr (target attr)]) attrs)]
        (d/update-schema conn types)
        (d/transact! conn [(assoc old :db/id 1)])
        (let [reports (collected! conn
                                 [#(d/transact! conn [[:db/add 1 (attrs 0) 10]
                                                     [:db/add 1 flag false]])
                                  #(d/transact! conn update-all)
                                  #(d/transact! conn update-all)])]
          (is (every? map? reports) (pr-str reports))
          (is (= [4 18 0] (mapv #(count (:tx-data %)) reports)))
          (is (some #(and (= (attrs 0) (:a %)) (= 10 (:v %)) (not (:added %)))
                    (:tx-data (reports 1))))
          (is (not-any? #(= flag (:a %)) (:tx-data (reports 1))))
          (is (= target (into {} (d/pull @conn attrs 1)))))))))

(deftest tempid-upsert-retry-restores-prior-members-resolver-state
  (let [dir (u/tmp-dir (str "resolver-restore-" (random-uuid)))
        conn (d/create-conn dir {:key {:db/unique :db.unique/identity}
                                :value {:db/valueType :db.type/long}} opts)]
    (try
      (d/transact! conn [{:db/id 1 :key "existing" :value 1}
                         {:db/id 2 :value 2}])
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/add 2 :value 99]])
                                ;; Allocate the tempid before resolving its
                                ;; identity, forcing the resolver's retry path.
                                #(d/transact! conn [[:db/add -1 :value 20]
                                                    [:db/add -1 :key "existing"]])
                                #(d/transact! conn [[:db.fn/cas 2 :value 99 100]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= 1 (get-in reports [1 :tempids -1])))
        (is (= 20 (:value (d/entity @conn 1))))
        (is (= 100 (:value (d/entity @conn 2))))
        (is (= 1 (d/entid @conn [:key "existing"]))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest escaped-capture-from-user-code-stays-expired
  (with-conn
    opts
    (fn [conn _]
      (let [kv (d/datalog-kv conn)
            control (:independent-control @(i/kv-info kv))
            escaped (atom nil)
            context {:datalog-conn conn :datalog-prepare? true}]
        (d/open-dbi kv "probe")
        (let [results
              (collected!
                conn
                [#((:internal-body! control)
                   (fn [view]
                     ((:native-prepare-flush! (meta view)))
                     (reset! escaped (:native-row-capture (meta view))))
                   context)
                 #((:internal-body! control)
                   (fn [_]
                     (try (@escaped "probe" [[:put 1 1]] :long :long)
                          (catch Exception _ :caught)))
                   context)])]
          (is (every? #(instance? Throwable %) results))
          (is (= :txlog/transaction-view-invalidated
                 (:error (ex-data (first results)))))
          (is (nil? (d/get-value kv "probe" 1 :long :long))))))))

(deftest resolved-batch-overlaps-general-and-dependent-requests
  (let [dir (u/tmp-dir (str "datalog-resolved-batch-" (random-uuid)))
        conn (d/create-conn dir
                            {:value {:db/valueType :db.type/long}
                             :key {:db/unique :db.unique/identity}
                             :tags {:db/cardinality :db.cardinality/many}
                             :link {:db/valueType :db.type/ref}} opts)]
    (try
      (d/transact! conn [{:db/id 1 :key "a" :value 10 :tags #{"old"}}
                         {:db/id 2 :key "b" :value 20 :link 1}])
      (let [wal-started (CountDownLatch. 1) native-done (CountDownLatch. 1)
            events (atom [])
            unobserve (phase/observe!
                        (fn [event _]
                          (swap! events conj event)
                          (case event
                            :native-tail-start (is (.await wal-started 5 TimeUnit/SECONDS))
                            :native-tail-applied (.countDown native-done)
                            :wal-start (do (.countDown wal-started)
                                           (assert (.await native-done 5 TimeUnit/SECONDS)))
                            nil)))]
        (try
          (let [reports (collected! conn
                                   [#(d/transact! conn [[:db/add 1 :value 11]])
                                    #(d/transact! conn [[:db.fn/cas 1 :value 11 12]])
                                    #(d/transact! conn [[:db/retract 1 :value 12]
                                                       [:db/retract 1 :key "a"]
                                                       [:db/retract 1 :tags "old"]])
                                    #(d/transact! conn [{:db/id 1 :value 10 :key "c" :tags #{"new"}}])
                                    #(d/transact! conn [[:db/add [:key "c"] :value 13]])
                                    #(d/transact! conn [{:db/id -1 :key "a" :value 30 :link [:key "c"]}])
                                    #(d/transact! conn [[:db/retractEntity 1]])
                                    #(d/transact! conn [[:db/add 1 :value 99]])])]
            (is (every? map? reports) (pr-str reports))
            (is (= 1 (count (filter #{:native-tail-start} @events))))
            (is (= 1 (count (filter #{:worker-dispatch} @events))))
            (is (= 99 (:value (d/entity @conn 1))))
            (is (nil? (:key (d/entity @conn 1))))
            (is (nil? (:tags (d/entity @conn 1))))
            (is (nil? (:link (d/entity @conn 2))))
            (is (= 30 (:value (d/entity @conn [:key "a"]))))
            (is (nil? (:link (d/entity @conn [:key "a"])))))
          (finally (unobserve))))
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (is (= 99 (:value (d/entity @reopened 1))))
             (is (= 30 (:value (d/entity @reopened [:key "a"]))))
             (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest transaction-functions-drain-preceding-resolved-writes-before-native-reads
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [calls (atom 0)
            reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :value 1]])
                                #(d/transact! conn
                                              [[:db.fn/call
                                                (fn [tx-db]
                                                  (swap! calls inc)
                                                  (is (= 1 (d/q '[:find ?v . :where [1 :value ?v]] tx-db)))
                                                  [[:db/add 1 :value 2]])]])
                                #(d/transact! conn [[:db/add 1 :value 3]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= 1 @calls))
        (is (= 3 (:value (d/entity @conn 1))))))))

(deftest batch-encoding-refreshes-after-schema-changes
  (with-conn opts
    (fn [conn dir]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/retract 1 :value 0]])
                                #(d/with-transaction [tx conn]
                                   (d/update-schema tx {:value {:db/noindex true}})
                                   (d/transact! tx [[:db/add 1 :value 2]]))])]
        (is (every? map? reports) (pr-str reports))
        (is (= 2 (:value (d/entity @conn 1))))
        (is (zero? (i/entries (d/datalog-kv conn) c/ave))))
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (is (= 2 (:value (d/entity @reopened 1))))
             (is (zero? (i/entries (d/datalog-kv reopened) c/ave)))
             (finally (d/close reopened)))))))

(deftest resolved-batch-retains-false-scalar-values
  (with-conn opts
    (fn [conn _]
      (d/update-schema conn {:flag {:db/valueType :db.type/boolean}})
      (d/transact! conn [{:db/id 1 :flag true}])
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :flag false]])
                                #(d/transact! conn [[:db/add 1 :flag true]])
                                #(d/transact! conn [[:db/add 1 :flag false]])])]
        (is (every? map? reports) (pr-str reports))
        (is (some #(and (= false (:v %)) (not (:added %)))
                  (:tx-data (second reports))))
        (is (= false (:flag (d/entity @conn 1))))
        (is (= #{[false]} (d/q '[:find ?v :where [1 :flag ?v]] @conn)))
        (is (= 1 (i/entries (d/datalog-kv conn) c/ave)))))))

(deftest collected-wide-scalars-read-pending-and-native-values
  (with-conn opts
    (fn [conn dir]
      (let [names [:alpha :beta :flag :omega]
            values ["bbb" "ccc" false "zzz"]
            updates (fn [values]
                      (mapv #(vector :db/add 1 %1 %2) names values))]
        (d/update-schema conn
                         {:alpha {:db/valueType :db.type/string}
                          :beta {:db/valueType :db.type/string :db/noindex true}
                          :flag {:db/valueType :db.type/boolean}
                          :omega {:db/valueType :db.type/string}
                          :tags {:db/cardinality :db.cardinality/many}})
        (d/transact! conn [{:db/id 1 :alpha "old" :beta "old" :flag false
                           :omega "old" :tags #{"leave"}}])
        (let [before-tx (:max-tx @conn)
              reports (collected! conn
                                   [#(d/transact! conn [[:db/add 1 :alpha "aaa"]
                                                       [:db/add 1 :flag true]])
                                    #(d/transact! conn [[:db/retract 1 :beta "old"]])
                                    #(d/transact! conn (updates values))
                                    #(d/transact! conn (updates values))
                                    #(d/transact! conn [[:db/retractEntity 1]])
                                    #(d/transact! conn (updates values))
                                    #(d/transact! conn (updates ["aaa" "bbb" true "yyy"]))])]
          (is (every? map? reports) (pr-str reports))
          (is (= [4 1 7 0 5 4 8] (mapv #(count (:tx-data %)) reports)))
          (is (= (range (inc before-tx) (+ before-tx 8))
                 (mapv #(get-in % [:tempids :db/current-tx]) reports)))
          (is (some #(and (= :alpha (:a %)) (= "aaa" (:v %)) (not (:added %)))
                    (:tx-data (nth reports 2)))))
        (is (= ["aaa" "bbb" true "yyy"] (mapv #(get (d/entity @conn 1) %) names)))
        (is (nil? (:tags (d/entity @conn 1))))
        (d/close conn)
        (let [reopened (d/create-conn dir)]
          (try (is (= ["aaa" "bbb" true "yyy"]
                      (mapv #(get (d/entity @reopened 1) %) names)))
               (is (nil? (:tags (d/entity @reopened 1))))
               (finally (d/close reopened))))))))

(deftest collected-scalars-share-one-storage-preparation
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [before-tx (:max-tx @conn)
            prepared-counts (atom [])
            prepare @#'s/encode-group-storage-datoms
            reports (with-redefs-fn
                      {#'s/encode-group-storage-datoms
                       (fn [store-schema datoms]
                         (swap! prepared-counts conj (count datoms))
                         (prepare store-schema datoms))}
                      #(collected! conn
                                   (mapv (fn [value]
                                           (fn []
                                             (d/transact! conn [[:db/add 1 :value value]]
                                                          {:request value})))
                                         (range 1 9))))
            tx-ids (mapv #(get-in % [:tempids :db/current-tx]) reports)
            lmdb (d/datalog-kv conn)]
        (is (every? map? reports) (pr-str reports))
        (is (= [16] @prepared-counts))
        (is (= (vec (range (inc (long before-tx)) (+ (long before-tx) 9))) tx-ids))
        (is (= (mapv #(hash-map :request %) (range 1 9)) (mapv :tx-meta reports)))
        (doseq [[value report] (map vector (range 1 9) reports)]
          (is (= #{[(dec value) false] [value true]}
                 (set (map (juxt :v :added) (:tx-data report)))))
          (is (every? #(= (get-in report [:tempids :db/current-tx]) (:tx %))
                      (:tx-data report))))
        (is (= 8 (:value (d/entity @conn 1))))
        (is (= (last tx-ids) (:max-tx @conn)
               (i/get-value lmdb c/meta :max-tx :attr :long)))
        (is (= (i/last-modified (:store @conn))
               (i/get-value lmdb c/meta :last-modified :attr :long)))))))

(deftest collected-storage-encodes-once-after-dispatch
  (doseq [reuse? [false true]]
    (with-conn opts
      (fn [conn dir]
        (d/transact! conn [{:db/id 1 :value 0}])
        (let [events (atom [])
              unobserve (phase/observe! #(swap! events conj [%1 %2]))]
          (try
            (let [reports (binding [rmw/*reuse-datalog-member-setup?* reuse?]
                            (collected! conn
                                        (mapv (fn [value]
                                                #(d/transact! conn [[:db/add 1 :value value]]))
                                              (range 1 9))))
                  counts (frequencies (map first @events))
                  deferred (filter #(= :native-capture-deferred (first %)) @events)
                  prefix (filter #(= :native-rows-prefix (first %)) @events)
                  tail (filter #(= :native-rows-tail (first %)) @events)
                  ^java.util.List phases (mapv first @events)]
              (is (every? map? reports) (pr-str reports))
              (is (= 1 (:storage-tail-frozen counts)))
              (is (= 1 (:storage-encode-start counts)))
              (is (= 1 (:storage-encode-complete counts)))
              (is (zero? (:native-capture-eager counts 0)))
              (is (= [16] (map #(get-in % [1 :rows]) deferred)))
              (is (every? #(get-in % [1 :preparing?]) deferred))
              (is (empty? prefix))
              (is (= 18 (reduce + (map #(get-in % [1 :rows]) tail))))
              (is (< (.indexOf phases :worker-dispatch)
                     (.indexOf phases :storage-encode-start)))
              (doseq [[value report] (map vector (range 1 9) reports)]
                (is (= [[(dec value) false] [value true]]
                       (mapv (juxt :v :added) (:tx-data report)))))
              (is (= 8 (:value (d/entity @conn 1)))))
            (finally (unobserve))))
        (d/close conn)
        (let [reopened (d/create-conn dir)]
          (try (is (= 8 (:value (d/entity @reopened 1))))
               (finally (d/close reopened))))))))

(deftest deferred-storage-encoding-failure-aborts-before-publication
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [before @conn
            unobserve (phase/observe!
                        (fn [event _]
                          (when (= :storage-encode-start event)
                            (throw (IOException. "Deferred storage encoding failure")))))]
        (try
          (let [reports (collected! conn
                                   (mapv (fn [value]
                                           #(d/transact! conn [[:db/add 1 :value value]]))
                                         (range 1 5)))]
            (is (every? #(instance? Throwable %) reports) (pr-str reports))
            (is (identical? before @conn))
            (is (= 0 (:value (d/entity @conn 1)))))
          (finally (unobserve)))))))

(deftest storage-tail-prepared-kv-suffix-and-metadata-keep-their-order
  (with-conn opts
    (fn [conn dir]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [lmdb (d/datalog-kv conn)
            events (atom [])]
        (d/open-dbi lmdb "suffix")
        (let [unobserve (phase/observe! #(swap! events conj [%1 %2]))]
          (try
            (let [results (collected! conn
                                      [#(d/transact! conn [[:db/add 1 :value 1]])
                                       #(d/transact! conn [[:db/add 1 :value 2]])
                                       #(d/transact-kv lmdb [[:put "suffix" 1 99 :long :long]])])
                  prefix (filter #(= :native-rows-prefix (first %)) @events)
                  tail (filter #(= :native-rows-tail (first %)) @events)]
              (is (every? map? (take 2 results)) (pr-str results))
              (is (= :transacted (last results)))
              (is (empty? prefix))
              (is (= 7 (reduce + (map #(get-in % [1 :rows]) tail))))
              (is (= 2 (:value (d/entity @conn 1))))
              (is (= 99 (d/get-value lmdb "suffix" 1 :long :long))))
            (finally (unobserve)))))
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try
          (is (= 2 (:value (d/entity @reopened 1))))
          (is (= 99 (d/get-value (d/datalog-kv reopened) "suffix" 1 :long :long)))
          (finally (d/close reopened)))))))

(deftest wal-payload-encoding-overlaps-native-application
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [native-done (CountDownLatch. 1)
            native-thread (atom nil) encoding-thread (atom nil)
            uninstall (phase/observe!
                        (fn [event _]
                          (case event
                            :native-tail-start (reset! native-thread (Thread/currentThread))
                            :native-tail-applied (.countDown native-done)
                            :wal-encode-start
                            (do (reset! encoding-thread (Thread/currentThread))
                                (assert (.await native-done 5 TimeUnit/SECONDS)))
                            nil)))]
        (try
          (let [reports (collected! conn
                                   (mapv (fn [value]
                                           #(d/transact! conn [[:db/add 1 :value value]]))
                                         (range 1 9)))]
            (is (every? map? reports) (pr-str reports))
            (is (some? @encoding-thread))
            (is (not (identical? @native-thread @encoding-thread)))
            (is (= 8 (:value (d/entity @conn 1)))))
          (finally (uninstall)))))))

(deftest wal-payload-encoding-failure-aborts-the-applied-batch-and-fences
  (with-conn opts
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [before @conn
            state (wal/state (d/datalog-kv conn))
            next-lsn @(:next-lsn state)
            native-done (CountDownLatch. 1)
            uninstall (phase/observe!
                        (fn [event _]
                          (case event
                            :native-tail-applied (.countDown native-done)
                            :wal-encode-start
                            (do (assert (.await native-done 5 TimeUnit/SECONDS))
                                (throw (IOException. "Injected WAL payload failure")))
                            nil)))]
        (try
          (let [reports (collected! conn
                                   (mapv (fn [value]
                                           #(d/transact! conn [[:db/add 1 :value value]]))
                                         [1 2]))]
            (is (every? #(instance? Throwable %) reports) (pr-str reports))
            (is (identical? before @conn))
            (is (= 0 (:value (d/entity @conn 1))))
            (is (= next-lsn @(:next-lsn state))))
          (finally (uninstall)))
        (is (thrown? Exception (d/transact! conn [[:db/add 1 :value 3]])))))))

(deftest collected-giant-datoms-retain-cross-request-order
  (let [dir (u/tmp-dir (str "datalog-batched-giants-" (random-uuid)))
        conn (d/create-conn dir {:text {:db/valueType :db.type/string}} opts)
        prefix (.repeat "shared" 300)
        old (str prefix "old")
        final (str prefix "final")
        check! (fn [conn]
                 (let [lmdb (d/datalog-kv conn)]
                   (is (= final (:text (d/entity @conn 1))))
                   (is (= [[1 final]] (mapv (juxt :e :v) (d/datoms @conn :eav 1 :text))))
                   (is (= [[1 final]] (mapv (juxt :e :v) (d/datoms @conn :ave :text))))
                   (is (empty? (d/q '[:find ?e :in $ ?v :where [?e :text ?v]]
                                    @conn old)))
                   (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :text ?v]]
                                      @conn final)))
                   (is (= 1 (i/entries lmdb c/eav)))
                   (is (= 1 (i/entries lmdb c/ave)))
                   (is (pos? (i/entries lmdb c/giants)))))]
    (try
      (let [reports (collected! conn
                               [#(d/transact! conn [[:db/add 1 :text old]])
                                #(d/transact! conn [[:db/retract 1 :text old]])
                                #(d/transact! conn [[:db/add 1 :text final]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= [[old true] [old false] [final true]]
               (mapv (fn [report]
                       (let [datom (first (:tx-data report))]
                         [(:v datom) (:added datom)]))
                     reports))))
      (check! conn)
      (d/close conn)
      (let [reopened (d/create-conn dir)]
        (try (check! reopened) (finally (d/close reopened))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest caught-storage-drain-failures-still-abort-the-whole-batch
  (doseq [infrastructure? [false true]]
    (with-conn opts
      (fn [conn _]
        (d/transact! conn [{:db/id 1 :value 0}])
        (let [lmdb (d/datalog-kv conn)
              control (:independent-control @(i/kv-info lmdb))
              collector (:collector control)
              state (wal/state lmdb)
              before @conn
              before-lsn @(:next-lsn state)
              injected? (atom false)
              caught (atom [])
              successors (atom 0)
              failure (if infrastructure?
                        (IOException. "Forced late storage failure")
                        (ex-info "Forced late storage rejection" {}))
              prepare @#'s/encode-group-storage-datoms
              results
              (with-redefs-fn
                {#'s/encode-group-storage-datoms
                 (fn [store-schema datoms]
                   (if (compare-and-set! injected? false true)
                     (throw failure)
                     (prepare store-schema datoms)))}
                (fn []
                  (collected! conn
                              [#(d/transact! conn [[:db/add 1 :value 1]])
                               (fn []
                                 ((:internal-body! control)
                                  (fn [native]
                                    (try
                                      ((:native-prepare-flush! (meta native)))
                                      (catch Throwable t (swap! caught conj t)))
                                    [])
                                  {:datalog-conn conn :datalog-prepare? true}))
                               #(d/transact! conn
                                             [[:db.fn/call
                                               (fn [_]
                                                 (swap! successors inc)
                                                 [[:db/add 1 :value 2]])]])])))]
          (is @injected?)
          (is (= 1 (count @caught)))
          (is (every? #(instance? Throwable %) results) (pr-str results))
          (is (zero? @successors))
          (is (identical? before @conn))
          (is (= 0 (:value (d/entity @conn 1))))
          (is (= before-lsn @(:next-lsn state)) "no WAL append after the failed drain")
          (is (zero? (:requests (batch/usage collector))))
          (if infrastructure?
            (do
              (is (not (batch/serving? collector)))
              (is (thrown? Throwable (d/transact! conn [[:db/add 1 :value 3]]))))
            (do
              (is (batch/serving? collector))
              (is (map? (d/transact! conn [[:db/add 1 :value 3]])))
              (is (= 3 (:value (d/entity @conn 1)))))))))))

(deftest no-op-native-read-does-not-inherit-preceding-storage-write-count
  (with-conn (assoc opts :wal-durability-profile :relaxed :wal-shared? false
                        :wal-group-commit 64 :wal-group-commit-ms 0)
    (fn [conn _]
      (d/transact! conn [{:db/id 1 :value 0}])
      (let [lmdb (d/datalog-kv conn)
            state (wal/state lmdb)]
        (i/force-txlog-sync! lmdb)
        (let [before-lsn @(:next-lsn state)
              reports (collected! conn
                                  [#(d/transact! conn [[:db/add 1 :value 1]])
                                   #(d/transact! conn
                                                 [[:db.fn/call
                                                   (fn [tx-db]
                                                     (is (= 1 (:value (d/entity tx-db 1))))
                                                     [])]])
                                   #(d/transact! conn [[:db/add 1 :value 2]])])
              metrics (wal/sync-manager-state (:sync-manager state))]
          (is (every? map? reports) (pr-str reports))
          (is (empty? (:tx-data (second reports))))
          (is (= 2 (:value (d/entity @conn 1))))
          (is (= (inc (long before-lsn)) @(:next-lsn state)))
          (is (= 1 (:pending-count metrics)))
          (is (= 2 (:unsynced-count metrics))))))))

(deftest transaction-functions-can-read-their-own-native-kv-writes
  (with-conn opts
    (fn [conn _]
      (d/open-dbi (d/datalog-kv conn) "aux")
      (let [reports (collected! conn
                               [#(d/transact! conn [{:db/id 1 :value 1}])
                                #(d/transact! conn
                                              [[:db.fn/call
                                                (fn [tx-db]
                                                  (let [native (d/datalog-kv tx-db)]
                                                    (d/transact-kv native [[:put "aux" :key :value]])
                                                    (is (= :value (d/get-value native "aux" :key))))
                                                  [[:db/add 1 :value 2]])]])
                                #(d/transact! conn [[:db/add 1 :value 3]])])]
        (is (every? map? reports) (pr-str reports))
        (is (= 3 (:value (d/entity @conn 1))))
        (is (= :value (d/get-value (d/datalog-kv conn) "aux" :key)))))))

(defn- datoms [^long start ^long n]
  (mapv #(hash-map :db/id % :value %) (range start (+ start n))))

(deftest conditional-native-writes-cannot-enter-the-deferred-tail
  (let [avg (byte-array 4097)
        unconditional (l/datom-kv-tx 1 avg true false false)
        conditional (l/datom-kv-tx 1 avg true true false)
        prepare (fn [rows] (get (#'embedded/prepare nil nil rows :data :data) :native-tail-bytes 0))]
    (is (> (long (prepare [unconditional])) 4096))
    (is (zero? (long (prepare [conditional]))))
    (is (zero? (long (prepare [unconditional
                              [:put c/kv-info :probe 1 :keyword :long [:nooverwrite]]]))))))

(deftest large-frozen-datalog-tail-applies-while-wal-worker-is-running
  (with-conn opts
    (fn [conn dir]
      (d/transact! conn (datoms 1 1))
      (let [wal-started (CountDownLatch. 1) native-done (CountDownLatch. 1)
            bodies (atom 0) events (atom [])
            uninstall (phase/observe!
                        (fn [event _]
                          (swap! events conj event)
                          (case event
                            :native-tail-start
                            (is (.await wal-started 5 TimeUnit/SECONDS))
                            :native-tail-applied (.countDown native-done)
                            :wal-start
                            (do (.countDown wal-started)
                                (when-not (.await native-done 5 TimeUnit/SECONDS)
                                  (throw (ex-info "Native tail did not overlap WAL" {}))))
                            nil)))]
        (try
          (d/transact! conn [[:db.fn/call
                             (fn [_] (swap! bodies inc) (datoms 2 600))]])
          (is (= 1 @bodies))
          (is (= 1 (count (filter #{:worker-dispatch} @events))))
          (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (is (< (.indexOf ^java.util.List @events :native-applied)
                 (.indexOf ^java.util.List @events :native-committed)))
          (finally (uninstall)))
        (d/close conn)
        (let [reopened (d/create-conn dir)]
          (try (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @reopened)))
               (finally (d/close reopened))))))))

(deftest transaction-reads-flush-large-tails-and-small-writes-stay-inline
  (with-conn opts
    (fn [conn _]
      (let [dispatches (atom 0)
            uninstall (phase/observe! (fn [event _]
                                        (when (= :worker-dispatch event)
                                          (swap! dispatches inc))))]
        (try
          (d/transact! conn (datoms 1 1))
          (is (zero? @dispatches))
          (d/with-transaction [tx conn]
            (d/transact! tx (datoms 2 600))
            (is (= 601 (d/q '[:find (count ?e) . :where [?e :value]] @tx)))
            (d/transact! tx [[:db/add 2 :value 999]])
            (is (= 999 (:value (d/entity @tx 2)))))
          (is (zero? @dispatches) "a read consumes the deferred tail before dispatch")
          (is (= 999 (:value (d/entity @conn 2))))
          (finally (uninstall)))))))

(deftest deferred-native-tail-resizes-without-rerunning-the-body
  (with-conn (assoc opts :kv-opts {:mapsize 1})
    (fn [conn _]
      (let [bodies (atom 0) resizes (atom 0)
            uninstall (phase/observe! (fn [event _]
                                        (when (= :native-resized event)
                                          (swap! resizes inc))))]
        (try
          (d/transact! conn [[:db.fn/call
                             (fn [_] (swap! bodies inc) (datoms 1 30000))]])
          (is (= 1 @bodies))
          (is (pos? @resizes))
          (is (= 30000 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (finally (uninstall)))))))

(deftest wal-failure-aborts-an-applied-tail-without-publishing-the-connection
  (with-conn opts
    (fn [conn _]
      (d/transact! conn (datoms 1 1))
      (let [before @conn bodies (atom 0) native-done (CountDownLatch. 1)
            uninstall (phase/observe!
                        (fn [event _]
                          (case event
                            :native-tail-applied (.countDown native-done)
                            :wal-start
                            (do
                              (when-not (.await native-done 5 TimeUnit/SECONDS)
                                (throw (ex-info "Native tail was not applied" {})))
                              (throw (ex-info "Forced WAL failure" {})))
                            nil)))]
        (try
          (is (thrown? Exception
                       (d/transact! conn [[:db.fn/call
                                          (fn [_] (swap! bodies inc) (datoms 2 600))]])))
          (is (zero? (.getCount native-done)))
          (is (= 1 @bodies))
          (is (identical? before @conn))
          (is (= 1 (d/q '[:find (count ?e) . :where [?e :value]] @conn)))
          (finally (uninstall)))))))
