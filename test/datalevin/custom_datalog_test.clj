(ns datalevin.custom-datalog-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.bits :as bits]
            [datalevin.core :as d]
            [datalevin.custom-datalog :as cd]
            [datalevin.custom-value :as cv]
            [datalevin.db :as db]
            [datalevin.datom :as datom]
            [datalevin.dump :as dump]
            [datalevin.interpret :as inter]
            [datalevin.lmdb :as l]
            [datalevin.kv :as kv]
            [datalevin.udf :as udf]
            [datalevin.util :as u])
  (:import [java.util UUID]))

(def ^:dynamic *dir* nil)
(def ^:dynamic *handles* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "custom-datalog-" (UUID/randomUUID)))
              *handles* (atom [])
              c/*db-background-sampling?* false]
      (try (f)
           (finally
             (doseq [conn (reverse @*handles*)] (d/close conn))
             (u/delete-files *dir*))))))

(def task-type
  {:index {:type :long :order-fn (inter/inter-fn [v] (:rank v))}})

(def task-schema
  {:task/one {:db/valueType :app/task}
   :task/many {:db/valueType :app/task :db/cardinality :db.cardinality/many}
   :task/id {:db/valueType :app/task :db/unique :db.unique/identity}
   :task/unique {:db/valueType :app/task :db/unique :db.unique/value}
   :task/name {:db/valueType :db.type/string}
   :task/ref {:db/valueType :db.type/ref}})

(defn- connect
  ([name] (connect name false))
  ([name wal?]
   (let [conn (d/create-conn (str *dir* "/" name) nil {:wal? wal? :validate-data? true})]
     (swap! *handles* conj conn)
     conn)))

(defn- tasks [name wal?]
  (let [conn (connect name wal?)]
    (d/register-type conn :app/task task-type)
    (d/update-schema conn task-schema)
    conn))

(def a {:rank 1 :name "a"})
(def b {:rank 1 :name "b"})
(def c {:rank 2 :name "c"})

(deftest comparator-keys-follow-runtime-and-schema-changes
  (let [runtime (udf/create-registry)
        conn (d/create-conn (str *dir* "/comparator-bindings") nil
                            {:wal? false :runtime-opts {:udf-registry runtime}})
        _ (swap! *handles* conj conn)
        kv (d/datalog-kv conn)
        desc {:udf/lang :test :udf/kind :order-fn :udf/id :app/rank}
        schema (atom {:task/value {:db/valueType :app/task}})
        compare-values (cd/value-comparator kv #(deref schema))]
    (udf/register! runtime desc :rank)
    (d/register-type conn :app/task {:index {:type :long :order-fn desc}})
    (is (neg? (compare-values :task/value a c)))
    (is (neg? (compare-values :task/value a c)))
    (udf/register! runtime desc #(- (long (:rank %))))
    ;; Reuse the exact input objects whose prefixes were cached.
    (is (pos? (compare-values :task/value a c)))
    (udf/unregister! runtime desc)
    (is (thrown? Exception (compare-values :task/value a c)))
    (udf/register! runtime desc :rank)
    (is (neg? (compare-values :task/value a c)))
    (d/register-type conn :app/reverse
                     {:index {:type :long :order-fn (inter/inter-fn [v] (- (:rank v)))}})
    (swap! schema assoc-in [:task/value :db/valueType] :app/reverse)
    (is (pos? (compare-values :task/value a c)))
    (is (not (zero? (compare-values :task/value a b))))
    (is (zero? (compare-values :task/value a (into {} a))))))

(deftest datalog-custom-storage-and-queries
  (doseq [wal? [false true]]
    (let [conn (tasks (str "queries-" wal?) wal?)
          kv (d/datalog-kv conn)]
      (d/transact! conn [{:db/id 1 :task/one a :task/many [a b c] :task/name "first"}
                         {:db/id 2 :task/one b :task/name "second" :task/ref 1}])
      (is (= 5 (d/entries kv c/custom-values)))
      (is (= a (:task/one (d/entity @conn 1))))
      (is (= #{a b c} (set (:task/many (d/pull @conn '[*] 1)))))
      (is (= #{[1 a] [2 b]} (d/q '[:find ?e ?v :where [?e :task/one ?v]] @conn)))
      (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :task/one ?v]] @conn a)))
      (is (= #{[2]} (d/q '[:find ?e :in $ ?v :where [?e :task/one ?v]] @conn b)))
      (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :task/many ?v]] @conn b)))
      (is (= #{} (d/q '[:find ?v :in $ ?v :where [1 :task/one ?v]] @conn b)))
      (is (= #{[1 2]} (d/q '[:find ?e ?r :where [?e :task/one ?v]
                                           [?r :task/one ?w] [(not= ?v ?w)]
                                           [?r :task/ref ?e]] @conn)))
      (is (= [1 1 2] (mapv (comp :rank :v) (d/datoms @conn :ave :task/many))))
      (is (= #{a b} (set (map :v (d/index-range @conn :task/many a b)))))
      (is (= #{a b c} (set (map :v (d/datoms @conn :eav 1 :task/many)))))
      (is (= [a] (mapv :v (d/datoms @conn :eav 1 :task/many a))))
      (is (= [b] (mapv :v (d/datoms @conn :ave :task/one b))))
      (let [id (d/get-value kv c/kv-info cv/id-key :keyword :data)]
        (d/transact! conn [[:db/add 1 :task/many a] [:db/add 1 :task/one a]])
        (is (= id (d/get-value kv c/kv-info cv/id-key :keyword :data))))
      (d/transact! conn [[:db/retract 1 :task/many b] [:db/add 1 :task/one c]])
      (is (= #{a c} (set (:task/many (d/entity @conn 1)))))
      (is (= c (:task/one (d/entity @conn 1))))
      (is (= 4 (d/entries kv c/custom-values)))
      (d/transact! conn [[:db/retractEntity 1]])
      (is (= 1 (d/entries kv c/custom-values)))
      (d/close conn)
      (let [reopened (connect (str "queries-" wal?) wal?)]
        (is (= b (:task/one (d/entity @reopened 2))))
        (is (= 1 (d/entries (d/datalog-kv reopened) c/custom-values)))))))

(deftest datalog-custom-identity-and-transactions
  (let [conn (tasks "identity" true)
        kv (d/datalog-kv conn)]
    (d/transact! conn [{:db/id 1 :task/id a :task/unique a}
                       {:db/id 2 :task/id b :task/unique b}])
    (is (= 1 (d/entid @conn [:task/id a])))
    (is (= 2 (d/entid @conn [:task/id b])))
    (d/transact! conn [{:task/id a :task/name "upserted"}])
    (is (= "upserted" (:task/name (d/entity @conn 1))))
    (is (thrown? Exception (d/transact! conn [{:db/id 3 :task/unique a}])))
    (is (= 4 (d/entries kv c/custom-values)))
    (d/transact! conn [[:db/add 3 :task/many a] [:db/add 3 :task/many b]
                       [:db/retract 3 :task/many a] [:db/add 3 :task/many c]])
    (is (= #{b c} (set (:task/many (d/entity @conn 3)))))
    (is (thrown? Exception
                 (d/with-transaction [tx conn]
                   (d/transact! tx [[:db/add 3 :task/many a]])
                   (is (= #{a b c} (set (:task/many (d/entity @tx 3)))))
                   (throw (ex-info "rollback" {})))))
    (is (= #{b c} (set (:task/many (d/entity @conn 3)))))
    (is (= 6 (d/entries kv c/custom-values)))
    (is (thrown? Exception
                 (d/transact! conn [[:db/add 3 :task/many a]
                                    [:db/add 4 :task/one {:rank "invalid"}]])))
    (is (= #{b c} (set (:task/many (d/entity @conn 3)))))
    (is (= 6 (d/entries kv c/custom-values)))))

(deftest custom-pending-values-and-uniqueness-changes
  (let [conn (tasks "pending" false)
        kv (d/datalog-kv conn)]
    (d/transact! conn [[:db/add 1 :task/many a]])
    (let [report (d/tx-data->simulated-report
                  @conn [[:db/add 1 :task/many c] [:db/add 1 :task/many b]])
          pending (:db-after report)]
      (is (= #{b c} (set (map :v (:eavt pending)))))
      (is (= #{a b} (set (map :v (db/-range-datoms pending :ave
                                   (datom/datom c/e0 :task/many a)
                                   (datom/datom c/emax :task/many b))))))
      (is (= [1 1 2] (mapv (comp :rank :v)
                           (db/-range-datoms pending :ave
                             (datom/datom c/e0 :task/many c/v0)
                             (datom/datom c/emax :task/many c/vmax)))))
      (is (= #{a} (set (:task/many (d/entity @conn 1)))))
      (is (= 1 (d/entries kv c/custom-values))))
    ;; The equal a values are separated by b within their shared order bucket.
    (d/transact! conn [[:db/add 2 :task/many b] [:db/add 3 :task/many a]])
    (is (thrown? Exception
                 (d/update-schema conn {:task/many {:db/unique :db.unique/value}})))
    (d/transact! conn [[:db/retract 3 :task/many a]])
    (d/update-schema conn {:task/many {:db/unique :db.unique/value}})
    (is (thrown? Exception (d/transact! conn [[:db/add 4 :task/many a]])))
    (is (= #{[1 2]} (d/q '[:find ?x ?y :where [?x :task/many ?a]
                                             [?y :task/many ?b]
                                             [(not= ?a ?b)] [(< ?x ?y)]] @conn)))))

(deftest custom-ranges-and-truncated-order-keys
  (let [conn (connect "ranges")]
    (d/register-type conn :app/number {:index {:type :long :order-fn (inter/inter-fn [v] v)}})
    (d/register-type conn :app/text {:index {:type :string :order-fn (inter/inter-fn [v] (:key v))}})
    (d/update-schema conn {:number {:db/valueType :app/number}
                          :text {:db/valueType :app/text :db/cardinality :db.cardinality/many}})
    (d/transact! conn (mapv (fn [e v] [:db/add e :number v]) (range 1 5) [-2 0 2 5]))
    (is (= #{[3] [4]} (d/q '[:find ?e :where [?e :number ?v] [(> ?v 0)]] @conn)))
    (is (= #{[2] [3]} (d/q '[:find ?e :where [?e :number ?v] [(<= 0 ?v 2)]] @conn)))
    (is (= [0 2] (mapv :v (d/index-range @conn :number 0 2))))
    (let [prefix (apply str (repeat 900 "x"))
          x {:key (str prefix "a") :data 1}
          y {:key (str prefix "b") :data 2}]
      (d/transact! conn [[:db/add 1 :text x] [:db/add 1 :text y]])
      (is (= [y] (mapv :v (d/datoms @conn :eav 1 :text y))))
      (d/transact! conn [[:db/retract 1 :text x]])
      (is (= #{y} (set (:text (d/entity @conn 1))))))))

(deftest custom-datalog-physical-replay
  (let [source (tasks "replay-source" true)
        source-kv (d/datalog-kv source)
        dest (d/open-kv (str *dir* "/replay-dest") {:wal? false})]
    (try
      (d/transact! source [{:db/id 1 :task/one a :task/many [a b]}])
      (d/transact! source [[:db/add 1 :task/one c] [:db/retract 1 :task/many a]])
      (doseq [{:keys [rows lsn]} (kv/open-tx-log-rows source-kv 0)]
        (kv/replay-txlog-rows! dest rows lsn))
      (is (= 2 (d/entries dest c/custom-values)))
      (finally (d/close-kv dest)))
    (let [replayed (connect "replay-dest")]
      (is (= c (:task/one (d/entity @replayed 1))))
      (is (= #{b} (set (:task/many (d/entity @replayed 1))))))))

(deftest custom-datalog-reader-keeps-payload-snapshot
  (let [registry (udf/create-registry)
        conn (d/create-conn (str *dir* "/snapshot") nil
                            {:wal? false :runtime-opts {:udf-registry registry}})
        serde-desc (fn [kind] {:udf/lang :test :udf/id :app/snapshot
                              :udf/kind kind :udf/version 1})
        entered (promise)
        resume (promise)
        first-read? (atom true)]
    (swap! *handles* conj conn)
    (udf/register! registry (serde-desc :serializer) bits/serialize)
    (udf/register! registry (serde-desc :deserializer)
                   (fn [payload]
                     (when (compare-and-set! first-read? true false)
                       (deliver entered true)
                       (deref resume 10000 nil))
                     (bits/deserialize payload)))
    (d/register-type conn :app/task
                     (assoc task-type :payload {:serialize (serde-desc :serializer)
                                                :deserialize (serde-desc :deserializer)}))
    (d/update-schema conn task-schema)
    (d/transact! conn [[:db/add 1 :task/one a] [:db/add 2 :task/one b]])
    (let [reader (future (mapv :v (d/datoms @conn :ave :task/one)))]
      (try
        (is (= true (deref entered 10000 :timeout)))
        (d/transact! conn [[:db/retractEntity 1] [:db/retractEntity 2]])
        (is (zero? (d/entries (d/datalog-kv conn) c/custom-values)))
        (deliver resume true)
        ;; The second payload must still be visible after both are deleted.
        (is (= [a b] (deref reader 10000 :timeout)))
        (finally (deliver resume true) (future-cancel reader))))))

(deftest datalog-custom-schema-validation
  (let [conn (connect "schema")]
    (is (thrown-with-msg? Exception #"not registered"
                         (d/update-schema conn task-schema)))
    (is (nil? (:task/one (d/schema conn))))
    (d/register-type conn :app/task task-type)
    (d/update-schema conn task-schema)
    (d/transact! conn [[:db/add 1 :task/one a]])
    (is (thrown? Exception (d/update-schema conn {:task/one {:db/valueType :db.type/string}})))
    (is (= :app/task (get-in (d/schema conn) [:task/one :db/valueType])))))

(deftype OpaqueTask [^long rank label]
  Object
  (hashCode [_] 7)
  (equals [_ other]
    (and (instance? OpaqueTask other)
         (= rank (.-rank ^OpaqueTask other))
         (= label (.-label ^OpaqueTask other)))))

(defn- descriptor [kind]
  {:udf/lang :test :udf/id :app/opaque :udf/kind kind :udf/version 1})

(deftest custom-serde-with-non-comparable-values
  (let [registry (udf/create-registry)
        conn (d/create-conn (str *dir* "/opaque") nil
                            {:wal? false :runtime-opts {:udf-registry registry}})
        x (OpaqueTask. 1 "x")
        y (OpaqueTask. 1 "y")
        z (OpaqueTask. 2 "z")]
    (swap! *handles* conj conn)
    (udf/register! registry (descriptor :order-fn) #(.-rank ^OpaqueTask %))
    (udf/register! registry (descriptor :serializer)
                   #(bits/serialize [(.-rank ^OpaqueTask %) (.-label ^OpaqueTask %)]))
    (udf/register! registry (descriptor :deserializer)
                   #(let [[rank label] (bits/deserialize %)] (OpaqueTask. rank label)))
    (d/register-type conn :app/opaque
                     {:index {:type :long :order-fn (descriptor :order-fn)}
                      :payload {:serialize (descriptor :serializer)
                                :deserialize (descriptor :deserializer)}})
    (d/update-schema conn {:item/value {:db/valueType :app/opaque
                                       :db/cardinality :db.cardinality/many}
                          :item/id {:db/valueType :app/opaque :db/unique :db.unique/identity}})
    (d/transact! conn [[:db/add 1 :item/value x] [:db/add 1 :item/value y]
                       [:db/add 1 :item/value z] [:db/add 1 :item/value (OpaqueTask. 1 "x")]
                       [:db/add 2 :item/id x] [:db/add 3 :item/id y]])
    (is (= #{x y z} (set (:item/value (d/entity @conn 1)))))
    (is (= [1 1 2] (mapv #(.-rank ^OpaqueTask (:v %)) (d/datoms @conn :ave :item/value))))
    (is (= 2 (d/entid @conn [:item/id (OpaqueTask. 1 "x")])))
    (is (= 3 (d/entid @conn [:item/id y])))
    (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :item/value ?v]] @conn y)))
    (is (= #{[1 2] [1 3]}
           (d/q '[:find ?e ?i :where [?e :item/value ?v] [?i :item/id ?v]] @conn)))
    (d/transact! conn [[:db/retract 1 :item/value y] [:db/add 1 :item/value x]])
    (is (= #{x z} (set (:item/value (d/entity @conn 1)))))
    (is (= #{[2]} (d/q '[:find ?i :in $ ?e :where [?e :item/value ?v]
                                                        [?i :item/id ?v]] @conn 1)))
    (is (= 4 (d/entries (d/datalog-kv conn) c/custom-values)))
    (udf/register! registry (descriptor :serializer)
                   (fn [^OpaqueTask v]
                     (when (= "bad" (.-label v)) (throw (ex-info "bad payload" {})))
                     (bits/serialize [(.-rank v) (.-label v)])))
    (is (thrown? Exception
                 (d/transact! conn [[:db/add 1 :item/value (OpaqueTask. 2 "new")]
                                    [:db/add 1 :item/value (OpaqueTask. 2 "bad")]])))
    (is (= #{x z} (set (:item/value (d/entity @conn 1)))))
    (is (= 4 (d/entries (d/datalog-kv conn) c/custom-values)))))

(deftest custom-datalog-backups
  (let [conn (tasks "backup" false)
        kv (d/datalog-kv conn)]
    (d/open-dbi kv "custom-keys" {:key-type :app/task})
    (d/transact-kv kv "custom-keys" [[:put c :key-payload]])
    (d/transact! conn [{:db/id 1 :task/one a :task/many [a b]}])
    (let [last-id (d/get-value kv c/kv-info cv/id-key :keyword :data)
          ^String text (with-out-str (dump/dump-datalog conn))]
      (is (< (.indexOf text c/kv-info) (.indexOf text c/custom-values) (.indexOf text c/ave)))
      (dump/load-datalog (str *dir* "/restored")
                         (java.io.PushbackReader. (java.io.StringReader. text)) nil nil)
      (let [restored (connect "restored")
            restored-kv (d/datalog-kv restored)]
        (is (= #{a b} (set (:task/many (d/entity @restored 1)))))
        (is (= 3 (d/entries restored-kv c/custom-values)))
        (is (not (some #{"custom-keys"} (d/list-dbis restored-kv))))
        (is (= last-id (d/get-value restored-kv c/kv-info cv/id-key :keyword :data)))
        (d/transact! restored [[:db/add 1 :task/many c]])
        (is (> (long (d/get-value restored-kv c/kv-info cv/id-key :keyword :data))
               (long last-id)))))
    (let [text (with-out-str (l/dump-all kv))
          path (str *dir* "/all-restored")
          target (d/open-kv path {:wal? false})]
      (try (l/load-all target (java.io.PushbackReader. (java.io.StringReader. text)))
           (finally (d/close-kv target)))
      (let [restored (connect "all-restored")]
        (is (= a (:task/one (d/entity @restored 1))))
        (is (= :key-payload (d/get-value (d/datalog-kv restored) "custom-keys" c)))))
    (let [buffer (java.io.ByteArrayOutputStream.)
          out (java.io.DataOutputStream. buffer)]
      (dump/dump-datalog conn out)
      (.flush out)
      (dump/load-datalog
       (str *dir* "/binary-restored")
       (java.io.DataInputStream. (java.io.ByteArrayInputStream. (.toByteArray buffer)))
       nil nil true)
      (let [restored (connect "binary-restored")]
        (is (= a (:task/one (d/entity @restored 1))))
        (is (= #{a b} (set (:task/many (d/entity @restored 1)))))))))
