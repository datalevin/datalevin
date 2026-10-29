(ns datalevin.db.index-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.native-value :as nv]
            [datalevin.test.core :refer [db-fixture]])
  (:import [java.util Date]))

(use-fixtures :each db-fixture)

(defn- with-cache [f]
  (let [store (reify i/IStore
                (dir [_] (str ::index-cache))
                (opts [_] {:cache-limit 64}))]
    (db/refresh-cache store 1)
    (try (f store) (finally (db/remove-cache store)))))

(deftest index-cache-preserves-storage-representations-and-invalidation
  (doseq [[stored missing] [[[1 2] '(1 2)]
                            [{:key [1 2]} {:key '(1 2)}]
                            [1.0M 1.00M]]
          pattern [(fn [v] [:search nil :value v])
                   (fn [v] [:search-tuples nil :value v])
                   (fn [v] [:first nil :value v])
                   (fn [v] [:count nil :value v nil])
                   (fn [v] [:populated? :ave :value v nil])
                   (fn [v] [:datoms :ave :value v nil])
                   (fn [v] [:datoms :eav 1 :value v])
                   (fn [v] [:datoms :ave :value v nil 1])
                   (fn [v] [:av-datoms :value v])
                   (fn [v] [:seek :ave :value v nil])
                   (fn [v] [:rseek :ave :value v nil 1])
                   (fn [v] [:index-range :value v v])
                   (fn [v] [:index-range-size :value v v])
                   (fn [v] [:range-datoms :ave
                            (datom/datom 0 :value v) (datom/datom 100 :value v)])]]
    (testing (str (first (pattern stored)) " " stored " / " missing)
      (with-cache
        (fn [store]
          (let [calls (atom 0)
                stored-pattern (pattern stored)
                missing-pattern (pattern missing)
                read (fn [input result]
                       (let [key (db/index-cache-key store input)]
                         (if-some [cached (when key (db/cache-get store key))]
                           cached
                           (do
                             (swap! calls inc)
                             (when key (db/cache-put store key result))
                             result))))]
            (is (= stored-pattern missing-pattern))
            (is (= [:stored] (read stored-pattern [:stored])))
            (is (= [] (read missing-pattern [])))
            (is (= [:stored] (read stored-pattern :unexpected)))
            (is (= [] (read missing-pattern :unexpected)))
            (is (= 2 @calls))
            (#'db/invalidate-cache store [(datom/datom 1 :value stored)] 1 2)
            (is (= [:updated] (read stored-pattern [:updated])))
            (is (= [] (read missing-pattern [])))
            (is (<= 3 @calls 4))
            (#'db/invalidate-cache store [(datom/datom 1 :value missing)] 1 3)
            (is (= [:created] (read missing-pattern [:created])))))))))

(defrecord OpaqueValue [rank])

(deftest ordinary-and-opaque-index-cache-inputs
  (with-cache
    (fn [store]
      (doseq [key [[:datoms :ave :value 1 nil]
                   [:search 1 :value "one"]
                   [:cardinality :value]
                   [:query-result {:attrs #{:value}} :parsed :inputs]]]
        (is (identical? key (db/index-cache-key store key))))
      (doseq [value [(->OpaqueValue 1) (Object.)]]
        (is (nil? (db/index-cache-key store [:search nil :value value])))))))

(def ^:private mutable-lookups
  [[:bytes :db.type/bytes #(byte-array [1 2])
    #(aset-byte ^bytes % 0 (byte 9)) #(aset-byte ^bytes % 0 (byte 1))]
   [:instant :db.type/instant #(Date. 1234)
    #(.setTime ^Date % 9) #(.setTime ^Date % 1234)]])

(deftest mutable-index-keys-retain-owned-identity-and-invalidation-values
  (doseq [[kind _ make-value mutate! restore!] mutable-lookups
          indexed? [false true]]
    (testing (str kind ", dependency index initialized: " indexed?)
      (with-cache
        (fn [store]
          (let [input (make-value)
                key (db/index-cache-key store [:datoms :ave :value input nil])]
            (db/cache-put store key [:stored])
            (is (= [:stored]
                   (db/cache-get store
                                 (db/index-cache-key store
                                                     [:datoms :ave :value (make-value) nil]))))
            (when indexed?
              (#'db/invalidate-cache store [(datom/datom 2 :other 1)] 2 2))
            (mutate! input)
            (is (= [:stored] (db/cache-get store key)))
            (#'db/invalidate-cache store [(datom/datom 1 :value (make-value))] 3 3)
            (restore! input)
            (is (nil? (db/cache-get store key)))
            (is (nil? (db/cache-get store
                                   (db/index-cache-key store
                                                       [:datoms :ave :value input nil]))))))))))

(def ^:private mutable-index-reads
  [[:eav #(d/datoms %1 :eav 1 :value %2)]
   [:ave #(d/datoms %1 :ave :value %2)]
   [:search #(d/search-datoms %1 nil :value %2)]
   [:fetch #(d/search-datoms %1 1 :value %2)]
   [:av-datoms #(db/-av-datoms %1 :value %2)]
   [:index-range #(d/index-range %1 :value %2 %2)]
   [:range-datoms #(db/-range-datoms %1 :ave
                                    (datom/datom c/e0 :value %2)
                                    (datom/datom c/emax :value %2))]])

(deftest mutating-lookup-inputs-cannot-hide-writes-from-invalidation
  (doseq [[kind value-type make-value mutate! restore!] mutable-lookups
          [operation read] mutable-index-reads
          transition [:insert :retract]]
    (testing (str kind " " operation " " transition)
      (with-open [^java.io.Closeable conn
                  (d/create-conn nil {:value {:db/valueType value-type}}
                                 {:background-sampling? false
                                  :kv-opts {:inmemory? true}})]
        (when (= :retract transition)
          (d/transact! conn [{:db/id 1 :value (make-value)}]))
        (let [view @conn
              input (make-value)
              before (if (= :insert transition) [] [1])
              after (if (= :insert transition) [1] [])]
          (dotimes [_ 2] (is (= before (mapv :e (read view input)))))
          (mutate! input)
          (d/transact! conn (if (= :insert transition)
                             [{:db/id 1 :value (make-value)}]
                             [[:db/retractEntity 1]]))
          (restore! input)
          (dotimes [_ 2] (is (= after (mapv :e (read view input))))))))))

(defn- with-remote-index [f]
  (let [rows (atom [(datom/datom 1 :value "original")])
        cacheable? (atom true)
        on-read (atom (fn []))
        reads (atom 0)
        location (str "dtlv://index-policy/" (random-uuid))
        store (reify i/IStore
                (dir [_] location)
                (opts [_] {:cache-limit 64})
                (slice [_ _ _ _]
                  (swap! reads inc)
                  (@on-read)
                  @rows)
                i/IRemoteDB
                (db-info [_] {:last-modified 1 :max-tx 1})
                i/IRemoteReadCache
                (prepare-remote-read-cache! [_])
                (remote-read-cacheable? [_] @cacheable?))
        database (db/map->DB {:store store :max-eid 1 :max-tx 1})]
    (db/refresh-cache store 1)
    (try
      (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
        (f {:store store :read #(d/datoms database :eav 1)
            :rows rows :cacheable? cacheable? :on-read on-read :reads reads}))
      (finally (db/remove-cache store)))))

(deftest remote-index-hits-obey-custom-type-and-receiver-policies
  (doseq [policy [:custom-type :receiver]]
    (testing (name policy)
      (with-remote-index
        (fn [{:keys [read rows cacheable? reads]}]
          (let [original (read)]
            (is (identical? original (read)))
            (is (= 1 @reads))
            (reset! rows [(datom/datom 1 :value "rebound")])
            (when (= :custom-type policy) (reset! cacheable? false))
            (binding [nv/*wire-reader* (when (= :receiver policy) (fn [_ _]))]
              (dotimes [_ 2] (is (= ["rebound"] (mapv :v (read))))))
            (is (= 3 @reads))
            (reset! cacheable? true)
            (reset! rows original)
            (is (identical? original (read)))
            (is (= 3 @reads))))))))

(deftest remote-index-publication-rechecks-decoder-policy
  (with-remote-index
    (fn [{:keys [store read rows cacheable? on-read reads]}]
      (reset! on-read #(reset! cacheable? false))
      (is (= ["original"] (mapv :v (read))))
      (is (nil? (db/cache-get store [:datoms :eav 1 nil nil])))
      (reset! on-read (fn []))
      (reset! cacheable? true)
      (reset! rows [(datom/datom 1 :value "current")])
      (let [current (read)]
        (is (= ["current"] (mapv :v current)))
        (is (identical? current (read)))
        (is (= 2 @reads))))))
