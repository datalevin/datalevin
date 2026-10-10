(ns ycsb-bench.application-key-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as d]
            [datalevin.pull-api :as pull]
            [datalevin.storage.entity :as entity]
            [datalevin.storage :as storage]
            [datalevin.storage.scan :as scans]
            [ycsb-bench.runner :as runner]
            [ycsb-bench.sql :as sql]
            [ycsb-bench.store :as store]
            [ycsb-bench.workload :as w]))

(def options
  (runner/options {:workload :e :records 30
                   :ops 200 :warmup 20 :threads 8 :pool-size 8
                   :field-count 12 :field-length 4 :scan-length 5
                   :server-mode :in-process :datalog-handles :independent}))

(def conditions
  (cond-> [[:datalevin :kv :embedded] [:datalevin :datalog :embedded]
           [:datalevin :kv :remote] [:datalevin :datalog :remote]
           [:sqlite :kv :embedded] [:sqlite :datalog :embedded]]
    (System/getenv "YCSB_PG_URL")
    (into [[:postgres :kv :remote] [:postgres :datalog :remote]])))

(defn- with-store [opts f]
  ((if (= :datalevin (:system opts)) store/with-store sql/with-store) opts f))

(defn- values [id]
  (mapv #(format "%04d" (+ (* 100 id) %)) (range 12)))

(deftest unique-string-keys-scramble-insertion-order
  (let [keys (mapv w/application-key (range 10000))]
    (is (= 10000 (count (distinct keys))))
    (is (every? #(re-matches #"user[0-9]+" %) keys))
    (is (not= keys (vec (sort keys))))
    (is (= (str "user" (w/fnvhash64 0)) (w/application-key 0)))
    (is (string? (w/application-key (dec Integer/MAX_VALUE))))))

(deftest upstream-generator-reference-values
  ;; Hash outputs from the unmodified upstream Utils.java; rank outputs from
  ;; ZipfianGenerator's Java formula with fixed uniform draws (theta=0.99).
  (doseq [[ordinal hash] [[0 6284781860667377211] [1 8517097267634966620]
                          [2 1820151046732198393] [255 8064062821143829734]
                          [256 2056600594528442646] [99999 7592201923306675823]
                          [2147483646 3408653790469006400]]]
    (is (= hash (w/fnvhash64 ordinal)))
    (is (= (str "user" hash) (w/application-key ordinal))))
  (doseq [[u rank ordinal] [[0.0 0 42439] [0.01 0 42439] [0.04 1 91481]
                            [0.1 6 29960] [0.5 134552 14271]
                            [0.9 1170869537 25774] [0.999999 9999787803 68880]]]
    (is (= rank (w/scrambled-rank u)))
    (is (= ordinal (rem (w/fnvhash64 (w/scrambled-rank u)) 100001)))))

(deftest scrambled-keyspace-stays-fixed-as-commits-advance
  (let [draws (atom [0.0 0.04])
        rng (proxy [java.util.Random] []
              (nextDouble [] (let [u (first @draws)] (swap! draws next) u)))]
    (is (= 42439 (w/choose-key rng :zipfian 100001 50000)))
    (is (= 91481 (w/choose-key rng :zipfian 100001 100000))))
  (let [draws (atom [0.04 0.0])
        rng (proxy [java.util.Random] []
              (nextDouble [] (let [u (first @draws)] (swap! draws next) u)))]
    (is (= 42439 (w/choose-key rng :zipfian 100001 50000))
        "Reject an uncommitted ordinal, then redraw with the same modulus")
    (is (nil? @draws))))

(deftest ordered-string-pages-with-independent-entity-ids
  (doseq [workload [:d :e]
          [system api mode] conditions]
    (testing (str [system api mode workload])
      (with-store
        (assoc options :system system :api api :mode mode :workload workload)
        (fn [group]
          (let [db (store/for-worker group 0)
                other (store/for-worker group 1)
                info (store/storage-info group)
                ;; Lexical order differs from insertion order and numeric order.
                records [["user90" (values 0)] ["user40" (values 1)]
                         ["user10" (values 2)] ["user30" (values 3)]
                         ["user2" (values 4)]]]
            (store/put-records! db records)
            (is (= 5 (store/record-count db)))
            (is (= (if (= workload :d) :application-key-latest-v1 :application-key-range-v1)
                   (:workload-model info)))
            (is (= (if (= system :datalevin) :ycsb/key :YCSB_KEY) (:record-key info)))
            (is (= :string (:key-type info)))
            (is (= :none (:payload-indexes info)))
            (doseq [[key fields] records]
              (is (= fields (store/read-record db key))))
            (doseq [start ["" "user10" "user11" "user30" "user31" "user40" "user90" "zz"]
                    n (range 1 6)]
              (let [expected (vec (take n (filter #(not (neg? (compare (first %) start)))
                                                   (sort-by first records))))]
                (is (= expected (store/scan-records db start n)) (str [start n]))))
            (is (empty? (store/scan-records db "user40" 0)))
            (when (and (= system :datalevin) (= api :datalog))
              (let [conn (:conn db)
                    ids (mapv #(:db/id (d/pull @conn [:db/id] [:ycsb/key %]))
                              (sort (map first records)))]
                (is (every? integer? ids))
                (is (not= ids (vec (sort ids))) "Key order must not follow entity IDs")
                (is (= {:db/valueType :db.type/string :db/unique :db.unique/value}
                       (select-keys (get (d/schema conn) :ycsb/key) [:db/valueType :db/unique])))
                (is (nil? (get (d/schema conn) :post/key)))))
            (when (#{:sqlite :postgres} system)
              (is (= [] (get-in info [:configuration :secondary-indexes])))
              (is (= :database-default (:key-collation info))))
            ;; A new key can land inside a previously scanned page.
            (store/put-records! other [["user50" (values 5)]])
            (with-redefs [d/prepare-q (fn [& _] (throw (AssertionError. "Reprepared inside scan")))]
              (is (= [["user40" (values 1)] ["user50" (values 5)] ["user90" (values 0)]]
                     (store/scan-records db "user40" 3)))
              (is (= [["user50" (values 5)]] (store/scan-records db "user41" 1))))
            (is (= 6 (store/record-count db))))
          {})))))

(deftest prepared-field-scans-do-not-materialize-entities
  (doseq [mode [:embedded :remote]]
    (testing (str mode)
      (with-store
        (assoc options :system :datalevin :api :datalog :mode mode
               :threads 2 :pool-size 2)
        (fn [group]
          (let [db (store/for-worker group 0)
                other (store/for-worker group 1)
                ;; Identical payloads must remain distinct rows; key order
                ;; differs from insertion order and internal entity order.
                records [["user90" (values 0)] ["user10" (values 0)]
                         ["user30" (values 1)] ["user2" (values 2)]]
                attributes (:attributes db)
                pull-many pull/pull-many
                select-entities entity/select-entities
                eav-scan scans/eav-scan-v-list-chunk
                encoded-reader storage/prepare-encoded-field-reader
                entity-keys (atom {})
                projected (atom [])
                fail (fn [& _] (throw (AssertionError. "Scan materialized an entity")))
                scan (fn [start n]
                       (reset! projected [])
                       ;; Remote permission checks may read unrelated system
                       ;; entities; only reject materializing benchmark fields.
                       (let [rows
                             (with-redefs [storage/prepare-encoded-field-reader
                                           (fn [schema attrs]
                                             (when-let [reader (encoded-reader schema attrs)]
                                               (fn [view ids]
                                                 (when (= (set attributes) (set attrs))
                                                   (swap! projected into (map @entity-keys ids)))
                                                 (reader view ids))))
                                           scans/eav-scan-v-list-chunk
                                           (fn [lmdb tuples eid-idx attrs-v & args]
                                             (when (= (set attributes)
                                                      (set (map first attrs-v)))
                                               (swap! projected into
                                                      (map #(aget ^objects % 1) tuples)))
                                             (apply eav-scan lmdb tuples eid-idx attrs-v args))
                                           pull/pull-many
                                           (fn [view pattern ids]
                                             (when (= attributes pattern) (fail))
                                             (pull-many view pattern ids))
                                           entity/select-entities
                                           (fn [lmdb ids names aids id?]
                                             (when (= (set attributes) (set names)) (fail))
                                             (select-entities lmdb ids names aids id?))]
                               (store/scan-records db start n))]
                         (is (= (sort (map first rows)) (sort @projected))
                             "Only keys selected by the limited AVE access reach field projection")
                         rows))]
            (store/put-records! db records)
            (reset! entity-keys
                    (into {} (map (fn [[key _]]
                                    [(:db/id (d/pull @(:conn db) [:db/id] [:ycsb/key key])) key])
                                  records)))
            (is (= :fields (:scan-projection (store/storage-info db))))
            (is (= (vec (sort-by first records)) (scan "" 100)))
            (is (= [["user2" (values 2)] ["user30" (values 1)]] (scan "user11" 2)))
            (is (empty? (scan "zz" 3)))
            (store/update-field! other "user30" 10 "edit")
            (store/put-records! other [["user25" (values 3)]])
            (swap! entity-keys assoc
                   (:db/id (d/pull @(:conn db) [:db/id] [:ycsb/key "user25"])) "user25")
            (is (= [["user25" (values 3)] ["user30" (assoc (values 1) 10 "edit")]]
                   (scan "user21" 2))))
          {})))))

(deftest concurrent-string-key-load-warmup-measurement-and-validation
  (doseq [workload [:d :e]
          [system api mode] conditions]
    (testing (str [system api mode workload])
      (let [result (runner/run-case! (assoc options :system system :api api :mode mode
                                                  :workload workload
                                                  :value-audit? true))
            inserts (get-in result [:measured :by-operation :insert :count] 0)]
        (is (pos? inserts))
        (is (= (if (= workload :d) :application-key-latest-v1 :application-key-range-v1)
               (get-in result [:configuration :workload-model])))
        (is (= :ycsb-fnv64-decimal (get-in result [:configuration :key-generator])))
        (is (= :passed (get-in result [:validation :status])))
        (is (= :passed (get-in result [:validation :value-checks :status])))
        (is (= (+ 30 inserts) (get-in result [:validation :records])))
        (is (= 200 (get-in result [:measured :operations])))
        (is (pos? (get-in result [:measured :by-operation
                                  (if (= workload :d) :read :scan) :count])))))))

(deftest latest-reads-use-client-generated-string-keys
  (let [opts (assoc options :workload :d :distribution :latest)
        space (w/keyspace 2)
        rng (proxy [java.util.Random] [17] (nextDouble [] 0.0))
        calls (atom [])
        db (reify store/Records
             (read-record [_ key]
               (swap! calls conj [:read key])
               (values 0))
             (put-records! [_ records]
               (swap! calls conj [:insert (ffirst records)])))
        cdf (w/zipf-cdf 3)]
    (#'runner/execute! db space rng cdf opts :read)
    (#'runner/execute! db space rng cdf opts :insert)
    (#'runner/execute! db space rng cdf opts :read)
    ;; Ordinal 2 is newer, although its hashed key sorts before ordinal 1.
    (is (= [[:read "user8517097267634966620"]
            [:insert "user1820151046732198393"]
            [:read "user1820151046732198393"]]
           @calls))
    (is (= 3 (:visible @space)))))

(deftest duplicate-string-keys-reject-and-roll-back-batches
  (doseq [workload [:d :e]
          durability [:strict :relaxed]
          [system api mode] conditions
          :when (= system :datalevin)]
    (testing (str [system api mode workload durability])
      (with-store
        (assoc options :system system :api api :mode mode :workload workload
               :durability durability :threads 2 :pool-size 2)
        (fn [group]
          (let [db (store/for-worker group 0)
                other (store/for-worker group 1)
                a (w/application-key 0)
                b (w/application-key 1)
                original (values 0)]
            (is (= :reject-duplicates (:insert-semantics (store/storage-info group))))
            (store/put-records! db [[a original]])
            (doseq [records [[[a (values 1)]]
                             [[b (values 1)] [a (values 2)]]
                             [[b (values 1)] [b (values 2)]]]]
              (is (thrown? Exception (store/put-records! other records)))
              (is (= original (store/read-record db a)))
              (is (= 1 (store/record-count db)))
              (is (= [[a original]] (store/scan-records db "" 3))
                  "No earlier row from a failed batch may remain"))
            (store/put-records! other [[b (values 3)]])
            (is (= (values 3) (store/read-record db b)))
            (is (= 2 (store/record-count db))))
          {})))))

(deftest concurrent-inserts-of-one-string-key-preserve-batch-atomicity
  (doseq [workload [:d :e]
          [system api mode] conditions]
    (testing (str [system api mode workload])
      (with-store
        (assoc options :system system :api api :mode mode :workload workload
               :threads 2 :pool-size 2)
        (fn [group]
          (let [start (promise)
                key (w/application-key 0)
                attempts (mapv (fn [worker]
                                 (future
                                   @start
                                   (let [record (values worker)]
                                     (try
                                       (store/put-records! (store/for-worker group worker)
                                                          [[key record]])
                                       {:status :inserted :values record}
                                       (catch Exception e {:status :rejected :error e})))))
                               [0 1])]
            (deliver start true)
            (let [results (mapv deref attempts)
                  winners (filter #(= :inserted (:status %)) results)
                  statuses (frequencies (map :status results))]
              ;; Separate transactions have one winner. Datalevin collector
              ;; members share one atomic native transaction, so a duplicate
              ;; rejects both when the requests are grouped together.
              (is (contains? (if (= system :datalevin)
                               #{{:inserted 1 :rejected 1} {:rejected 2}}
                               #{{:inserted 1 :rejected 1}})
                             statuses)
                  (pr-str results))
              (when (= system :datalevin)
                (doseq [{:keys [error]} results :when error]
                  (is (some #(re-find #"MDB_KEYEXIST|unique constraint"
                                      (or (ex-message %) ""))
                            (take-while some? (iterate ex-cause error)))
                      (pr-str error))))
              (is (= (count winners) (store/record-count group)))
              (is (= (mapv (fn [winner] [key (:values winner)]) winners)
                     (store/scan-records group "" 3)))
              (if-let [winner (first winners)]
                (is (= (:values winner) (store/read-record group key)))
                (do
                  (is (thrown? Exception (store/read-record group key)))
                  ;; A clean rejection must leave a healthy writer and must
                  ;; not publish either rejected payload or poison later writes.
                  (store/put-records! group [[key (values 3)]])
                  (is (= (values 3) (store/read-record group key)))
                  (is (= 1 (store/record-count group)))))
              (let [stored (store/read-record group key)]
                (is (thrown? Exception (store/put-records! group [[key (values 4)]])))
                (is (= stored (store/read-record group key)))
                (is (= 1 (store/record-count group))))))
          {})))))

(deftest page-checks-and-final-validation
  (let [a ["a" (values 1)], b ["b" (values 2)], c ["c" (values 3)]]
    (is (nil? (#'runner/validate-key-page! [a b c] "a" 3 options)))
    (is (nil? (#'runner/validate-key-page! [a c] "a" 3 options)))
    (doseq [bad [[] [b] [a a] [a c b] [a [1 (values 0)]]]]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Incorrect application-key page"
                           (#'runner/validate-key-page! bad "a" 3 options))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (#'runner/validate-key-page! [a b c] "a" 2 options))))
  (let [keys (vec (sort (map w/application-key (range 10))))
        scans (atom [])
        truncate? (atom false)
        fake-store (reify store/Records
                     (record-count [_] 10)
                     (scan-records [_ start n]
                       (swap! scans conj [start n])
                       (mapv #(vector % (values 0))
                             (take (if @truncate? 1 n)
                                   (drop-while #(neg? (compare % start)) keys)))))]
    (is (= :passed (:status (#'runner/validate-database! fake-store 10 options))))
    (is (= [[(nth keys 0) 5] [(nth keys 5) 5]] @scans))
    (reset! truncate? true)
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Missing or unordered application keys"
                         (#'runner/validate-database! fake-store 10 options)))))

(deftest point-workload-string-key-operations
  (doseq [workload [:a :b :c :f]
          [system api mode] conditions]
    (testing (str [system api mode workload])
      (with-store
        (assoc options :system system :api api :mode mode :workload workload :threads 1 :pool-size 1)
        (fn [db]
          (let [keys (mapv w/application-key (range 4))
                key (first keys)]
            (is (= :string (:key-type (store/storage-info db))))
            (is (= :ycsb-fnv64-decimal (:key-generator (store/storage-info db))))
            (store/put-records! db (mapv #(vector % (vec (repeat 12 "aaaa"))) keys))
            (is (= 4 (store/record-count db)))
            (when (and (= system :datalevin) (= api :datalog))
              (let [conn (:conn (store/for-worker db 0))]
                (is (= (set keys) (set (d/q '[:find [?key ...] :where [_ :ycsb/key ?key]] @conn))))
                (doseq [[ordinal k] (map-indexed vector keys)]
                  (let [entity-id (:db/id (d/pull @conn [:db/id] [:ycsb/key k]))]
                    (is (integer? entity-id))
                    (is (not= (w/fnvhash64 ordinal) entity-id))))))
            (store/update-field! db key 1 "zzzz")
            (store/read-record db key)
            (store/update-field! db key 0 "baaa")
            (is (= ["baaa" "zzzz"] (subvec (store/read-record db key) 0 2)))
            (is (thrown? Exception (store/put-records! db [[key (vec (repeat 12 "oops"))]])))
            (is (= ["baaa" "zzzz"] (subvec (store/read-record db key) 0 2)))
            (is (thrown? Exception (store/update-field! db (w/application-key 99) 1 "oops")))
            (is (= 4 (store/record-count db)))
            (doseq [k (next keys)]
              (is (= (vec (repeat 12 "aaaa")) (store/read-record db k)))))
          {})))))

(deftest point-workload-key-generation-and-validation
  (doseq [workload [:a :b :c]
          [system api mode] conditions]
    (testing (str [system api mode workload])
      (let [result (runner/run-case!
                     (assoc options :system system :api api :mode mode :workload workload
                            :records 8 :ops 100 :warmup 10 :threads 2 :pool-size 2
                            :field-count 3 :value-audit? true))]
        (is (= :ycsb-fnv64-decimal (get-in result [:configuration :key-generator])))
        (is (= :passed (get-in result [:validation :value-checks :status])))
        (is (= 8 (get-in result [:validation :records])))
        (is (= 100 (get-in result [:measured :operations])))))))
