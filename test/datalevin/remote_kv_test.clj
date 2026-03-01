(ns datalevin.remote-kv-test
  (:require
   [datalevin.client :as cl]
   [datalevin.remote :as sut]
   [datalevin.interface :as if]
   [datalevin.lmdb :as l]
   [datalevin.constants :as c]
   [datalevin.bits :as b]
   [datalevin.util :as u]
   [datalevin.datom :as d]
   [datalevin.core :as dc]
   [datalevin.interpret :as i]
   [datalevin.test.core :refer [server-fixture]]
   [clojure.test :refer [is testing deftest use-fixtures]])
  (:import
   [java.util UUID Arrays]
   [java.security MessageDigest]))

(use-fixtures :each server-fixture)

(deftest txn-log-operational-apis-test
  (let [dir   (str "dtlv://datalevin:datalevin@localhost/"
                   (UUID/randomUUID))
        store (sut/open-kv dir {:flags    (conj c/default-env-flags :nosync)
                                :wal? true})]
    (if/open-dbi store "a")
    (if/transact-kv store [[:put "a" :k :v]])
    (let [watermarks  (if/txlog-watermarks store)
          committed   (:last-committed-lsn watermarks)
          tx-records  (if/open-tx-log store 1 committed)
          force-res   (if/force-txlog-sync! store)
          lmdb-force  (if/force-lmdb-sync! store)
          forced-watermarks (:watermarks lmdb-force)
          snapshot1   (if/create-snapshot! store)
          _           (if/transact-kv store [[:put "a" :k2 :v2]])
          snapshot2   (if/create-snapshot! store)
          snapshots   (if/list-snapshots store)
          sched-state (if/snapshot-scheduler-state store)]
      (is (:wal? watermarks))
      (is (pos? committed))
      (is (every? #(contains? % :tx-kind) tx-records))
      (is (every? #{:user :vector-checkpoint :unknown}
                  (map :tx-kind tx-records)))
      (is (some #(= :user (:tx-kind %)) tx-records))
      (is (<= 0 (or (:batched-sync-count watermarks) -1)))
      (is (contains? watermarks :avg-commit-wait-ms))
      (is (map? (:avg-commit-wait-ms-by-mode watermarks)))
      (is (<= 0 (or (:segment-roll-count watermarks) -1)))
      (is (<= 0 (or (:segment-roll-duration-ms watermarks) -1)))
      (is (<= 0 (or (:segment-prealloc-success-count watermarks) -1)))
      (is (<= 0 (or (:segment-prealloc-failure-count watermarks) -1)))
      (is (contains? watermarks :append-p99-near-roll-ms))
      (is (<= 0 (or (:vec-checkpoint-count watermarks) -1)))
      (is (<= 0 (or (:vec-checkpoint-duration-ms watermarks) -1)))
      (is (<= 0 (or (:vec-checkpoint-bytes watermarks) -1)))
      (is (<= 0 (or (:vec-checkpoint-failure-count watermarks) -1)))
      (is (<= 0 (or (:vec-replay-lag-lsn watermarks) -1)))
      (is (map? (:vec-checkpoint-domains watermarks)))
      (is (:synced? force-res))
      (is (:synced? lmdb-force))
      (is (= committed
             (get-in lmdb-force [:watermarks :last-applied-lsn])))
      (is (number? (:last-checkpoint-ms forced-watermarks)))
      (is (<= 0 (or (:checkpoint-staleness-ms forced-watermarks) -1)))
      (is (number? (:checkpoint-stale-threshold-ms forced-watermarks)))
      (is (boolean? (:checkpoint-stale? forced-watermarks)))
      (is (<= 0 (or (:durable-applied-lag-lsn forced-watermarks) -1)))
      (is (number? (:durable-applied-lag-threshold-lsn forced-watermarks)))
      (is (boolean? (:durable-applied-lag-alert? forced-watermarks)))
      (is (<= 1 (or (:lmdb-sync-count forced-watermarks) 0)))
      (is (:ok? snapshot1))
      (is (:ok? snapshot2))
      (is (= 2 (count snapshots)))
      (is (= :current (:slot (first snapshots))))
      (is (= :previous (:slot (second snapshots))))
      (is (string? (get-in snapshot2 [:snapshot :path])))
      (is (= 2 (:snapshot-count sched-state)))
      (is (false? (:enabled? sched-state)))
      (is (boolean? (:snapshot-age-alert? sched-state)))
      (is (<= 0 (or (:snapshot-failure-count sched-state) -1)))
      (is (<= 0 (or (:snapshot-consecutive-failure-count sched-state) -1)))
      (is (boolean? (:snapshot-build-failure-alert? sched-state))))
    (if/close-kv store)))

(deftest txn-log-copy-response-metadata-test
  (let [db-name (str "copy-meta-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri {:flags    (conj c/default-env-flags :nosync)
                                  :wal? true})
        client  (cl/new-client uri)]
    (try
      (cl/open-database client db-name c/db-store-kv)
      (if/open-dbi store "z")
      (if/transact-kv store [[:put "z" :k :v]])
      (let [{:keys [type result copy-meta copy-format checksum
                    checksum-algo bytes chunk-bytes chunks]}
            (cl/request client {:type :copy :mode :request
                                :args [db-name true]})
            ^MessageDigest md (MessageDigest/getInstance "SHA-256")
            copied-bytes (reduce
                          (fn [n chunk]
                            (let [^bytes bs chunk]
                              (.update md bs 0 (alength bs))
                              (+ n (alength bs))))
                          0 result)
            actual-checksum (u/hexify (.digest md))]
        (is (= :command-complete type))
        (is (= :binary-chunks copy-format))
        (is (= :sha-256 checksum-algo))
        (is (number? chunk-bytes))
        (is (pos? chunk-bytes))
        (is (number? chunks))
        (is (pos? chunks))
        (is (seq result))
        (is (every? #(instance? (class (byte-array 0)) %) result))
        (is (pos? copied-bytes))
        (is (= (long copied-bytes) (long bytes)))
        (is (= checksum actual-checksum))
        (is (map? copy-meta))
        (is (number? (:started-ms copy-meta)))
        (is (number? (:completed-ms copy-meta)))
        (is (number? (:duration-ms copy-meta)))
        (is (<= (:started-ms copy-meta) (:completed-ms copy-meta)))
        (is (true? (:compact? copy-meta)))
        (is (map? (:backup-pin copy-meta)))
        (is (string? (get-in copy-meta [:backup-pin :pin-id])))
        (is (number? (get-in copy-meta [:backup-pin :floor-lsn])))
        (is (number? (get-in copy-meta [:backup-pin :expires-ms]))))
      (finally
        (when-not (cl/disconnected? client)
          (cl/disconnect client))
        (if/close-kv store)))))

(deftest txn-log-copy-high-level-metadata-test
  (let [db-name (str "copy-meta-high-level-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri {:flags    (conj c/default-env-flags :nosync)
                                  :wal? true})
        dst     (u/tmp-dir (str "copy-meta-high-level-dst-" (UUID/randomUUID)))]
    (try
      (if/open-dbi store "z")
      (if/transact-kv store [[:put "z" :k :v]])
      (let [copy-meta (if/copy store dst true)]
        (is (map? copy-meta))
        (is (number? (:started-ms copy-meta)))
        (is (number? (:completed-ms copy-meta)))
        (is (number? (:duration-ms copy-meta)))
        (is (<= (:started-ms copy-meta) (:completed-ms copy-meta)))
        (is (true? (:compact? copy-meta)))
        (is (map? (:backup-pin copy-meta)))
        (is (string? (get-in copy-meta [:backup-pin :pin-id])))
        (is (number? (get-in copy-meta [:backup-pin :floor-lsn])))
        (is (number? (get-in copy-meta [:backup-pin :expires-ms]))))
      (let [copy-store (l/open-kv dst)]
        (try
          (if/open-dbi copy-store "z")
          (is (= :v (if/get-value copy-store "z" :k)))
          (finally
            (if/close-kv copy-store))))
      (finally
        (if/close-kv store)
        (u/delete-files dst)))))

(deftest transact-kv-copy-in-uses-request-params-test
  (let [db-name (str "tx-kv-copy-in-params-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri)]
    (try
      (if/open-dbi store "a")
      (let [txs (mapv (fn [i] [:put i (inc i)])
                      (range c/+wire-datom-batch-size+))]
        ;; Force copy-in path with compact tx vectors that rely on pulled-out
        ;; dbi-name/k-type/v-type request args.
        (if/transact-kv store "a" txs :long :long)
        (is (= c/+wire-datom-batch-size+ (if/entries store "a")))
        (is (= 43 (if/get-value store "a" 42 :long :long))))
      (finally
        (if/close-kv store)))))

(deftest batch-kv-rpc-test
  (let [db-name (str "batch-kv-rpc-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri)]
    (try
      (if/open-dbi store "a")
      (if/transact-kv store [[:put "a" 1 2 :long :long]
                             [:put "a" 2 3 :long :long]
                             [:put "a" 3 4 :long :long]])
      (is (= [2 [[1 2] [2 3]] 3]
             (sut/batch-kv
               store
               [[:get-value "a" 1 :long :long true]
                [:get-first-n "a" 2 [:all] :long :long false]
                [:range-count "a" [:all] :long]])))
      (let [{:keys [type message]}
            (cl/request
              (.-client ^datalevin.remote.KVStore store)
              {:type :batch-kv
               :args [db-name [[:unsupported-call "a"]]]})]
        (is (= :error-response type))
        (is (re-find #"Unsupported batch-kv call" message)))
      (finally
        (if/close-kv store)))))

(deftest get-values-batch-test
  (let [db-name (str "get-values-batch-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri)]
    (try
      (if/open-dbi store "a")
      (if/transact-kv store [[:put "a" 1 2 :long :long]
                             [:put "a" 2 3 :long :long]
                             [:put "a" 3 4 :long :long]])
      (sut/reset-chatty-kv-stats!)
      (binding [sut/*chatty-kv-detect-threshold* 3
                sut/*chatty-kv-detect-window-ms* 10000]
        (is (= [2 3 4 nil]
               (sut/get-values store "a" [1 2 3 4] :long :long true)))
        (is (= [[1 2] [2 3]]
               (sut/get-values store "a" [1 2] :long :long false)))
        ;; This path should not trigger per-key get-value chatty detection.
        (is (zero? (or (get (sut/chatty-kv-stats) [db-name "a" :get-value]) 0))))
      (finally
        (if/close-kv store)))))

(deftest range-seq-uses-batch-kv-test
  (let [db-name (str "range-seq-batch-kv-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri)]
    (try
      (if/open-dbi store "a")
      (if/transact-kv store [[:put "a" 1 10 :long :long]
                             [:put "a" 2 20 :long :long]
                             [:put "a" 3 30 :long :long]
                             [:put "a" 4 40 :long :long]
                             [:put "a" 5 50 :long :long]])
      (with-open [^java.lang.AutoCloseable rs
                  (if/range-seq store "a" [:closed 2 5]
                                :long :long true {:batch-size 2})]
        (is (= [20 30 40 50] (seq rs))))
      (with-open [^java.lang.AutoCloseable rs
                  (if/range-seq store "a" [:closed-back 5 2]
                                :long :long true {:batch-size 2})]
        (is (= [50 40 30 20] (seq rs))))
      (finally
        (if/close-kv store)))))

(deftest chatty-kv-detection-test
  (let [db-name (str "chatty-kv-detect-" (UUID/randomUUID))
        uri     (str "dtlv://datalevin:datalevin@localhost/" db-name)
        store   (sut/open-kv uri)]
    (try
      (if/open-dbi store "a")
      (if/transact-kv store [[:put "a" 1 2 :long :long]])
      (sut/reset-chatty-kv-stats!)
      (binding [sut/*chatty-kv-detect-threshold* 3
                sut/*chatty-kv-detect-window-ms* 10000]
        (dotimes [_ 5]
          (if/get-value store "a" 1 :long :long true)))
      (is (pos? (or (get (sut/chatty-kv-stats)
                         [db-name "a" :get-value])
                    0)))
      (finally
        (if/close-kv store)))))

(deftest txn-log-rollback-switch-test
  (let [dir   (str "dtlv://datalevin:datalevin@localhost/"
                   (UUID/randomUUID))
        store (sut/open-kv dir {:flags                (conj
                                                        c/default-env-flags
                                                        :nosync)
                                :wal?            true
                                :wal-rollout-mode :rollback})]
    (if/open-dbi store "a")
    (if/transact-kv store [[:put "a" :k :v]])
    (is (= :v (if/get-value store "a" :k)))
    (let [watermarks (if/txlog-watermarks store)
          txlog-force (if/force-txlog-sync! store)
          lmdb-force (if/force-lmdb-sync! store)
          snapshot (if/create-snapshot! store)
          verified (if/verify-commit-marker! store)
          retention (if/txlog-retention-state store)
          gc-res (if/gc-txlog-segments! store)]
      (is (:wal? watermarks))
      (is (= :rollback (:rollout-mode watermarks)))
      (is (false? (:write-path-enabled? watermarks)))
      (is (true? (:rollback? watermarks)))
      (is (false? (:synced? txlog-force)))
      (is (:skipped? txlog-force))
      (is (= :rollback (:reason txlog-force)))
      (is (:synced? lmdb-force))
      (is (= :rollback
             (get-in lmdb-force [:watermarks :rollout-mode])))
      (is (false? (:ok? snapshot)))
      (is (:skipped? snapshot))
      (is (= :rollback (:reason snapshot)))
      (is (false? (:ok? verified)))
      (is (:skipped? verified))
      (is (= :rollback (:reason verified)))
      (is (= :rollback (get-in verified [:watermarks :rollout-mode])))
      (is (:wal? retention))
      (is (:skipped? retention))
      (is (= :rollback (:reason retention)))
      (is (= :rollback (get-in retention [:watermarks :rollout-mode])))
      (is (false? (:ok? gc-res)))
      (is (:skipped? gc-res))
      (is (= :rollback (:reason gc-res)))
      (is (= :rollback (get-in gc-res [:watermarks :rollout-mode]))))
    (if/close-kv store)))

(deftest kv-store-ops-test
  (let [dir   "dtlv://datalevin:datalevin@localhost/testkv"
        store (sut/open-kv dir)]
    (is (instance? datalevin.remote.KVStore store))

    (is (= c/default-env-flags (if/get-env-flags store)))
    (if/set-env-flags store #{:nosync} true)
    (is (= (conj c/default-env-flags :nosync) (if/get-env-flags store)))

    (if/open-dbi store "a")
    (if/open-dbi store "b")
    (if/open-dbi store "c" {:key-size (inc Long/BYTES)
                           :val-size (inc Long/BYTES)})
    (if/open-dbi store "d")

    (testing "list dbis"
      (is (= #{"a" "b" "c" "d"} (set (if/list-dbis store)))))

    (testing "transact-kv"
      (if/transact-kv store
                     [[:put "a" 1 2]
                      [:put "a" 'a 1]
                      [:put "a" 5 {}]
                      [:put "a" :annunaki/enki true :attr :data]
                      [:put "a" :datalevin ["hello" "world"]]
                      [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                      [:put "b" 2 3]
                      [:put "b" (byte 0x01) #{1 2} :byte :data]
                      [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
                      [:put "b" [-1 -235254457N] 5]
                      [:put "b" :a 4]
                      [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                      [:put "b" 1 :long :long :data]
                      [:put "b" :long 1 :data :long]
                      [:put "b" 2 3 :long :long]
                      [:put "b" "ok" 42 :string :int]
                      [:put "d" 3.14 :pi :double :keyword]
                      [:put "d" #inst "1969-01-01" "nice year" :instant :string]
                      [:put "d" [-1 0 1 2 3 4] 1 [:long]]
                      [:put "d" [:a :b :c :d] [1 2 3] [:keyword] [:long]]
                      [:put "d" [-1 "heterogeneous" :datalevin/tuple] 2
                       [:long :string :keyword]]
                      [:put "d"  [:ok -0.687 "nice"] [2 4]
                       [:keyword :double :string] [:long]]]))

    (testing "entries"
      (is (= 5 (:entries (if/stat store))))
      (is (= 6 (:entries (if/stat store "a"))))
      (is (= 6 (if/entries store "a")))
      (is (= 10 (if/entries store "b"))))

    (testing "get-value"
      (is (= 2 (if/get-value store "a" 1)))
      (is (= [1 2] (if/get-value store "a" 1 :data :data false)))
      (is (= true (if/get-value store "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (if/get-value store "a" 42 :long :datom)))
      (is (nil? (if/get-value store "a" 2)))
      (is (nil? (if/get-value store "b" 1)))
      (is (= 5 (if/get-value store "b" [-1 -235254457N])))
      (is (= 1 (if/get-value store "a" 'a)))
      (is (= {} (if/get-value store "a" 5)))
      (is (= ["hello" "world"] (if/get-value store "a" :datalevin)))
      (is (= 3 (if/get-value store "b" 2)))
      (is (= 4 (if/get-value store "b" :a)))
      (is (= #{1 2} (if/get-value store "b" (byte 0x01) :byte)))
      (is (= :bk (if/get-value store "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (if/get-value store "b" :bv :data :bytes)))
      (is (= :long (if/get-value store "b" 1 :long :data)))
      (is (= 1 (if/get-value store "b" :long :data :long)))
      (is (= 3 (if/get-value store "b" 2 :long :long)))
      (is (= 42 (if/get-value store "b" "ok" :string :int)))
      (is (= :pi (if/get-value store "d" 3.14 :double :keyword)))
      (is (= "nice year"
             (if/get-value store "d" #inst "1969-01-01" :instant :string))))

    (testing "get-first and get-first-n"
      (is (= [1 2] (if/get-first store "a" [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n store "a" 2 [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n store "a" 3 [:closed 1 10] :data))))

    (testing "delete"
      (if/transact-kv store [[:del "a" 1]
                            [:del "a" :non-exist]
                            [:del "a" "random things that do not exist"]])
      (is (nil? (if/get-value store "a" 1))))

    (testing "entries-again"
      (is (= 5 (if/entries store "a")))
      (is (= 10 (if/entries store "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (if/get-value store "z" 1))))

    (testing "handle val overflow automatically"
      (if/transact-kv store [[:put "c" 1 (range 50000)]])
      (is (= (range 50000) (if/get-value store "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception
                   (if/transact-kv store [[:put "a" (range 1000) 1]]))))

    (testing "close then re-open, clear and drop"
      (if/close-kv store)
      (is (if/closed-kv? store))
      (let [store (sut/open-kv dir)]
        (if/open-dbi store "a")
        (is (= ["hello" "world"] (if/get-value store "a" :datalevin)))
        (if/clear-dbi store "a")
        (is (nil? (if/get-value store "a" :datalevin)))
        (if/drop-dbi store "a")
        (is (thrown? Exception (if/get-value store "a" 1)))
        (if/close-kv store)))

    (testing "range and filter queries"
      (let [store (sut/open-kv dir)]
        (if/open-dbi store "r" {:key-size (inc Long/BYTES)
                               :val-size (inc Long/BYTES)})
        (let [ks   (shuffle (range 0 10000))
              vs   (map inc ks)
              txs  (map (fn [k v] [:put "r" k v :long :long]) ks vs)
              pred (i/inter-fn [kv]
                     (let [^long k (dc/read-buffer (dc/k kv) :long)]
                       (< 10 k 20)))
              fks  (range 11 20)
              fvs  (map inc fks)
              res  (map (fn [k v] [k v]) fks fvs)
              rc   (count res)]
          (if/transact-kv store txs)
          (is (= rc (if/range-filter-count store "r" pred [:all] :long nil)))
          (is (= fvs (if/range-filter store "r" pred [:all] :long :long true)))
          (is (= res (if/range-filter store "r" pred [:all] :long :long)))
          (is (= 12 (if/get-some store "r" pred [:all] :long :long true)))
          (is (= [0 1] (if/get-first store "r" [:all] :long :long)))
          (is (= 10000 (if/range-count store "r" [:all] :long)))
          (is (= (range 1 10001)
                 (if/get-range store "r" [:all] :long :long true))))
        (if/close-kv store)))))

(deftest async-basic-ops-test
  (let [dir  (str "dtlv://datalevin:datalevin@localhost/" (UUID/randomUUID))
        lmdb (l/open-kv dir {:spill-opts {:spill-threshold 50}})]

    (if/open-dbi lmdb "a")
    (if/open-dbi lmdb "b")
    (if/open-dbi lmdb "c" {:key-size (inc Long/BYTES) :val-size (inc Long/BYTES)})
    (if/open-dbi lmdb "d")

    (testing "transacting nil will throw"
      (is (thrown? Exception @(dc/transact-kv-async lmdb [[:put "a" nil 1]])))
      (is (thrown? Exception @(dc/transact-kv-async lmdb [[:put "a" 1 nil]]))))

    (testing "transact-kv-async"
      @(dc/transact-kv-async lmdb
                             [[:put "a" 1 2]
                              [:put "a" 'a 1]
                              [:put "a" 5 {}]
                              [:put "a" :annunaki/enki true :attr :data]
                              [:put "a" :datalevin ["hello" "world"]]
                              [:put "a" 42 (d/datom 1 :a/b {:id 4}) :long :datom]
                              [:put "b" 2 3]
                              [:put "b" (byte 0x01) #{1 2} :byte :data]
                              [:put "b" (byte-array [0x41 0x42]) :bk :bytes :data]
                              [:put "b" [-1 -235254457N] 5]
                              [:put "b" :a 4]
                              [:put "b" :bv (byte-array [0x41 0x42 0x43]) :data :bytes]
                              [:put "b" 1 :long :long :data]
                              [:put "b" :long 1 :data :long]
                              [:put "b" 2 3 :long :long]
                              [:put "b" "ok" 42 :string :long]
                              [:put "d" 3.14 :pi :double :keyword]
                              [:put "d" #inst "1969-01-01" "nice year" :instant :string]
                              [:put "d" [-1 0 1 2 3 4] 1 [:long]]
                              [:put "d" [:a :b :c :d] [1 2 3] [:keyword] [:long]]
                              [:put "d" [-1 "heterogeneous" :datalevin/tuple] 2
                               [:long :string :keyword]]
                              [:put "d"  [:ok -0.687 "nice"] [2 4]
                               [:keyword :double :string] [:long]]]))

    (testing "entries"
      (is (= 5 (:entries (if/stat lmdb))))
      (is (= 6 (:entries (if/stat lmdb "a"))))
      (is (= 6 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "get-value"
      (is (= 1 (if/get-value lmdb "d" [-1 0 1 2 3 4] [:long])))
      (is (= [1 2 3] (if/get-value lmdb "d" [:a :b :c :d] [:keyword] [:long])))
      (is (= 2 (if/get-value lmdb "d" [-1 "heterogeneous" :datalevin/tuple]
                             [:long :string :keyword])))
      (is (= [2 4] (if/get-value lmdb "d" [:ok -0.687 "nice"]
                                 [:keyword :double :string] [:long])))
      (is (= 2 (if/get-value lmdb "a" 1)))
      (is (= [1 2] (if/get-value lmdb "a" 1 :data :data false)))
      (is (= true (if/get-value lmdb "a" :annunaki/enki :attr :data)))
      (is (= (d/datom 1 :a/b {:id 4}) (if/get-value lmdb "a" 42 :long :datom)))
      (is (nil? (if/get-value lmdb "a" 2)))
      (is (nil? (if/get-value lmdb "b" 1)))
      (is (= 5 (if/get-value lmdb "b" [-1 -235254457N])))
      (is (= 1 (if/get-value lmdb "a" 'a)))
      (is (= {} (if/get-value lmdb "a" 5)))
      (is (= ["hello" "world"] (if/get-value lmdb "a" :datalevin)))
      (is (= 3 (if/get-value lmdb "b" 2)))
      (is (= 4 (if/get-value lmdb "b" :a)))
      (is (= #{1 2} (if/get-value lmdb "b" (byte 0x01) :byte)))
      (is (= :bk (if/get-value lmdb "b" (byte-array [0x41 0x42]) :bytes)))
      (is (Arrays/equals ^bytes (byte-array [0x41 0x42 0x43])
                         ^bytes (if/get-value lmdb "b" :bv :data :bytes)))
      (is (= :long (if/get-value lmdb "b" 1 :long :data)))
      (is (= 1 (if/get-value lmdb "b" :long :data :long)))
      (is (= 3 (if/get-value lmdb "b" 2 :long :long)))
      (is (= 42 (if/get-value lmdb "b" "ok" :string :long)))
      (is (= :pi (if/get-value lmdb "d" 3.14 :double :keyword)))
      (is (= "nice year"
             (if/get-value lmdb "d" #inst "1969-01-01" :instant :string))))

    (testing "get-first and get-first-n"
      (is (= [1 2] (if/get-first lmdb "a" [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 2 [:closed 1 10] :data)))
      (is (= [[1 2] [5 {}]] (if/get-first-n lmdb "a" 3 [:closed 1 10] :data))))

    (testing "delete"
      @(dc/transact-kv-async lmdb [[:del "a" 1]
                                   [:del "a" :non-exist]
                                   [:del "a" "random things that do not exist"]])
      (is (nil? (if/get-value lmdb "a" 1))))

    (testing "entries-again"
      (is (= 5 (if/entries lmdb "a")))
      (is (= 10 (if/entries lmdb "b"))))

    (testing "non-existent dbi"
      (is (thrown? Exception (if/get-value lmdb "z" 1))))

    (testing "handle val overflow automatically"
      @(dc/transact-kv-async lmdb [[:put "c" 1 (range 100000)]])
      (is (= (range 100000) (if/get-value lmdb "c" 1))))

    (testing "key overflow throws"
      (is (thrown? Exception
                   @(dc/transact-kv-async lmdb [[:put "a" (range 1000) 1]]))))

    (u/delete-files dir)))

(deftest list-basic-ops-test
  (let [dir     (str "dtlv://datalevin:datalevin@localhost/" (UUID/randomUUID))
        lmdb    (l/open-kv dir)
        sum     (volatile! 0)
        visitor (i/inter-fn
                    [vb]
                  (let [^long v (b/read-buffer vb :long)]
                    (vswap! sum #(+ ^long %1 ^long %2) v)))]
    (if/open-list-dbi lmdb "l")

    (if/put-list-items lmdb "l" "a" [1 2 3 4] :string :long)
    (if/put-list-items lmdb "l" "b" [5 6 7] :string :long)
    (if/put-list-items lmdb "l" "c" [3 6 9] :string :long)

    (is (= (if/entries lmdb "l") 10))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (if/get-range lmdb "l" [:all] :string :long)))
    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "l" [:closed "a" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "l" [:closed "b" "b"] :string :long)))
    (is (= [["b" 5] ["b" 6] ["b" 7]]
           (if/get-range lmdb "l" [:open-closed "a" "b"] :string :long)))
    (is (= [["c" 3] ["c" 6] ["c" 9] ["b" 5] ["b" 6] ["b" 7]
            ["a" 1] ["a" 2] ["a" 3] ["a" 4]]
           (if/get-range lmdb "l" [:all-back] :string :long)))

    (is (= ["a" 1]
           (if/get-first lmdb "l" [:closed "a" "a"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (if/get-first-n lmdb "l" 2 [:closed "a" "c"] :string :long)))
    (is (= [["a" 1] ["a" 2]]
           (if/list-range-first-n lmdb "l" 2 [:closed "a" "c"] :string
                                  [:closed 1 5] :long)))
    (is (= [3 6 9]
           (if/get-list lmdb "l" "c" :string :long)))

    (is (= [["a" 1] ["a" 2] ["a" 3] ["a" 4] ["b" 5] ["b" 6] ["b" 7]
            ["c" 3] ["c" 6] ["c" 9]]
           (if/list-range lmdb "l" [:all] :string [:all] :long)))
    (is (= [["a" 2] ["a" 3] ["a" 4] ["c" 3]]
           (if/list-range lmdb "l" [:closed "a" "c"] :string
                          [:closed 2 4] :long)))
    (is (= [["c" 9] ["c" 6] ["c" 3] ["b" 7] ["b" 6] ["b" 5]
            ["a" 4] ["a" 3] ["a" 2] ["a" 1]]
           (if/list-range lmdb "l" [:all-back] :string [:all-back] :long)))
    (is (= [["c" 3]]
           (if/list-range lmdb "l" [:at-least "b"] :string
                          [:at-most-back 4] :long)))

    (is (= [["b" 5]]
           (if/list-range lmdb "l" [:open "a" "c"] :string
                          [:less-than 6] :long)))

    (is (= (if/list-count lmdb "l" "a" :string) 4))
    (is (= (if/list-count lmdb "l" "b" :string) 3))

    (is (not (if/in-list? lmdb "l" "a" 7 :string :long)))
    (is (if/in-list? lmdb "l" "b" 7 :string :long))

    (is (= (if/get-list lmdb "l" "a" :string :long) [1 2 3 4]))
    (is (= (if/get-list lmdb "l" "a" :string :long) [1 2 3 4]))

    (if/visit-list lmdb "l" visitor "a" :string)
    (is (= @sum 10))

    (if/del-list-items lmdb "l" "a" :string)

    (is (= (if/list-count lmdb "l" "a" :string) 0))
    (is (not (if/in-list? lmdb "l" "a" 1 :string :long)))
    (is (empty? (if/get-list lmdb "l" "a" :string :long)))

    (if/put-list-items lmdb "l" "b" [1 2 3 4] :string :long)

    (is (= [1 2 3 4 5 6 7]
           (if/get-list lmdb "l" "b" :string :long)))
    (is (= (if/list-count lmdb "l" "b" :string) 7))
    (is (if/in-list? lmdb "l" "b" 1 :string :long))

    (if/del-list-items lmdb "l" "b" [1 2] :string :long)

    (is (= (if/list-count lmdb "l" "b" :string) 5))
    (is (not (if/in-list? lmdb "l" "b" 1 :string :long)))
    (is (= [3 4 5 6 7]
           (if/get-list lmdb "l" "b" :string :long)))
    (if/close-kv lmdb)))

(deftest list-string-test
  (let [dir   (str "dtlv://datalevin:datalevin@localhost/" (UUID/randomUUID))
        lmdb  (l/open-kv dir {:flags (conj c/default-env-flags :nosync)})
        pred  (i/inter-fn [kv]
                (let [^String v (b/read-buffer (l/v kv) :string)]
                  (< (count v) 5)))
        pred1 (i/inter-fn [kv]
                (let [^String v (b/read-buffer (l/v kv) :string)]
                  (when (< (count v) 5) v)))]
    (if/open-list-dbi lmdb "str")
    (is (if/list-dbi? lmdb "str"))

    (if/put-list-items lmdb "str" "a" ["abc" "hi" "defg" ] :string :string)
    (if/put-list-items lmdb "str" "b" ["hello" "world" "nice"] :string :string)

    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:all] :string :string)))
    (is (= [["a" "abc"] ["a" "defg"] ["a" "hi"]
            ["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:closed "a" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:closed "b" "b"] :string :string)))
    (is (= [["b" "hello"] ["b" "nice"] ["b" "world"]]
           (if/get-range lmdb "str" [:open-closed "a" "b"] :string :string)))

    (is (= [["b" "nice"]]
           (if/list-range-filter lmdb "str" pred [:greater-than "a"] :string
                                 [:all] :string)))

    (is (= ["nice"]
           (if/list-range-keep lmdb "str" pred1 [:greater-than "a"] :string
                               [:all] :string)))
    (is (= "nice"
           (if/list-range-some lmdb "str" pred1 [:greater-than "a"] :string
                               [:all] :string)))

    (is (= ["a" "abc"]
           (if/get-first lmdb "str" [:closed "a" "a"] :string :string)))

    (is (= (if/list-count lmdb "str" "a" :string) 3))
    (is (= (if/list-count lmdb "str" "b" :string) 3))

    (is (not (if/in-list? lmdb "str" "a" "hello" :string :string)))
    (is (if/in-list? lmdb "str" "b" "hello" :string :string))

    (is (= (if/get-list lmdb "str" "a" :string :string)
           ["abc" "defg" "hi"]))

    (if/del-list-items lmdb "str" "a" :string)

    (is (= (if/list-count lmdb "str" "a" :string) 0))
    (is (not (if/in-list? lmdb "str" "a" "hi" :string :string)))
    (is (empty? (if/get-list lmdb "str" "a" :string :string)))

    (if/put-list-items lmdb "str" "b" ["good" "peace"] :string :string)

    (is (= (if/list-count lmdb "str" "b" :string) 5))
    (is (if/in-list? lmdb "str" "b" "good" :string :string))

    (if/del-list-items lmdb "str" "b" ["hello" "world"] :string :string)

    (is (= (if/list-count lmdb "str" "b" :string) 3))
    (is (not (if/in-list? lmdb "str" "b" "world" :string :string)))

    (if/close-kv lmdb)))

(deftest copy-test
  (let [src    "dtlv://datalevin:datalevin@localhost/copytest"
        rstore (sut/open-kv src)
        dst    (u/tmp-dir (str "copy-test-" (UUID/randomUUID)))]
    (if/open-dbi rstore "z")
    (let [ks  (shuffle (range 0 10000))
          vs  (map inc ks)
          txs (map (fn [k v] [:put "z" k v :long :long]) ks vs)]
      (if/transact-kv rstore txs))
    (if/copy rstore dst)
    (let [cstore (l/open-kv dst)]
      (if/open-dbi cstore "z")
      (is (= (if/get-range rstore "z" [:all] :long :long)
             (if/get-range cstore "z" [:all] :long :long)))
      (if/close-kv cstore))
    (if/close-kv rstore)
    (u/delete-files dst)))

(deftest re-index-test
  (let [dir  "dtlv://datalevin:datalevin@localhost/re-index"
        lmdb (sut/open-kv dir)]
    (if/open-dbi lmdb "misc")

    (if/transact-kv
        lmdb
      [[:put "misc" :datalevin "Hello, world!"]
       [:put "misc" 42 {:saying "So Long, and thanks for all the fish"
                        :source "The Hitchhiker's Guide to the Galaxy"}]])
    (let [lmdb1 (if/re-index lmdb {})]
      (if/open-dbi lmdb1 "misc")
      (is (= [[42
               {:saying "So Long, and thanks for all the fish",
                :source "The Hitchhiker's Guide to the Galaxy"}]
              [:datalevin "Hello, world!"]]
             (if/get-range lmdb1 "misc" [:all])))

      ;; TODO https://github.com/juji-io/datalevin/issues/212
      #_(if/visit
            lmdb1 "misc"
            (i/inter-fn
                [kv]
              (let [k (dc/read-buffer (dc/k kv) :data)]
                (when (= k 42)
                  (if/transact-kv
                      lmdb1
                    [[:put "misc" 42 "Don't panic"]]))))

            [:all])
      #_(is (= [[42 "Don't panic"] [:datalevin "Hello, world!"]]
               (if/get-range lmdb1 "misc" [:all])))
      (if/close-kv lmdb1))))
