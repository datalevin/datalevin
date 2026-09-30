(ns datalevin.txlog.native-application-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.kv.encoding :as encoding]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.tx-group :as group]
            [datalevin.tx-state :as state]
            [datalevin.tx-state.kv :as pending]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.tx-state.view :as view]
            [datalevin.txlog :as wal]
            [datalevin.util :as u]
            [datalevin.validate :as vld])
  (:import [datalevin.tx_group Group]
           [java.util.concurrent ConcurrentLinkedQueue CountDownLatch]
           [java.util.concurrent.locks ReentrantLock]))

(defn- caught [f] (try (f) (catch Throwable e e)))
(defn- join [job] (deref job 10000 ::timeout))
(defn- rows [n] [[:put "data" n (str n) :long :string]])

(defn- with-native [hooks f]
  (let [dir (u/tmp-dir (str "wal-native-application-" (random-uuid)))
        opts {:wal? true :wal-shared? false :wal-sync-mode :fsync
              :db-identity (str (random-uuid))
              :wal-segment-prealloc? false :snapshot-scheduler? false}
        db (l/open-kv dir opts)
        _ (i/open-dbi db "data" (get-in hooks [:dbi-opts "data"] {}))
        _ (i/open-list-dbi db "items" (get-in hooks [:dbi-opts "items"] {}))
        _ (when-let [initial (:initial-rows hooks)] (i/transact-kv db initial))
        raw (kv/raw-lmdb db)
        wal-state (wal/state db)
        base (long @(:meta-last-applied-lsn wal-state))
        slot (volatile! nil)
        pipeline (when (:kv-pipeline? hooks)
                   (pending/create raw (dissoc hooks :kv-pipeline?)))
        runtime (or (:state pipeline)
                    (state/create wal-state
                                  (merge (:runtime-opts hooks)
                                         {:applied-lsn base :hooks hooks
                                          :apply-range! (fn [entries token]
                                                          (kvtx/apply-appended-range!
                                                           raw @slot entries token))})))]
    (vreset! slot runtime)
    (try (f {:db db :raw raw :runtime runtime :wal wal-state
             :dir dir :opts opts :base base :pipeline pipeline})
         (finally
           (state/close! runtime 1000 #(i/close-kv db))
           (u/delete-files dir)))))

(defn- append! [runtime n]
  (state/append! runtime (state/reserve! runtime 200 10000) (rows n) n))

(defn- append-group! [runtime values]
  (let [record (state/prepare-record)
        entries (mapv #(state/prepare-entry (state/reserve! runtime 200 10000)
                                             (rows %) % record) values)
        token (state/acquire-preparation! runtime (:reservation (first entries)) 10000)]
    (try
      (let [root @(:root runtime)]
        (state/append-batch! runtime entries
                             {:expected-root root :root (update root :lsn inc)}))
      (finally (state/release-preparation! token)))))

(deftest application-and-recovery-never-split-a-collected-record
  (with-native
    {:runtime-opts {:application-max-records 1}}
    (fn [{:keys [db runtime raw wal dir opts base]}]
      (let [a (append-group! runtime [1 2]) b (append-group! runtime [3 4])]
        (is (every? #(identical? @(:append-batch (first a)) @(:append-batch %)) a))
        (is (= 1 (state/await! runtime (first a))))
        (is (= [[1 "1"] [2 "2"]] (vec (i/get-range raw "data" [:all] :long :string))))
        (is (= 2 (state/await! runtime (second a))))
        (is (= {:bytes 400 :requests 2} (state/usage runtime)))
        (is (= (inc (long base)) @(:meta-last-applied-lsn wal)))
        (is (= (inc (long base))
               (i/get-value raw c/kv-info c/wal-local-payload-lsn :keyword :data)))
        (is (:ok? (i/verify-commit-marker! db)))
        (is (= (+ (long base) 2) @(:last-durable-lsn (:sync-manager wal))))
        (with-redefs [state/phase!
                      (fn [phase _]
                        (when (= :native-writer-acquired phase)
                          (throw (ex-info "Injected suffix abort" {:applied? false}))))]
          (let [outcomes (mapv #(ex-data (caught (fn [] (state/await! runtime %))))
                               b)]
            (is (= [:committed :committed] (mapv :outcome outcomes)))
            (is (= [(+ (long base) 2) (+ (long base) 2)] (mapv :txlog-lsn outcomes)))
            (is (= (mapv #(wal/append-identity @(:append-batch %) @(:lsn %)) b)
                   (mapv :txlog-record outcomes)))))
        (is (= 1 (state/await! runtime (first a))))
        (state/close! runtime 1000 #(i/close-kv db))
        (let [reopened (l/open-kv dir opts)]
          (try
            (is (= [[1 "1"] [2 "2"] [3 "3"] [4 "4"]]
                   (vec (i/get-range reopened "data" [:all] :long :string))))
            (is (= [(inc (long base)) (+ (long base) 2)]
                   (mapv :lsn (kv/open-tx-log reopened (inc (long base))))))
            (is (:ok? (i/verify-commit-marker! reopened)))
            (finally (i/close-kv reopened))))))))

(deftest native-prefix-commit-persists-data-and-watermark-together
  (with-native
    {}
    (fn [{:keys [db runtime wal base]}]
      (let [a (append! runtime 1) b (append! runtime 2)
            hi (+ (long base) 2)]
        (is (= 2 (state/await! runtime b)))
        (is (= 1 (state/await! runtime a)))
        (is (= [[1 "1"] [2 "2"]] (vec (i/get-range db "data" [:all] :long :string))))
        (is (= hi (i/get-value db c/kv-info c/wal-local-payload-lsn :keyword :data)))
        (is (= hi @(:meta-last-applied-lsn wal)))
        (is (:ok? (i/verify-commit-marker! db)))
        (is (= [(inc (long base)) hi] (mapv :lsn (kv/open-tx-log db (inc (long base))))))))))

(deftest blocked-wal-force-leaves-the-native-writer-free
  (let [entered (promise) release (CountDownLatch. 1)
        first? (atom true)]
    (with-native
      {:before-sync! (fn [_ _]
                       (when (compare-and-set! first? true false)
                         (deliver entered true)
                         (.await release)))}
      (fn [{:keys [raw runtime db]}]
        (let [a (append! runtime 1)
              first-job (future (caught #(state/await! runtime a)))
              jobs (atom [first-job])]
          (try
            (is (deref entered 10000 false))
            (is (= :writer-free
                   (deref (future
                            (l/with-transaction-kv [tx raw]
                              (i/abort-transact-kv tx)
                              :writer-free))
                          1000 ::blocked)))
            (let [b (deref (future (append! runtime 2)) 1000 ::blocked)]
              (is (map? b))
              (swap! jobs conj (future (caught #(state/await! runtime b)))))
            (.countDown release)
            (is (= [1 2] (mapv join @jobs)))
            (is (:ok? (i/verify-commit-marker! db)))
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest recovery-applies-two-durable-records-without-another-append
  (with-native
    {}
    (fn [{:keys [db runtime raw wal dir opts base]}]
      (let [a (append! runtime 1) b (append! runtime 2)
            error (ex-info "Injected application abort" {:applied? false})]
        (with-redefs [state/phase! (fn [phase _]
                                     (when (= :native-writer-acquired phase)
                                       (throw error)))]
          (let [first-error (caught #(state/await! runtime a))
                next-error (caught #(state/await! runtime b))]
            (is (= :committed (:outcome (ex-data first-error))))
            (is (= :committed (:outcome (ex-data next-error))))
            (is (= [(inc (long base)) (+ (long base) 2)]
                   (mapv #(-> % ex-data :txlog-lsn) [first-error next-error])))))
        (is (empty? (i/get-range raw "data" [:all] :long :string)))
        (is (= (+ (long base) 2) @(:last-durable-lsn (:sync-manager wal))))
        (state/close! runtime 1000 #(i/close-kv db))
        (let [reopened (l/open-kv dir opts)]
          (try
            (is (= [[1 "1"] [2 "2"]]
                   (vec (i/get-range reopened "data" [:all] :long :string))))
            (is (= [(inc (long base)) (+ (long base) 2)]
                   (mapv :lsn (kv/open-tx-log reopened (inc (long base))))))
            (is (:ok? (i/verify-commit-marker! reopened)))
            (finally (i/close-kv reopened))))))))

(defn- encoded [n type] (view/encode n type 65536))
(defn- read-view [raw rtx root base dbi kr vr]
  (view/with-rows raw rtx root base dbi kr :long vr :string
    #(mapv (fn [[k v]] [(view/decode k :long) (view/decode v :string)]) %)))

(deftest kv-pending-view-merges-encoded-keys-and-independent-list-deltas
  (with-native
    {:initial-rows [[:put "data" 1 "old" :long :string]
                    [:put "data" 2 "remove" :long :string]
                    [:put "data" 5 "keep" :long :string]
                    [:put-list "items" 1 ["a" "c" "e"] :long :string]
                    [:put-list "items" 2 ["b" "d"] :long :string]]}
    (fn [{:keys [raw base]}]
      (let [encode-row (fn [[op dbi k v]]
                         [op dbi (encoded k :long)
                          (if (#{:put-list :del-list} op)
                            (mapv #(encoded % :string) v)
                            (when v (encoded v :string)))])
            first-root (view/stage (view/empty-root base) (inc (long base))
                                   (mapv encode-row [[:put "data" 1 "new"]
                                                     [:del "data" 2]
                                                     [:put "data" 3 "added"]
                                                     [:del-list "items" 1 ["c"]]
                                                     [:put-list "items" 1 ["b" "e"]]])
                                   #(i/list-dbi? raw %))
            next-root (view/stage first-root (+ (long base) 2)
                                  (mapv encode-row [[:del "items" 2]
                                                    [:put-list "items" 2 ["z"]]])
                                  #(i/list-dbi? raw %))
            rtx (i/get-rtx raw)]
        (try
          (is (= [[1 "new"] [3 "added"] [5 "keep"]]
                 (read-view raw rtx first-root base "data" [:all] [:all])))
          (is (= [[5 "keep"] [3 "added"] [1 "new"]]
                 (read-view raw rtx first-root base "data" [:all-back] [:all])))
          (is (= [[3 "added"]]
                 (read-view raw rtx first-root base "data" [:open 1 5] [:all])))
          (is (= [[1 "a"] [1 "b"] [1 "e"] [2 "b"] [2 "d"]]
                 (read-view raw rtx first-root base "items" [:all] [:all])))
          (is (= [[1 "a"] [1 "b"] [1 "e"] [2 "z"]]
                 (read-view raw rtx next-root base "items" [:all] [:all])))
          (is (= [[1 "e"] [1 "b"]]
                 (read-view raw rtx next-root base "items" [:closed 1 1]
                            [:closed-back "e" "b"])))
          (is (= [[2 "z"] [1 "a"] [1 "b"] [1 "e"]]
                 (read-view raw rtx next-root base "items" [:all-back] [:all])))
          (is (= :native (view/fast-path (view/footprint next-root "data" [:at-least 5] :long base))))
          (is (= :small (view/fast-path (view/footprint next-root "data" [:all] :long base))))
          (is (empty? (:dbis (view/prune next-root (+ (long base) 2)))))
          (is (= [[1 "a"] [1 "b"] [1 "e"] [2 "b"] [2 "d"]]
                 (read-view raw rtx first-root base "items" [:all] [:all]))
              "Pruning/new roots do not mutate an older pinned view")
          (finally (i/return-rtx raw rtx)))))))

(deftest pending-view-fast-path-boundaries
  (doseq [[keys bytes expected] [[0 0 :native] [1 1 :small] [32 16384 :small]
                                 [33 1 :merged] [1 16385 :merged]]]
    (is (= expected (view/fast-path {:keys keys :bytes bytes})))))

(deftest partial-prune-preserves-untouched-delta-nodes
  (let [root (reduce (fn [root n]
                       (view/stage root n
                                   [[:put "data" (encoded n :long)
                                     (encoded n :long)]]
                                   (constantly false)))
                     (view/empty-root 0) (range 1 4097))
        key (encoded 4096 :long)
        original (get-in root [:dbis "data" :keys key])
        first-prune (view/prune root 256)
        second-prune (view/prune first-prune 512)]
    (is (= 3840 (count (get-in first-prune [:dbis "data" :keys]))))
    (is (= 3584 (count (get-in second-prune [:dbis "data" :keys]))))
    (is (= 3584 (count (:by-lsn second-prune))))
    (is (identical? original (get-in first-prune [:dbis "data" :keys key])))
    (is (identical? original (get-in second-prune [:dbis "data" :keys key])))
    (is (identical? second-prune (view/prune second-prune 512)))
    (is (= 4096 (count (get-in root [:dbis "data" :keys]))))
    (is (empty? (:dbis (view/prune second-prune 4096))))))

(deftest partial-prune-keeps-newer-versions-of-touched-keys
  (let [key (encoded 1 :long)
        a (encoded "a" :string)
        b (encoded "b" :string)
        c (encoded "c" :string)
        first-root (view/stage (view/empty-root 0) 1
                               [[:put "data" key a]
                                [:put-list "items" key [a b]]]
                               #(= "items" %))
        next-root (view/stage first-root 2
                              [[:put "data" key c]
                               [:del-list "items" key [a]]
                               [:put-list "items" key [c]]]
                              #(= "items" %))
        pruned (view/prune next-root 1)
        data-node (get-in pruned [:dbis "data" :keys key])
        list-node (get-in pruned [:dbis "items" :keys key])]
    (is (= 2 (first (:put data-node))))
    (is (= [2 false] (get (:values list-node) a)))
    (is (= [2 true] (get (:values list-node) c)))
    (is (nil? (get (:values list-node) b)))
    (is (= [1 2] (vec (keys (:by-lsn next-root)))))
    (is (= [2] (vec (keys (:by-lsn pruned)))))
    (is (empty? (:dbis (view/prune pruned 2))))))

(deftest repeated-keys-share-one-index-entry-per-record-and-stay-pinned
  (let [root (reduce (fn [root lsn]
                       ;; Equal encodings arrive in separately owned arrays,
                       ;; including in separate preparation calls at one LSN.
                       (reduce (fn [root n]
                                 (view/stage root lsn
                                             [[:put "data" (encoded 1 :long)
                                               (encoded n :long)]]
                                             (constantly false)))
                               root (range 4)))
                     (view/empty-root 0) (range 1 33))
        key (ffirst (get-in root [:dbis "data" :keys]))
        node (get-in root [:dbis "data" :keys key])
        pruned (view/prune root 31)]
    (is (= (vec (repeat 32 1)) (mapv (comp count val) (:by-lsn root))))
    (is (every? #(identical? key (second (first (val %)))) (:by-lsn root)))
    (is (= [32] (vec (keys (:by-lsn pruned)))))
    (is (identical? node (get-in pruned [:dbis "data" :keys key]))
        "Pruning overwritten versions preserves an already clean node")
    (is (= 32 (count (:by-lsn root))) "The pinned predecessor is unchanged")
    (is (= 3 (view/decode (second (:put node)) :long)))
    (is (identical? pruned (view/stage pruned 33 [] (constantly false))))))

(deftest pruned-deltas-match-native-prefixes-after-hot-key-and-list-overwrites
  (let [groups [[[:put "data" 1 "a"] [:put-list "items" 1 ["a" "b"]]
                 [:put-list "items" 2 ["z"]]]
                [[:put "data" 1 "b"] [:put "data" 1 "c"]
                 [:del "items" 1] [:put-list "items" 1 ["b" "c"]]
                 [:del-list "items" 2 ["z"]]]
                [[:put "data" 2 "d"] [:del-list "items" 1 ["b"]]
                 [:put "items" 1 "d"] [:del "data" 1] [:put "data" 1 "e"]]
                [[:del "items" 1] [:put-list "items" 1 ["x"]]
                 [:put-list "items" 2 ["q" "r"]] [:del "data" 2]]
                [[:put-list "items" 1 ["y" "z"]] [:del-list "items" 1 ["x" "y"]]
                 [:put "data" 1 "f"]]
                [[:put "data" 3 "keep"] [:del-list "items" 1 ["z"]] [:del "data" 1]]
                [[:put "data" 1 "final"] [:put-list "items" 1 ["a" "b"]]
                 [:del-list "items" 1 ["a"]] [:put-list "items" 2 ["q" "s"]]
                 [:del-list "items" 2 ["r"]] [:put "data" 2 "second"]]]
        encode-row (fn [[op dbi k v]]
                     [op dbi (encoded k :long)
                      (if (#{:put-list :del-list} op)
                        (mapv #(encoded % :string) v)
                        (when v (encoded v :string)))])
        roots (reductions
               (fn [root [lsn rows]]
                 (reduce #(view/stage %1 lsn (mapv encode-row %2) #{"items"})
                         root (partition-all 2 rows)))
               (view/empty-root 0) (map vector (range 1 8) groups))
        root (last roots)]
    (doseq [prefix [0 1 3 6 7]]
      (with-native
        {:initial-rows (mapv (fn [[op dbi k v]]
                               (if (= :del op) [op dbi k :long]
                                   [op dbi k v :long :string]))
                             (mapcat identity (take prefix groups)))}
        (fn [{:keys [raw]}]
          ;; The fixture contains exactly this logical prefix. The view's
          ;; version numbers are independent of the fixture's physical WAL LSN.
          (let [pruned (view/prune root prefix)
                reader (i/get-rtx raw)]
            (try
              (doseq [candidate [root pruned]]
                (is (= [[1 "final"] [2 "second"] [3 "keep"]]
                       (read-view raw reader candidate prefix "data" [:all] [:all])))
                (is (= [[1 "b"] [2 "q"] [2 "s"]]
                       (read-view raw reader candidate prefix "items" [:all] [:all])))
                (is (= [[2 "s"] [2 "q"] [1 "b"]]
                       (read-view raw reader candidate prefix "items" [:all-back] [:all-back]))))
              (when (<= prefix 3)
                (is (= [[1 "e"] [2 "d"]]
                       (read-view raw reader (nth roots 3) prefix "data" [:all] [:all])))
                (is (= [[1 "c"] [1 "d"]]
                       (read-view raw reader (nth roots 3) prefix "items" [:all] [:all]))))
              (is (= 7 (count (:by-lsn root))))
              (finally (i/return-rtx raw reader)))))))))

(deftest prepared-kv-explicit-request-has-one-record-and-sees-own-writes
  (doseq [submit [pending/submit! pending/submit-replayable!]]
    (with-native
      {:kv-pipeline? true}
      (fn [{:keys [pipeline db base runtime]}]
        (is (= {:count 2 :value "second" :items [[1 "a"] [1 "b"]]}
               (submit
                pipeline 65536
                (fn [tx]
                  (pending/transact! tx [[:put "data" 1 "first" :long :string]])
                  (is (= "first" (pending/get-value tx "data" 1 :long :string)))
                  (pending/transact! tx [[:put "data" 1 "second" :long :string]
                                         [:put "data" 2 "other" :long :string]
                                         [:put-list "items" 1 ["a" "b"] :long :string]])
                  (is (pending/in-list? tx "items" 1 "b" :long :string))
                  {:count (pending/range-count tx "data" [:all] :long)
                   :value (pending/get-value tx "data" 1 :long :string)
                   :items (pending/get-range tx "items" [:all] :long :string)}))))
        (is (= [(inc (long base))] (mapv :lsn (kv/open-tx-log db (inc (long base))))))
        (is (= "second" (i/get-value db "data" 1 :long :string)))
        (is (= {:bytes 0 :requests 0} (state/usage runtime)))
        (is (empty? (:dbis @(:root runtime))))))))

(deftest pending-range-count-uses-native-base-and-small-delta-corrections
  (let [payload (apply str (repeat 1024 "x"))]
    (with-native
      {:kv-pipeline? true
       :initial-rows (into (mapv (fn [n] [:put "data" n payload :long :string])
                                 (range 256))
                           [[:put-list "items" 1 ["a" "c" "e"] :long :string]
                            [:put-list "items" 2 ["b" "d"] :long :string]])}
      (fn [{:keys [pipeline]}]
        (is (= {:empty 256 :empty-list 5 :updated 256 :bounded 3
                :open 1 :unaffected 11 :reverse 256 :reverse-bounded 3
                :list 4 :list-bounded 3 :merged 289}
               (pending/submit!
                pipeline 65536
                (fn [tx]
                  (let [empty-count (pending/range-count tx "data" [:all] :long)
                        empty-list (pending/range-count tx "items" [:all] :long)]
                    (pending/transact!
                     tx [[:put "data" 0 "new" :long :string]
                         [:del "data" 1 :long]
                         [:put "data" 256 "added" :long :string]
                         [:put-list "items" 1 ["a" "b"] :long :string]
                         [:del-list "items" 1 ["c"] :long :string]
                         [:del "items" 2 :long]
                         [:put-list "items" 2 ["z"] :long :string]])
                    (let [small {:empty empty-count :empty-list empty-list
                                 :updated (pending/range-count tx "data" [:all] :long)
                                 :bounded (pending/range-count tx "data" [:closed 0 3] :long)
                                 :open (pending/range-count tx "data" [:open 0 3] :long)
                                 :unaffected (pending/range-count tx "data" [:closed 10 20] :long)
                                 :reverse (pending/range-count tx "data" [:all-back] :long)
                                 :reverse-bounded (pending/range-count tx "data" [:closed-back 3 0] :long)
                                 :list (pending/range-count tx "items" [:all] :long)
                                 :list-bounded (pending/range-count tx "items" [:closed 1 1] :long)}]
                      (pending/transact!
                       tx (mapv (fn [n] [:put "data" n "extra" :long :string])
                                (range 300 333)))
                      (assoc small :merged
                             (pending/range-count tx "data" [:all] :long))))))))))))

(deftest rmw-preparation-advances-while-first-wal-flush-is-stalled
  (let [entered (promise) release (CountDownLatch. 1)
        appended (CountDownLatch. 8) bodies (atom 0)]
    (with-native
      {:kv-pipeline? true
       :hooks {:before-sync! (fn [_ _]
                               (when-not (realized? entered)
                                 (deliver entered true) (.await release)))}}
      (fn [{:keys [pipeline db raw base]}]
        (let [jobs (atom [])]
          (try
            (with-redefs [state/phase! (fn [event _]
                                         (when (= event :receipt-published)
                                           (.countDown appended)))]
              (dotimes [index 8]
                (when (= index 1) (is (deref entered 10000 false)))
                (swap! jobs conj
                       (future
                         (caught
                          #(pending/submit!
                            pipeline 4096
                            (fn [tx]
                              (swap! bodies inc)
                              (let [n (inc (long (or (pending/get-value
                                                      tx "data" 0 :long :long) 0)))]
                                (pending/transact! tx [[:put "data" 0 n :long :long]])
                                n)))))))
              (is (deref entered 10000 false))
              (is (.await appended 1 java.util.concurrent.TimeUnit/SECONDS)
                  "Every RMW appends without waiting for the held native-free flush")
              (is (nil? (i/get-value raw "data" 0 :long :long))
                  "Strict public reads do not expose pending roots")
              (.countDown release)
              (is (= (range 1 9) (sort (map join @jobs)))))
            (is (= 8 @bodies))
            (is (= 8 (i/get-value db "data" 0 :long :long)))
            (let [records (vec (kv/open-tx-log db (inc (long base))))]
              (is (<= 2 (count records) 8))
              (is (= (range (inc (long base)) (+ (long base) 1 (count records)))
                     (map :lsn records)))
              (is (= (range 1 9)
                     (map #(view/decode (nth % 3) :long) (mapcat :ops records)))))
            (is (:ok? (i/verify-commit-marker! db)))
            (finally (.countDown release) (doseq [job @jobs] (join job)))))))))

(deftest preparation-abort-and-budget-rejection-never-append
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline db runtime base]}]
      (is (= :aborted
             (pending/submit! pipeline 4096
                              (fn [tx]
                                (pending/transact! tx [[:put "data" 1 "discard" :long :string]])
                                (pending/abort! tx)
                                :aborted))))
      (is (= :not-committed
             (:outcome (ex-data
                        (caught #(pending/submit!
                                  pipeline 16
                                  (fn [tx]
                                    (pending/transact! tx [[:put "data" 2 "too big" :long :string]]))))))))
      (is (empty? (kv/open-tx-log db (inc (long base)))))
      (is (empty? (i/get-range db "data" [:all] :long :string)))
      (is (= {:bytes 0 :requests 0} (state/usage runtime))))))

(deftest invalid-input-is-rejected-before-wal-and-does-not-poison-reopen
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline db runtime wal base dir opts]}]
      (doseq [row [[:put "data" nil 2 :data :long]
                   [:put "data" 1 nil :long :data]
                   [:put "data" (byte-array 0) 2 :raw :long]
                   [:put "data" (byte-array 512) 2 :raw :long]
                   [:put-list "items" 1 [(byte-array 0)] :long :raw]
                   [:put-list "items" 1 [(byte-array 512)] :long :raw]
                   [:put-list "items" 1 [nil] :long :data]]]
        (let [error (caught #(pending/submit-rows! pipeline 8192 [row]))]
          (is (instance? Throwable error))
          (is (= :not-committed (:outcome (ex-data error))))
          (is (= (inc (long base)) @(:next-lsn wal)))
          (is (nil? @(:failure runtime)))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))))
      (is (= :transacted (pending/submit-rows! pipeline 4096 (rows 1))))
      (is (= [(inc (long base))] (mapv :lsn (kv/open-tx-log db (inc (long base))))))
      (i/close-kv db)
      (let [reopened (l/open-kv dir opts)]
        (try
          (is (= "1" (i/get-value reopened "data" 1 :long :string)))
          (is (:ok? (i/verify-commit-marker! reopened)))
          (finally (i/close-kv reopened)))))))

(deftest malformed-raw-storage-rows-cannot-enter-application-or-replay
  (with-native
    {:dbi-opts {"data" {:validate-data? true}
                "items" {:validate-data? true}}}
    (fn [{:keys [db raw wal base]}]
      (let [key (byte-array [1])
            value (byte-array [2])
            invalid-rows [[:put "data" (byte-array 0) value :raw :raw]
                          [:put "data" (byte-array 512) value :raw :raw]
                          [:put-list "items" key [(byte-array 0)] :raw :raw]
                          [:put-list "items" key [(byte-array 512)] :raw :raw]
                          [:del-list "items" key [(byte-array 0)] :raw :raw]]
            next-lsn @(:next-lsn wal)]
        (doseq [row invalid-rows]
          (is (= :kv/invalid-encoded-size
                 (:error (ex-data
                          (caught #(vld/validate-storage-tx-data
                                    (l/->kv-tx-data row) true)))))))
        (doseq [path [:application :replay]
                row invalid-rows]
          (let [error (caught #(if (= path :application)
                                 (i/transact-kv raw (encoding/storage-rows [row]))
                                 (kvtx/replay-txlog-rows! raw [row] (inc (long base)))))]
            (is (instance? Throwable error) (str path " rejected malformed :raw row"))))
        (is (zero? (i/entries db "data")))
        (is (zero? (i/entries db "items")))
        (is (= next-lsn @(:next-lsn wal)))
        (is (nil? @(:fatal-error wal)))))))

(deftest validated-user-types-apply-and-recover-as-physical-wal-rows
  (with-native
    {:kv-pipeline? true
     :dbi-opts {"data" {:validate-data? true} "items" {:validate-data? true}}}
    (fn [{:keys [pipeline db runtime wal base dir opts]}]
      (let [bad (caught #(pending/submit-rows!
                          pipeline 4096 [[:put "data" 1 "bad" :long :long]]))]
        (is (= :not-committed (:outcome (ex-data bad))))
        (is (= (inc (long base)) @(:next-lsn wal))))
      (is (= :transacted
             (pending/submit-rows! pipeline 8192
                                   [[:put "data" 1 2 :long :long]
                                    [:put "data" :generic [1 2]]
                                    [:put-list "items" 1 [2 3] :long :long]])))
      (is (= 2 (i/get-value db "data" 1 :long :long)))
      (is (= [1 2] (i/get-value db "data" :generic :data :data)))
      (is (= [2 3] (vec (i/get-list db "items" 1 :long :long))))
      ;; Leave another valid typed request durable but unapplied, forcing open
      ;; recovery through a DBI whose validation option remains enabled.
      (with-redefs [state/phase! (fn [event _]
                                   (when (= event :native-writer-acquired)
                                     (throw (ex-info "Injected abort" {:applied? false}))))]
        (is (= :committed
               (:outcome (ex-data
                          (caught #(pending/submit-rows!
                                    pipeline 8192
                                    [[:put "data" 1 4 :long :long]
                                     [:del-list "items" 1 [2] :long :long]])))))))
      (is (some? @(:failure runtime)))
      (i/close-kv db)
      (let [reopened (l/open-kv dir opts)]
        (try
          (is (= 4 (i/get-value reopened "data" 1 :long :long)))
          (is (= [1 2] (i/get-value reopened "data" :generic :data :data)))
          (is (= [3] (vec (i/get-list reopened "items" 1 :long :long))))
          (is (= [(inc (long base)) (+ (long base) 2)]
                 (mapv :lsn (kv/open-tx-log reopened (inc (long base))))))
          (is (:ok? (i/verify-commit-marker! reopened)))
          (finally (i/close-kv reopened)))))))

(deftest read-only-preparation-waits-for-its-observed-prefix
  (doseq [fail? [false true]]
    (let [flush-entered (promise) read-observed (promise) release (CountDownLatch. 1)]
      (with-native
        {:kv-pipeline? true
         :hooks {:before-sync! (fn [_ _]
                                 (deliver flush-entered true)
                                 (.await release)
                                 (when fail? (throw (ex-info "Injected force failure" {}))))}}
        (fn [{:keys [pipeline db base]}]
          (let [writer (future (caught #(pending/submit-rows! pipeline 4096 (rows 1))))
                reader (atom nil)]
            (try
              (is (deref flush-entered 10000 false))
              (reset! reader
                      (future
                        (caught #(pending/submit!
                                  pipeline 4096
                                  (fn [tx]
                                    (let [value (pending/get-value tx "data" 1 :long :string)]
                                      (deliver read-observed value)
                                      value))))))
              (is (= "1" (deref read-observed 10000 ::timeout)))
              (is (nil? (i/get-value db "data" 1 :long :string)))
              (is (= ::waiting (deref @reader 30 ::waiting)))
              (.countDown release)
              (if fail?
                (do (is (instance? Throwable (join writer)))
                    (is (instance? Throwable (join @reader))))
                (do (is (= :transacted (join writer)))
                    (is (= "1" (join @reader)))))
              (is (= [(inc (long base))] (mapv :lsn (kv/open-tx-log db (inc (long base))))))
              (finally
                (.countDown release)
                (join writer)
                (when @reader (join @reader))))))))))

(deftest read-only-receipts-retain-capacity-until-completion
  (doseq [fail? [false true]]
    (let [flush-entered (promise) read-prepared (promise) capacity-waiting (promise)
          release (CountDownLatch. 1) prepared (atom 0)
          value (apply str (repeat 2048 \1))]
      (with-native
        {:kv-pipeline? true :wal-pending-max-requests 2 :wal-pending-max-bytes 8192
         :hooks {:before-sync! (fn [_ _]
                                 (deliver flush-entered true)
                                 (.await release)
                                 (when fail? (throw (ex-info "Injected force failure" {}))))}}
        (fn [{:keys [pipeline runtime]}]
          (with-redefs [state/phase! (fn [event _]
                                       (when (= event :capacity-wait)
                                         (deliver capacity-waiting true)))]
            (let [writer (future (caught #(pending/submit-rows! pipeline 4096 (rows 1))))
                  jobs (atom [writer])
                  read! #(pending/submit!
                          pipeline 4096
                          (fn [tx]
                            (swap! prepared inc)
                            (let [observed (pending/get-value tx "data" 1 :long :string)
                                  result (apply str (repeat 2048 observed))]
                              (deliver read-prepared true)
                              result)))]
              (try
                (is (deref flush-entered 10000 false))
                (let [reader (future (caught read!))]
                  (swap! jobs conj reader)
                  (is (deref read-prepared 10000 false))
                  ;; Cross the preparation boundary before inspecting the
                  ;; receipt's credits; its prefix is still awaiting force.
                  (let [^ReentrantLock preparation (:preparation runtime)]
                    (.lock preparation)
                    (try
                      (is (= {:bytes 8192 :requests 2} (state/usage runtime)))
                      (finally (.unlock preparation))))
                  (let [next-reader (future (caught read!))]
                    (swap! jobs conj next-reader)
                    (is (deref capacity-waiting 1000 false))
                    (is (= 1 @prepared)
                        "A full budget prevents another retained read result")
                    (is (= {:bytes 8192 :requests 2} (state/usage runtime)))
                    (.countDown release)
                    (if fail?
                      (do
                        (doseq [job @jobs] (is (instance? Throwable (join job))))
                        (is (= {:bytes 4096 :requests 1} (state/usage runtime))
                            "Only the failed appended write remains retained"))
                      (do
                        (is (= :transacted (join writer)))
                        (is (= value (join reader)))
                        (is (= value (join next-reader)))
                        (is (= 2 @prepared))
                        (is (= {:bytes 0 :requests 0} (state/usage runtime)))))))
                (finally
                  (.countDown release)
                  (doseq [job @jobs] (join job)))))))))))

(deftest preencoded-input-shares-collection-with-rmw-and-keeps-owned-bytes
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline db base]}]
      (is (= :transacted
             (pending/submit-rows! pipeline 8192
                                   [[:put "data" 1 "first" :long :string]
                                    [:put-list "items" 1 ["a" "b"] :long :string]])))
      (is (= "first"
             (pending/submit! pipeline 4096
                              #(pending/get-value % "data" 1 :long :string))))
      (pending/submit-rows! pipeline 8192
                            [[:del "data" 1 :long]
                             [:del-list "items" 1 ["a"] :long :string]])
      (is (nil? (i/get-value db "data" 1 :long :string)))
      (is (= ["b"] (vec (i/get-list db "items" 1 :long :string))))
      (is (= [(inc (long base)) (+ (long base) 2)]
             (mapv :lsn (kv/open-tx-log db (inc (long base)))))))))

(deftest blind-wal-encoding-does-not-occupy-preparation-or-collection
  (let [encoded (promise) release (CountDownLatch. 1)
        calls (atom []) prepare-body wal/prepare-append-body]
    (with-native
      {:kv-pipeline? true}
      (fn [{:keys [pipeline runtime db base]}]
        (with-redefs [wal/prepare-append-body
                      (fn [rows hooks]
                        (let [key (view/decode (nth (first rows) 2) :long)
                              owned (prepare-body rows hooks)]
                          (swap! calls conj [key (.isHeldByCurrentThread
                                                 ^ReentrantLock (:preparation runtime))])
                          (when (= 1 key)
                            (deliver encoded true)
                            (.await release))
                          owned))]
          (let [first-job (future (caught #(pending/submit-rows! pipeline 4096 (rows 1))))
                second-job (volatile! nil)]
            (try
              (is (deref encoded 10000 false))
              (is (= [[1 false]] @calls))
              (is (= {:bytes 4096 :requests 1} (state/usage runtime))
                  "The encoder retains admission credits before collection")
              (vreset! second-job
                       (future
                         (caught #(pending/submit!
                                   pipeline 4096
                                   (fn [tx]
                                     (let [before (pending/get-value tx "data" 1 :long :string)]
                                       (pending/transact! tx (rows 2))
                                       [before :rmw]))))))
              (is (= [nil :rmw] (deref @second-job 1000 ::blocked))
                  "Another group can append, sync and apply while encoding is blocked")
              (is (= "2" (i/get-value db "data" 2 :long :string)))
              (is (= [(inc (long base))]
                     (mapv :lsn (kv/open-tx-log db (inc (long base))))))
              (let [released-at (System/currentTimeMillis)]
                (.countDown release)
                (is (= :transacted (join first-job)))
                (is (= [[1 false] [2 true]] @calls)
                    "Blind encoding runs once, outside preparation; RMW encoding stays ordered")
                (is (= "1" (i/get-value db "data" 1 :long :string)))
                (let [records (vec (kv/open-tx-log db (inc (long base))))]
                  (is (= [(inc (long base)) (+ (long base) 2)] (mapv :lsn records)))
                  (is (<= released-at (:tx-time (peek records))))))
              (is (= {:bytes 0 :requests 0} (state/usage runtime)))
              (is (:ok? (i/verify-commit-marker! db)))
              (finally
                (.countDown release)
                (join first-job)
                (when @second-job (join @second-job))))))))))

(deftest ordered-rmw-body-runs-once-outside-the-collector
  (let [locks (atom [])]
    (with-native
      {:kv-pipeline? true}
      (fn [{:keys [pipeline runtime db]}]
        (is (= :transacted
               (pending/submit!
                pipeline 4096
                (fn [tx]
                  (swap! locks conj
                         [(.isHeldByCurrentThread ^ReentrantLock (:preparation runtime))
                          (.isHeldByCurrentThread ^ReentrantLock
                                                  (.-lock ^Group (:collector pipeline)))])
                  (pending/transact! tx (rows 1))))))
        (is (= [[true false]] @locks)
            "An opaque body executes once under ordering, without collector ownership")
        (is (= "1" (i/get-value db "data" 1 :long :string)))
        (is (= {:bytes 0 :requests 0} (state/usage runtime)))
        (is (:ok? (i/verify-commit-marker! db)))))))

(deftest replayable-preparation-is-concurrent-and-conflicts-fall-back-once
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db]}]
      (let [entered (CountDownLatch. 2) release (CountDownLatch. 1)
            calls (atom [0 0]) locks (atom [])
            jobs (mapv
                  (fn [idx]
                    (future
                      (let [caller (Thread/currentThread)]
                        (caught
                         #(pending/submit-replayable!
                           pipeline 4096
                           (fn [tx]
                             (let [attempt (nth (swap! calls update idx inc) idx)
                                   before (long (or (pending/get-value tx "data" 0 :long :long) 0))]
                               (swap! locks conj
                                      [attempt
                                       (identical? caller (Thread/currentThread))
                                       (.isHeldByCurrentThread ^ReentrantLock (:preparation runtime))
                                       (.isHeldByCurrentThread ^ReentrantLock
                                                               (.-lock ^Group (:collector pipeline)))])
                               (when (= 1 attempt)
                                 (.countDown entered)
                                 (assert (.await release 10 java.util.concurrent.TimeUnit/SECONDS)))
                               (pending/transact! tx [[:put "data" 0 (inc before) :long :long]])
                               (inc before))))))))
                  (range 2))]
        (try
          (is (.await entered 5 java.util.concurrent.TimeUnit/SECONDS)
              "Both bodies execute concurrently before either submits to collection")
          (is (= [1 1] @calls))
          (is (every? #(= [1 true false false] %) @locks))
          (.countDown release)
          (is (= #{1 2} (set (mapv join jobs))))
          (is (= [1 2] (sort @calls))
              "Only the stale candidate is evaluated a second time")
          (is (= [true false] (subvec (first (filter #(= 2 (first %)) @locks)) 2)))
          (is (= 2 (i/get-value db "data" 0 :long :long)))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))
          (is (empty? @(:pins runtime)))
          (is (zero? (:native-users (lifetime/state (:lifetime runtime)))))
          (is (:ok? (i/verify-commit-marker! db)))
          (finally (.countDown release) (doseq [job jobs] (join job))))))))

(deftest stalled-replayable-read-does-not-hold-collection-or-append
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db base]}]
      (let [entered (promise) release (CountDownLatch. 1) calls (atom 0)
            reader (future
                     (caught #(pending/submit-replayable!
                               pipeline 4096
                               (fn [tx]
                                 (let [value (pending/get-value tx "data" 1 :long :string)]
                                   (when (= 1 (swap! calls inc))
                                     (deliver entered true)
                                     (assert (.await release 10 java.util.concurrent.TimeUnit/SECONDS)))
                                   value)))))]
        (try
          (is (deref entered 5000 false))
          (is (= :transacted
                 (deref (future (pending/submit-rows! pipeline 4096 (rows 1)))
                        1000 ::blocked)))
          (is (= "1" (i/get-value db "data" 1 :long :string)))
          (.countDown release)
          (is (= "1" (join reader)) "A stale read-only result is recomputed before publication")
          (is (= 2 @calls))
          (is (= [(inc (long base))] (mapv :lsn (kv/open-tx-log db (inc (long base))))))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))
          (is (empty? @(:pins runtime)))
          (finally (.countDown release) (join reader)))))))

(deftest replayable-readers-close-on-the-caller-before-collection
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db]}]
      (let [prepared (promise) release (CountDownLatch. 1)]
        (with-redefs [state/phase!
                      (fn [event candidate]
                        (when (= :speculative-prepared event)
                          (deliver prepared (:pin candidate))
                          (assert (.await release 10 java.util.concurrent.TimeUnit/SECONDS))))]
          (let [job (future (caught #(pending/submit-replayable!
                                     pipeline 4096
                                     (fn [tx] (pending/transact! tx (rows 1))))))]
            (try
              (let [pin (deref prepared 5000 ::timeout)]
                (is (map? pin))
                (is (nil? (:reader pin)))
                (is (nil? (:lease pin)))
                (is (= 1 (count @(:pins runtime))))
                (is (= {:bytes 4096 :requests 1} (state/usage runtime)))
                (is (zero? (:native-users (lifetime/state (:lifetime runtime))))))
              (.countDown release)
              (is (= :transacted (join job)))
              (is (= "1" (i/get-value db "data" 1 :long :string)))
              (is (= {:bytes 0 :requests 0} (state/usage runtime)))
              (is (empty? @(:pins runtime)))
              (finally (.countDown release) (join job)))))))))

(deftest rejected-replayable-preparation-releases-its-reader-pin-and-budget
  (doseq [mode [:abort :body-error :encoding-error :budget]]
    (with-native
      {:kv-pipeline? true}
      (fn [{:keys [pipeline runtime wal db base]}]
        (let [failure (ex-info "Rejected speculative preparation" {})
              calls (atom 0)
              encode wal/prepare-append-body]
          (with-redefs [wal/prepare-append-body
                        (if (= mode :encoding-error) (fn [_ _] (throw failure)) encode)]
            (let [result (caught #(pending/submit-replayable!
                                  pipeline (if (= mode :budget) 512 4096)
                                  (fn [tx]
                                    (swap! calls inc)
                                    (case mode
                                      :abort (do (pending/transact! tx (rows 1))
                                                 (pending/abort! tx) :aborted)
                                      :body-error (throw failure)
                                      (pending/transact! tx (rows 1))))))]
              (if (= mode :abort)
                (is (= :aborted result))
                (is (instance? Throwable result)))
              (is (= (if (= mode :budget) 0 1) @calls))))
          (is (empty? (kv/open-tx-log db (inc (long base)))))
          (is (= (inc (long base)) @(:next-lsn wal)))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))
          (is (empty? @(:pins runtime)))
          (is (zero? (:native-users (lifetime/state (:lifetime runtime)))))
          (is (nil? @(:failure runtime))))))))

(deftest close-fences-off-collector-preparation-and-drains-its-reader
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db base]}]
      (let [entered (promise) release (CountDownLatch. 1) disposed (atom 0)
            job (future
                  (caught #(pending/submit-replayable!
                            pipeline 4096
                            (fn [tx]
                              (pending/get-value tx "data" 1 :long :string)
                              (deliver entered true)
                              (assert (.await release 10 java.util.concurrent.TimeUnit/SECONDS))
                              :prepared))))]
        (try
          (is (deref entered 5000 false))
          (is (pos? (:native-users (lifetime/state (:lifetime runtime)))))
          (is (instance? Throwable (caught #(state/close! runtime 10 (fn [] (swap! disposed inc))))))
          (is (zero? @disposed))
          (.countDown release)
          (is (instance? Throwable (join job)))
          (is (empty? (kv/open-tx-log db (inc (long base)))))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))
          (is (empty? @(:pins runtime)))
          (is (zero? (:native-users (lifetime/state (:lifetime runtime)))))
          (finally (.countDown release) (join job)))))))

(deftest replayable-body-is-not-repeated-after-a-durable-application-failure
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db base dir opts]}]
      (let [calls (atom 0)]
        (with-redefs [state/phase!
                      (fn [event _]
                        (when (= event :native-writer-acquired)
                          (throw (ex-info "Injected native application failure" {:applied? false}))))]
          (let [error (caught #(pending/submit-replayable!
                               pipeline 4096
                               (fn [tx]
                                 (swap! calls inc)
                                 (pending/transact! tx (rows 1)))))]
            (is (= :txlog/write-committed (:error (ex-data error))))
            (is (= :committed (:outcome (ex-data error))))
            (is (false? (:retryable? (ex-data error))))
            (is (= (inc (long base)) (:txlog-lsn (ex-data error))))))
        (is (= 1 @calls))
        (is (empty? @(:pins runtime)))
        (is (= [(inc (long base))] (mapv :lsn (kv/open-tx-log db (inc (long base))))))
        (state/close! runtime 1000 #(i/close-kv db))
        (let [reopened (l/open-kv dir opts)]
          (try
            (is (= "1" (i/get-value reopened "data" 1 :long :string)))
            (is (:ok? (i/verify-commit-marker! reopened)))
            (is (= 1 @calls))
            (finally (i/close-kv reopened))))))))

(deftest blind-wal-encoding-failure-releases-admission-without-fencing
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime wal db base]}]
      (let [failure (ex-info "Injected payload encoding failure" {})
            before @(:next-lsn wal)]
        (with-redefs [wal/prepare-append-body (fn [_ _] (throw failure))]
          (is (identical? failure
                          (caught #(pending/submit-rows! pipeline 4096 (rows 1))))))
        (is (= before @(:next-lsn wal)))
        (is (empty? (kv/open-tx-log db (inc (long base)))))
        (is (nil? @(:failure runtime)))
        (is (= {:bytes 0 :requests 0} (state/usage runtime)))
        (is (= :transacted (pending/submit-rows! pipeline 4096 (rows 2))))
        (is (= "2" (i/get-value db "data" 2 :long :string)))))))

(defn- queued-count? [pipeline n]
  (let [^ConcurrentLinkedQueue queue (.-queue ^Group (:collector pipeline))
        deadline (+ (System/nanoTime) 1000000000)]
    (loop []
      (cond (= n (.size queue)) true
            (>= (System/nanoTime) deadline) false
            :else (do (Thread/sleep 1) (recur))))))

(deftest collected-blind-and-rmw-payloads-keep-the-same-request-order
  (with-native
    {:kv-pipeline? true :write-batch-size 8 :wal-preparation-max-nanos 0}
    (fn [{:keys [pipeline db base]}]
      (let [batches (atom []) jobs (atom [])
            ^ReentrantLock lock (.-lock ^Group (:collector pipeline))]
        (with-redefs [state/phase! (fn [event context]
                                     (when (= event :receipt-published)
                                       (swap! batches conj @(:append-batch context))))]
          ;; Seal an already-ready group. Bodies no longer keep collection open.
          (.lock lock)
          (try
            (swap! jobs conj (future (caught #(pending/submit!
                                               pipeline 4096
                                               (fn [tx]
                                                 (pending/transact! tx (rows 1))
                                                 :leader)))))
            (is (queued-count? pipeline 1))
            (swap! jobs conj (future (caught #(pending/submit-rows! pipeline 4096 (rows 2)))))
            (is (queued-count? pipeline 2))
            (swap! jobs conj
                   (future
                     (caught #(pending/submit!
                               pipeline 8192
                               (fn [tx]
                                 (let [before (pending/get-value tx "data" 2 :long :string)]
                                   (pending/transact! tx [[:put "data" 2 (str before "!") :long :string]])
                                   (pending/transact! tx (rows 3))
                                   before))))))
            (is (queued-count? pipeline 3))
            (finally (.unlock lock)))
          (try
            (is (= [:leader :transacted "2"] (mapv join @jobs)))
            (is (= 3 (count @batches)))
            (is (every? #(identical? (first @batches) %) @batches))
            (is (= [[1 "1"] [2 "2!"] [3 "3"]]
                   (vec (i/get-range db "data" [:all] :long :string))))
            (is (= [(inc (long base))]
                   (mapv :lsn (kv/open-tx-log db (inc (long base))))))
            (is (:ok? (i/verify-commit-marker! db)))
            (finally (doseq [job @jobs] (join job)))))))))

(deftest read-only-member-observes-preappend-group-rejection
  (with-native
    {:kv-pipeline? true :wal-preparation-max-nanos 0}
    (fn [{:keys [pipeline runtime db base]}]
      (let [failure (ex-info "Injected group encoding failure" {:outcome :not-committed})
            jobs (atom [])
            ^ReentrantLock lock (.-lock ^Group (:collector pipeline))]
        (with-redefs [wal/prepare-append-body (fn [_ _] (throw failure))]
          (.lock lock)
          (try
            (swap! jobs conj (future (caught #(pending/submit!
                                               pipeline 4096
                                               (fn [tx]
                                                 (pending/transact! tx (rows 1))
                                                 :written)))))
            (is (queued-count? pipeline 1))
            (swap! jobs conj (future (caught #(pending/submit!
                                               pipeline 4096
                                               (fn [tx] (pending/get-value tx "data" 1 :long :string))))))
            (is (queued-count? pipeline 2))
            (finally (.unlock lock)))
          (try
            (is (every? #(identical? failure %) (mapv join @jobs)))
            (is (nil? @(:failure runtime)))
            (is (= {:bytes 0 :requests 0} (state/usage runtime)))
            (is (empty? (kv/open-tx-log db (inc (long base)))))
            (finally (doseq [job @jobs] (join job)))))
        (is (= :transacted (pending/submit-rows! pipeline 4096 (rows 2))))
        (is (= "2" (i/get-value db "data" 2 :long :string)))))))

(deftest bounded-preparation-appends-before-running-a-slow-successor
  (let [clock (atom 0) first-entered (promise) release-first (CountDownLatch. 1)
        second-entered (promise) release-second (CountDownLatch. 1)
        calls (atom [])]
    (with-native
      {:kv-pipeline? true :write-batch-size 8 :wal-preparation-max-nanos 10}
      (fn [{:keys [pipeline runtime db raw base]}]
        (with-redefs [group/preparation-nano-time (fn ^long [] (long @clock))]
          (let [a (future
                    (caught #(pending/submit!
                              pipeline 4096
                              (fn [tx]
                                (swap! calls conj :first)
                                (pending/transact! tx (rows 1))
                                (deliver first-entered true)
                                (.await release-first)
                                (reset! clock 10)
                                :first))))
                b (volatile! nil)]
            (try
              (is (deref first-entered 10000 false))
              (vreset! b (future
                           (caught #(pending/submit!
                                     pipeline 4096
                                     (fn [tx]
                                       (swap! calls conj :second)
                                       (let [value (pending/get-value tx "data" 1 :long :string)]
                                         (deliver second-entered value)
                                         (.await release-second)
                                         (pending/transact! tx (rows 2))
                                         value))))))
              (let [^ReentrantLock lock (.-lock ^Group (:collector pipeline))]
                (is (.tryLock lock 1000 java.util.concurrent.TimeUnit/MILLISECONDS)
                    "The first body does not retain collector ownership")
                (when (.isHeldByCurrentThread lock) (.unlock lock)))
              (.countDown release-first)
              (is (= "1" (deref second-entered 10000 ::timeout)))
              (is (= :first (deref a 1000 ::blocked))
                  "First record flushes and publishes while the successor owns preparation")
              (is (= "1" (i/get-value raw "data" 1 :long :string)))
              (is (= [(inc (long base))]
                     (mapv :lsn (kv/open-tx-log db (inc (long base))))))
              (is (not (realized? @b)))
              (.countDown release-second)
              (is (= "1" (join @b)))
              (is (= [:first :second] @calls))
              (is (= [(inc (long base)) (+ (long base) 2)]
                     (mapv :lsn (kv/open-tx-log db (inc (long base))))))
              (is (= {:bytes 0 :requests 0} (state/usage runtime)))
              (is (:ok? (i/verify-commit-marker! db)))
              (finally
                (.countDown release-first) (.countDown release-second)
                (join a) (when @b (join @b))))))))))

(deftest native-owner-must-exit-before-facade-close-and-reopen
  (let [entered (promise) release (CountDownLatch. 1)]
    (with-native
      {:kv-pipeline? true :wal-close-timeout-ms 10 :wal-apply-timeout-ms 100}
      (fn [{:keys [pipeline db raw runtime wal dir opts base]}]
        (with-redefs [state/phase!
                      (fn [event _]
                        (when (= event :native-writer-acquired)
                          (deliver entered (Thread/currentThread))
                          (loop []
                            (when-not (try (.await release) true
                                           (catch InterruptedException _ false))
                              (recur)))))]
          (let [job (future (caught #(pending/submit!
                                      pipeline 4096
                                      (fn [tx] (pending/transact! tx (rows 1))))))]
            (try
              (let [^Thread owner (deref entered 10000 nil)]
                (is (some? owner))
                (when owner (.interrupt owner)))
              (let [error (caught #(i/close-kv db))]
                (is (= :txlog/native-not-quiescent (:error (ex-data error))))
                (is (:process-restart-required? (ex-data error))))
              (is (not (i/closed-kv? raw)))
              (is (.isOpen ^java.nio.channels.FileChannel @(:segment-channel wal)))
              (is (= :fenced (:phase (lifetime/state (:lifetime runtime)))))
              (is (instance? Throwable (caught #(l/open-kv dir opts))))
              (.countDown release)
              (is (= :txlog/write-committed (:error (ex-data (join job)))))
              (is (= base @(:published runtime))
                  "Only the preexisting base remains published")
              (i/close-kv db)
              (is (i/closed-kv? raw))
              (let [reopened (l/open-kv dir opts)]
                (try (is (= "1" (i/get-value reopened "data" 1 :long :string)))
                     (finally (i/close-kv reopened))))
              (finally (.countDown release) (join job)))))))))

(deftest a-borrowed-native-reader-keeps-the-environment-alive-after-fencing
  (with-native
    {:kv-pipeline? true :wal-close-timeout-ms 10
     :initial-rows [[:put "data" 1 "before" :long :string]]}
    (fn [{:keys [raw db runtime]}]
      (let [reader (i/get-rtx raw)
            handle (i/get-dbi raw "data" false)]
        (try
          (is (= :txlog/native-not-quiescent
                 (:error (ex-data (caught #(i/close-kv db))))))
          (is (not (i/closed-kv? raw)))
          (l/put-read-key handle reader 1 :long)
          (is (= "before" (view/decode
                           (let [^java.nio.ByteBuffer buffer (l/get-kv handle reader)
                                 bytes (byte-array (.remaining buffer))]
                             (.get buffer bytes) bytes) :string)))
          (is (= :fenced (:phase (lifetime/state (:lifetime runtime)))))
          (finally (i/return-rtx raw reader)))
        (i/close-kv db)
        (is (i/closed-kv? raw))))))

(deftest read-adapters-retain-one-lease-at-the-borrow-boundary
  (with-native
    {:kv-pipeline? true :initial-rows (rows 1)}
    (fn [{:keys [raw runtime]}]
      (let [calls (atom 0)
            lock (proxy [java.util.concurrent.locks.ReentrantLock] []
                   (lock []
                     (swap! calls inc)
                     (let [^java.util.concurrent.locks.ReentrantLock this this]
                       (proxy-super lock))))
            guard (:lifetime runtime)
            changed (.newCondition lock)
            instrumented (assoc guard :lock lock :changed changed
                                :users (datalevin.utl.NativeUsers. lock changed))]
        (vswap! (i/kv-info raw) assoc :native-lifetime instrumented)
        (try
          (i/get-value raw "data" 1 :long :string)
          (is (nil? ((:unpublished-reader-lsn @(i/kv-info raw)) nil))
              "An idle runtime never probes the native publication marker")
          (doseq [read! [#(i/get-value raw "data" 1 :long :string)
                         #(i/get-range raw "data" [:all] :long :string)
                         #(i/range-count raw "data" [:all] :long)
                         #(i/entries raw "data")
                         #(i/stat raw "data")]]
            (reset! calls 0)
            (read!)
            (is (zero? @calls) "Steady-state borrowing takes no shared lifetime lock")
            (is (zero? (:native-users (lifetime/state instrumented)))))
          (finally (vswap! (i/kv-info raw) assoc :native-lifetime guard)))))))

(deftest kv-capacity-and-preparation-waits-use-the-submission-budget
  (doseq [blocked [:capacity :preparation]]
    (with-native
      {:kv-pipeline? true :wal-preparation-timeout-ms 20
       :wal-pending-max-bytes 4096}
      (fn [{:keys [pipeline runtime wal base]}]
        (let [reserved (state/reserve! runtime (if (= blocked :capacity) 4096 0) 5000)
              token (when (= blocked :preparation)
                      (state/acquire-preparation! runtime reserved 1000))]
          (try
            (let [result (join (future (caught #(pending/submit-rows! pipeline 4096 (rows 1)))))]
              (is (instance? Throwable result))
              (is (= :not-committed (:outcome (ex-data result))))
              (is (= (if (= blocked :capacity) :txlog/pending-capacity
                         :txlog/preparation-timeout) (:error (ex-data result))))
              (is (= (inc (long base)) @(:next-lsn wal)))
              (is (nil? @(:failure runtime))))
            (finally
              (when token (state/release-preparation! token))
              (state/release! reserved)))
          (is (= {:bytes 0 :requests 0} (state/usage runtime))))))))

(deftest expired-kv-body-never-appends-or-poisons-admission
  (doseq [submit [pending/submit! pending/submit-replayable!]]
    (with-native
      {:kv-pipeline? true :wal-preparation-timeout-ms 20}
      (fn [{:keys [pipeline runtime wal base]}]
        (let [now (atom (System/nanoTime))]
          (with-redefs [lifetime/nano-time (fn ^long [] (long @now))]
            (is (= :not-committed
                   (:outcome (ex-data
                              (caught #(submit
                                        pipeline 4096
                                        (fn [tx]
                                          (pending/transact! tx (rows 1))
                                          (swap! now + 30000000)))))))))
          (is (= (inc (long base)) @(:next-lsn wal)))
          (is (nil? @(:failure runtime)))
          (is (= {:bytes 0 :requests 0} (state/usage runtime)))
          (is (empty? @(:pins runtime)))
          (is (= :transacted (pending/submit-rows! pipeline 4096 (rows 2)))))))))

(deftest public-readers-wait-for-publication-and-wake-on-failure
  (doseq [fail? [false true]]
    (let [committed (promise) read-waiting (promise) release (CountDownLatch. 1)]
      (with-native
        {:kv-pipeline? true}
        (fn [{:keys [pipeline raw runtime base]}]
          (let [info (i/kv-info raw)
                check (:unpublished-reader-lsn @info)]
            (vswap! info assoc :unpublished-reader-lsn
                    (fn [reader]
                      (when-let [lsn (check reader)]
                        (deliver read-waiting true)
                        lsn))))
          (with-redefs [state/phase!
                        (fn [event _]
                          (when (= event :after-native-commit)
                            (deliver committed true)
                            (.await release)
                            (when fail?
                              (throw (ex-info "Injected post-commit failure"
                                              {:applied? true})))))]
            (let [writer (future (caught #(pending/submit-rows! pipeline 4096 (rows 1))))
                  reader (atom nil)]
              (try
                (is (deref committed 10000 false))
                (is (= base @(:published runtime)))
                ;; Only preparation may capture this native snapshot, matched
                ;; to its retained root; it must not wait for public visibility.
                (let [snapshot (pending/native-snapshot raw)]
                  (try (is (= (inc (long base)) (:base-lsn snapshot)))
                       (finally ((:close! snapshot)))))
                (reset! reader (future (caught #(i/get-value raw "data" 1 :long :string))))
                (is (deref read-waiting 10000 false))
                (is (= ::waiting (deref @reader 20 ::waiting)))
                (.countDown release)
                (if fail?
                  (do
                    (is (= :txlog/write-committed (:error (ex-data (join writer)))))
                    (is (instance? Throwable (join @reader)))
                    (is (= base @(:published runtime))))
                  (do
                    (is (= :transacted (join writer)))
                    (is (= "1" (join @reader)))))
                (is (zero? (:native-users (lifetime/state (:lifetime runtime)))))
                (finally
                  (.countDown release)
                  (join writer)
                  (when @reader (join @reader)))))))))))

(deftest published-records-retain-budget-until-old-read-pins-are-released
  (let [entered (promise) release (CountDownLatch. 1)]
    (with-native
      {:kv-pipeline? true
       :hooks {:before-sync! (fn [_ _] (deliver entered true) (.await release))}}
      (fn [{:keys [pipeline raw runtime]}]
        (let [job (future (pending/submit! pipeline 4096
                                           #(pending/transact! % (rows 1))))]
          (try
            (is (deref entered 10000 false))
            (let [reservation (state/reserve! runtime 4096 1000)
                  token (state/acquire-preparation! runtime reservation 1000)
                  snapshot (state/capture-view runtime @(:root runtime)
                                               #(pending/native-snapshot raw))]
              (state/release-preparation! token)
              (try
                (.countDown release)
                (is (= :transacted (join job)))
                (is (= {:bytes 8192 :requests 2} (state/usage runtime)))
                (is (= [[1 "1"]]
                       (view/with-rows raw (:reader snapshot) (:root snapshot)
                         (:base-lsn snapshot) "data" [:all] :long [:all] :string
                         #(mapv (fn [[k v]] [(view/decode k :long)
                                             (view/decode v :string)]) %))))
                (finally (state/release-view! snapshot)))
              (is (= {:bytes 4096 :requests 1} (state/usage runtime)))
              (state/release! reservation)
              (is (= {:bytes 0 :requests 0} (state/usage runtime))))
            (finally (.countDown release) (join job))))))))

(deftest arrivals-during-the-preparation-wait-share-one-wal-record
  (with-native
    {:kv-pipeline? true}
    (fn [{:keys [pipeline runtime db base]}]
      (let [held (state/reserve! runtime 1 5000)
            token (state/acquire-preparation! runtime held 1000)
            ^ReentrantLock preparation (:preparation runtime)
            ^ConcurrentLinkedQueue queue (.-queue ^Group (:collector pipeline))
            first-job (future (caught #(pending/submit-rows! pipeline 4096 (rows 1))))
            second-job (atom nil)]
        (try
          ;; The first runner releases collector leadership and waits here for
          ;; the ordered preparation turn; a request arriving now must join it.
          (is (loop [attempt 0]
                (cond (.hasQueuedThreads preparation) true
                      (= attempt 5000) false
                      :else (do (Thread/sleep 1) (recur (inc attempt))))))
          (reset! second-job
                  (future (caught #(pending/submit-rows! pipeline 4096 (rows 2)))))
          (is (loop [attempt 0]
                (cond (pos? (.size queue)) true
                      (= attempt 5000) false
                      :else (do (Thread/sleep 1) (recur (inc attempt))))))
          (finally
            (state/release-preparation! token)
            (state/release! held)))
        (is (= :transacted (join first-job)))
        (is (= :transacted (join @second-job)))
        (is (= [[1 "1"] [2 "2"]] (vec (i/get-range db "data" [:all] :long :string))))
        (is (= [(inc (long base))]
               (mapv :lsn (kv/open-tx-log db (inc (long base)))))
            "both arrivals share one physical WAL record/LSN")))))
