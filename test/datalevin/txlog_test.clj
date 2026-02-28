(ns datalevin.txlog-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.java.io :as io]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.interface :as i]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as l]
   [datalevin.txlog :as sut]
   [datalevin.util :as u])
  (:import
   [java.io RandomAccessFile]
   [java.nio ByteBuffer]
   [java.nio.channels FileChannel]
   [java.nio.file Files StandardOpenOption]
   [java.nio.charset StandardCharsets]
   [java.util UUID Random]))

(defmacro with-temp-dir
  [[sym] & body]
  `(let [~sym (u/tmp-dir (str "txlog-test-" (UUID/randomUUID)))]
     (u/create-dirs ~sym)
     (try
       ~@body
       (finally
         (u/delete-files ~sym)))))

(defn- ->bytes
  [^String s]
  (.getBytes s StandardCharsets/UTF_8))

(defn- open-fuzz-lmdb
  [dir opts]
  (let [lmdb (l/open-kv dir opts)]
    (i/open-dbi lmdb "fuzz")
    lmdb))

(defn- long-kv-range->sorted-map
  [lmdb dbi]
  (into (sorted-map) (i/get-range lmdb dbi [:all] :long :long)))

(defn- txlog-lsns
  [dir]
  (let [txlog-dir (str dir u/+separator+ "txlog")]
    (->> (sut/segment-files txlog-dir)
         (mapcat (fn [{:keys [file]}]
                   (let [{:keys [records]}
                         (sut/scan-segment (.getPath file)
                                           {:allow-preallocated-tail? true})]
                     (mapv (fn [{:keys [body]}]
                             (-> body
                                 sut/decode-commit-row-payload
                                 :lsn
                                 long))
                           records))))
         vec)))

(defn- contiguous-lsns?
  [lsns]
  (every? (fn [[a b]] (= (inc ^long a) ^long b))
          (partition 2 1 lsns)))

(defn- apply-fuzz-row
  [model row]
  (let [[op dbi k v] row]
    (if (= "fuzz" dbi)
      (case op
        :put (assoc model (long k) (long v))
        :del (dissoc model (long k))
        model)
      model)))

(defn- apply-fuzz-rows
  [model rows]
  (reduce apply-fuzz-row model rows))

(defn- replay-fuzz-model
  [records]
  (reduce (fn [model {:keys [ops]}]
            (apply-fuzz-rows model ops))
          (sorted-map)
          records))

(defn- expected-open-range
  [records from-lsn upto-lsn]
  (->> records
       (filter #(>= (long (:lsn %)) (long from-lsn)))
       (filter #(if (some? upto-lsn)
                  (<= (long (:lsn %)) (long upto-lsn))
                  true))
       vec))

(defn- assert-open-txlog-range-and-replay!
  [lmdb model ^Random rng]
  (let [records (vec (i/open-tx-log lmdb 0))
        lsns    (mapv (comp long :lsn) records)
        max-lsn (long (or (last lsns) 0))]
    (is (= model (long-kv-range->sorted-map lmdb "fuzz")))
    (is (= model (replay-fuzz-model records)))
    (is (:ok? (i/verify-commit-marker! lmdb)))
    (when (seq lsns)
      (is (contiguous-lsns? lsns))
      (is (= 1 (first lsns)))
      (is (= (long (count lsns)) max-lsn)))
    (dotimes [_ 6]
      (let [from      (long (.nextInt rng (int (+ 3 max-lsn))))
            choose?   (.nextBoolean rng)
            upto-lsn  (when choose?
                        (long (+ from (.nextInt rng 8))))
            expected  (expected-open-range records from upto-lsn)
            actual    (if (some? upto-lsn)
                        (vec (i/open-tx-log lmdb from upto-lsn))
                        (vec (i/open-tx-log lmdb from)))]
        (is (= expected actual))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (i/open-tx-log lmdb 10 9)))))

(defn- assert-open-vs-segment-lsns!
  [dir lmdb model]
  (let [records      (vec (i/open-tx-log lmdb 0))
        api-lsns     (mapv (comp long :lsn) records)
        segment-lsns (txlog-lsns dir)]
    (is (= model (long-kv-range->sorted-map lmdb "fuzz")))
    (is (:ok? (i/verify-commit-marker! lmdb)))
    (is (= segment-lsns api-lsns))
    (when (seq api-lsns)
      (is (contiguous-lsns? api-lsns))
      (is (apply < api-lsns)))))

(deftest txn-log-fuzz-open-range-and-replay-test
  (doseq [seed [11 29 47]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      2
                    :wal-segment-max-bytes 96
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 360)
                (assert-open-txlog-range-and-replay! @lmdb-v model rng)
                (let [op   (.nextInt rng 100)
                      lmdb @lmdb-v]
                  (cond
                    (< op 66)
                    (let [rows (vec (for [_ (range (inc (.nextInt rng 4)))]
                                      (let [k (long (.nextInt rng 128))]
                                        (if (< (.nextInt rng 100) 72)
                                          [:put "fuzz"
                                           k
                                           (long (.nextInt rng 1000000))
                                           :long
                                           :long]
                                          [:del "fuzz" k :long]))))]
                      (i/transact-kv lmdb rows)
                      (recur (inc step) (apply-fuzz-rows model rows)))

                    (< op 82)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))

                    (< op 92)
                    (do
                      (assert-open-txlog-range-and-replay! lmdb model rng)
                      (recur (inc step) model))

                    :else
                    (do
                      (is (= model (long-kv-range->sorted-map lmdb "fuzz")))
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-lmdb dir opts))
                      (assert-open-txlog-range-and-replay! @lmdb-v model rng)
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-segment-scan-gc-reopen-parity-test
  (doseq [seed [2 9 27]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 88
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 420)
                (assert-open-vs-segment-lsns! dir @lmdb-v model)
                (let [op   (.nextInt rng 100)
                      lmdb @lmdb-v]
                  (cond
                    (< op 58)
                    (let [k (long (.nextInt rng 128))
                          v (long (.nextInt rng 1000000))]
                      (i/transact-kv lmdb [[:put "fuzz" k v :long :long]])
                      (recur (inc step) (assoc model k v)))

                    (< op 80)
                    (let [k (long (.nextInt rng 128))]
                      (i/transact-kv lmdb [[:del "fuzz" k :long]])
                      (recur (inc step) (dissoc model k)))

                    (< op 88)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))

                    (< op 96)
                    (let [watermarks (i/txlog-watermarks lmdb)
                          applied-lsn (long (or (:applied-lsn watermarks) 0))
                          floor       (when (and (pos? applied-lsn)
                                                 (.nextBoolean rng))
                                        (let [delta (inc (.nextInt
                                                          rng
                                                          (inc (min 32
                                                                    (int applied-lsn)))))]
                                          (long (max 0 (- applied-lsn delta)))))
                          gc-res      (if (some? floor)
                                        (i/gc-txlog-segments! lmdb floor)
                                        (i/gc-txlog-segments! lmdb))
                          retention   (i/txlog-retention-state lmdb)]
                      (is (<= 0 (long (or (:deleted-count gc-res) 0))))
                      (is (<= 0 (long (or (:safety-deletable-segment-count
                                           retention)
                                          0))))
                      (let [records (vec (i/open-tx-log lmdb 0))]
                        (when (seq records)
                          (is (<= (long (or (:min-retained-lsn retention) 0))
                                  (long (:lsn (first records)))))))
                      (assert-open-vs-segment-lsns! dir lmdb model)
                      (recur (inc step) model))

                    :else
                    (do
                      (is (= model (long-kv-range->sorted-map lmdb "fuzz")))
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-lmdb dir opts))
                      (assert-open-vs-segment-lsns! dir @lmdb-v model)
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-relaxed-recovery-and-gc-test
  (doseq [seed [7 19 41]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 128
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 400)
                (do
                  (is (= model (long-kv-range->sorted-map @lmdb-v "fuzz")))
                  (is (:ok? (i/verify-commit-marker! @lmdb-v)))
                  (let [ret    (i/txlog-retention-state @lmdb-v)
                        gc-res (i/gc-txlog-segments! @lmdb-v)]
                    (is (<= 0 (or (:safety-deletable-segment-count ret) -1)))
                    (is (<= 0 (or (:deleted-count gc-res) -1))))
                  (let [segments (sut/segment-files
                                  (str dir u/+separator+ "txlog"))]
                    (is (<= 1 (count segments)))))
                (let [op   (.nextInt rng 100)
                      k    (long (.nextInt rng 96))
                      v    (long (.nextInt rng 1000000))
                      lmdb @lmdb-v]
                  (cond
                    (< op 58)
                    (do
                      (i/transact-kv lmdb [[:put "fuzz" k v :long :long]])
                      (recur (inc step) (assoc model k v)))

                    (< op 82)
                    (do
                      (i/transact-kv lmdb [[:del "fuzz" k :long]])
                      (recur (inc step) (dissoc model k)))

                    (< op 90)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))

                    (< op 96)
                    (do
                      (if (.nextBoolean rng)
                        (i/gc-txlog-segments! lmdb)
                        (let [watermarks (i/txlog-watermarks lmdb)
                              floor      (max 0
                                              (- (long (or (:applied-lsn
                                                            watermarks)
                                                           0))
                                                 (.nextInt rng 16)))]
                          (i/gc-txlog-segments! lmdb floor)))
                      (recur (inc step) model))

                    :else
                    (do
                      (is (= model (long-kv-range->sorted-map lmdb "fuzz")))
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-lmdb dir opts))
                      (is (= model (long-kv-range->sorted-map @lmdb-v "fuzz")))
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-strict-lsn-contiguity-test
  (doseq [seed [5 13 23]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-durability-profile :strict
                    :wal-sync-mode         :none
                    :wal-segment-max-bytes 96
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 280)
                (do
                  (is (= model (long-kv-range->sorted-map @lmdb-v "fuzz")))
                  (is (:ok? (i/verify-commit-marker! @lmdb-v)))
                  (i/close-kv @lmdb-v)
                  (vreset! lmdb-v nil)
                  (let [lsns (txlog-lsns dir)]
                    (is (seq lsns))
                    (is (apply < lsns))
                    (is (contiguous-lsns? lsns))
                    (is (= (long (count lsns)) (long (last lsns)))))
                  (vreset! lmdb-v (open-fuzz-lmdb dir opts))
                  (is (= model (long-kv-range->sorted-map @lmdb-v "fuzz"))))
                (let [op   (.nextInt rng 100)
                      k    (long (.nextInt rng 128))
                      v    (long (.nextInt rng 1000000))
                      lmdb @lmdb-v]
                  (cond
                    (< op 70)
                    (do
                      (i/transact-kv lmdb [[:put "fuzz" k v :long :long]])
                      (recur (inc step) (assoc model k v)))

                    (< op 95)
                    (do
                      (i/transact-kv lmdb [[:del "fuzz" k :long]])
                      (recur (inc step) (dissoc model k)))

                    :else
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(defn- apply-key-iter-op
  [model {:keys [op k v]}]
  (case op
    :put (assoc model (long k) (long v))
    :del (dissoc model (long k))
    model))

(defn- apply-key-iter-ops
  [model ops]
  (reduce apply-key-iter-op model ops))

(defn- key-iter-ops->rows
  [ops]
  (mapv (fn [{:keys [op k v]}]
          (case op
            :put [:put "fuzz" (long k) (long v) :long :long]
            :del [:del "fuzz" (long k) :long]))
        ops))

(defn- verify-key-iter-model!
  [lmdb dbi ref-map]
  (is (= (vec ref-map)
         (vec (i/get-range lmdb dbi [:all] :long :long))))
  (is (= (if (empty? ref-map) [] (vec (rseq ref-map)))
         (vec (i/get-range lmdb dbi [:all-back] :long :long))))
  (is (= (vec (keys ref-map))
         (vec (i/key-range lmdb dbi [:all] :long))))
  (is (= (count ref-map)
         (i/range-count lmdb dbi [:all] :long)))
  (is (= (count ref-map)
         (i/key-range-count lmdb dbi [:all] :long)))
  (let [visited (volatile! [])]
    (i/visit-key-range lmdb dbi (fn [k] (vswap! visited conj k))
                       [:all] :long false)
    (is (= (vec (keys ref-map)) @visited)))
  (let [visited (volatile! [])]
    (i/visit lmdb dbi (fn [k v] (vswap! visited conj [k v]))
             [:all] :long :long false)
    (is (= (vec ref-map) @visited)))
  (when (>= (count ref-map) 2)
    (let [ks (vec (keys ref-map))
          lo (first ks)
          hi (last ks)]
      (is (= (vec (subseq ref-map >= lo <= hi))
             (vec (i/get-range lmdb dbi [:closed lo hi] :long :long))))
      (when (> (count ks) 2)
        (let [mid (nth ks (quot (count ks) 2))]
          (is (= (vec (subseq ref-map >= mid))
                 (vec (i/get-range lmdb dbi [:at-least mid] :long :long))))
          (is (= (vec (subseq ref-map <= mid))
                 (vec (i/get-range lmdb dbi [:at-most mid] :long :long))))
          (is (= (vec (subseq ref-map > mid))
                 (vec (i/get-range lmdb dbi [:greater-than mid] :long :long)))))))))

(defn- random-key-iter-op
  [^Random rng key-bound val-bound]
  (let [k (long (.nextInt rng (int key-bound)))]
    (if (< (.nextInt rng 100) 68)
      {:op :put :k k :v (long (.nextInt rng (int val-bound)))}
      {:op :del :k k})))

(defn- random-key-iter-ops
  [^Random rng]
  (vec (repeatedly (inc (.nextInt rng 6))
                   #(random-key-iter-op rng 192 1000000))))

(defn- open-fuzz-list-lmdb
  [dir opts]
  (let [lmdb (l/open-kv dir opts)]
    (i/open-list-dbi lmdb "fuzzl")
    lmdb))

(defn- random-distinct-longs
  [^Random rng n bound]
  (loop [acc (sorted-set)]
    (if (>= (count acc) n)
      (vec acc)
      (recur (conj acc (long (.nextInt rng (int bound))))))))

(defn- random-list-iter-op
  [^Random rng key-bound val-bound]
  (let [k (long (.nextInt rng (int key-bound)))
        roll (.nextInt rng 100)]
    (cond
      (< roll 55)
      {:op :put-list
       :k  k
       :vs (random-distinct-longs rng (inc (.nextInt rng 5)) val-bound)}

      (< roll 85)
      {:op :del-list
       :k  k
       :vs (random-distinct-longs rng (inc (.nextInt rng 4)) val-bound)}

      :else
      {:op :wipe :k k})))

(defn- random-list-iter-ops
  [^Random rng]
  (vec (repeatedly (inc (.nextInt rng 5))
                   #(random-list-iter-op rng 128 256))))

(defn- random-key-from-model
  [^Random rng model]
  (let [ks (vec (keys model))]
    (when (seq ks)
      (nth ks (.nextInt rng (int (count ks)))))))

(defn- first-missing-at-or-above
  [s start]
  (loop [candidate (long start)]
    (if (contains? s candidate)
      (recur (inc candidate))
      candidate)))

(defn- miss-heavy-del-list-values
  [^Random rng s]
  (if (seq s)
    (let [lo       (long (first s))
          hi       (long (last s))
          mid      (long (quot (+ lo hi) 2))
          miss-mid (first-missing-at-or-above s mid)
          miss-hi  (first-missing-at-or-above s
                                              (+ hi 1 (.nextInt rng 16)))]
      (->> [-2 -1 lo miss-mid hi miss-hi]
           (map long)
           distinct
           sort
           vec))
    (->> [-2 -1
          (long (.nextInt rng 320))
          (long (+ 512 (.nextInt rng 320)))]
         distinct
         sort
         vec)))

(defn- apply-list-iter-op
  [model {:keys [op k vs]}]
  (let [k (long k)]
    (case op
      :put-list
      (update model k (fn [s] (into (or s (sorted-set)) (map long vs))))

      :del-list
      (if-let [s (get model k)]
        (let [new-s (reduce disj s (map long vs))]
          (if (empty? new-s) (dissoc model k)
              (assoc model k new-s)))
        model)

      :wipe
      (dissoc model k)

      model)))

(defn- apply-list-iter-ops
  [model ops]
  (reduce apply-list-iter-op model ops))

(defn- mixed-hit-miss-del?
  [s vs]
  (and (seq s)
       (some #(contains? s (long %)) vs)
       (some #(not (contains? s (long %))) vs)))

(defn- apply-list-iter-ops+mixed-del-count
  [model ops]
  (reduce (fn [[m mixed-del-count] {:keys [op k vs] :as tx-op}]
            (let [s             (get m (long k))
                  mixed?        (and (= op :del-list)
                                     (mixed-hit-miss-del? s vs))
                  next-model    (apply-list-iter-op m tx-op)
                  mixed-next    (if mixed?
                                  (inc mixed-del-count)
                                  mixed-del-count)]
              [next-model mixed-next]))
          [model 0]
          ops))

(defn- list-iter-ops->rows
  [ops]
  (mapv (fn [{:keys [op k vs]}]
          (case op
            :put-list [:put-list "fuzzl" (long k) (mapv long vs) :long :long]
            :del-list [:del-list "fuzzl" (long k) (mapv long vs) :long :long]
            :wipe [:del "fuzzl" (long k) :long]))
        ops))

(defn- random-miss-heavy-list-op
  [^Random rng model]
  (let [roll       (.nextInt rng 100)
        existing-k (when (seq model)
                     (random-key-from-model rng model))
        pick-k     (fn [prefer-existing?]
                     (if (and prefer-existing?
                              existing-k
                              (< (.nextInt rng 100) 85))
                       existing-k
                       (long (.nextInt rng 40))))]
    (cond
      (< roll 40)
      {:op :put-list
       :k  (pick-k false)
       :vs (random-distinct-longs rng (inc (.nextInt rng 5)) 320)}

      (< roll 92)
      (let [k (pick-k true)]
        {:op :del-list
         :k  k
         :vs (miss-heavy-del-list-values rng (get model k))})

      :else
      {:op :wipe
       :k  (pick-k true)})))

(defn- random-list-churn-ops
  [^Random rng model]
  (let [existing-k (when (seq model)
                     (random-key-from-model rng model))
        k-main     (if (and existing-k
                            (< (.nextInt rng 100) 82))
                     existing-k
                     (long (.nextInt rng 40)))
        put-main   {:op :put-list
                    :k  k-main
                    :vs (random-distinct-longs rng (+ 2 (.nextInt rng 4)) 320)}
        model1     (apply-list-iter-op model put-main)
        del-main   {:op :del-list
                    :k  k-main
                    :vs (miss-heavy-del-list-values rng (get model1 k-main))}
        k-side     (long (.nextInt rng 40))
        side-op    (if (< (.nextInt rng 100) 57)
                     {:op :put-list
                      :k  k-side
                      :vs (random-distinct-longs rng
                                                 (inc (.nextInt rng 4))
                                                 320)}
                     {:op :del-list
                      :k  k-side
                      :vs (miss-heavy-del-list-values rng
                                                      (get model1 k-side))})
        model2     (apply-list-iter-ops model [put-main del-main side-op])
        extra-del  (when (< (.nextInt rng 100) 35)
                     {:op :del-list
                      :k  k-main
                      :vs (miss-heavy-del-list-values rng (get model2 k-main))})
        maybe-wipe (when (< (.nextInt rng 100) 16)
                     {:op :wipe
                      :k  (if (< (.nextInt rng 100) 75)
                            k-main
                            (long (.nextInt rng 40)))})]
    (->> [put-main del-main side-op extra-del maybe-wipe]
         (remove nil?)
         vec)))

(defn- flatten-list-ref
  ([m]
   (flatten-list-ref m false false))
  ([m key-back? val-back?]
   (vec (let [key-seq (if key-back?
                        (when (seq m) (rseq m))
                        (seq m))]
          (for [[k vs] key-seq
                v      (if val-back? (rseq vs) (seq vs))]
            [k v])))))

(defn- verify-list-iter-model!
  [lmdb dbi ref-map]
  (is (= (flatten-list-ref ref-map)
         (vec (i/list-range lmdb dbi [:all] :long [:all] :long))))
  (is (= (flatten-list-ref ref-map true false)
         (vec (i/list-range lmdb dbi [:all-back] :long [:all] :long))))
  (is (= (flatten-list-ref ref-map false true)
         (vec (i/list-range lmdb dbi [:all] :long [:all-back] :long))))
  (is (= (reduce + 0 (map count (vals ref-map)))
         (i/list-range-count lmdb dbi [:all] :long [:all] :long)))
  (let [visited (volatile! [])]
    (i/visit-list-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                        [:all] :long [:all] :long false)
    (is (= (flatten-list-ref ref-map) @visited)))
  (when (seq ref-map)
    (let [[k vs] (first ref-map)
          v-lo   (first vs)
          v-hi   (last vs)]
      (is (= (vec (for [v (subseq vs >= v-lo <= v-hi)] [k v]))
             (vec (i/list-range lmdb dbi [:closed k k] :long
                                [:closed v-lo v-hi] :long)))))
    (when-let [[k vs] (first (filter (fn [[_ s]] (>= (count s) 3)) ref-map))]
      (let [mid (nth (vec vs) (quot (count vs) 2))]
        (is (= (vec (for [v (subseq vs >= mid)] [k v]))
               (vec (i/list-range lmdb dbi [:closed k k] :long
                                  [:at-least mid] :long))))
        (is (= (vec (for [v (subseq vs <= mid)] [k v]))
               (vec (i/list-range lmdb dbi [:closed k k] :long
                                  [:at-most mid] :long))))
        (is (= (vec (for [v (subseq vs > mid)] [k v]))
               (vec (i/list-range lmdb dbi [:closed k k] :long
                                  [:greater-than mid] :long))))
        (is (= (vec (for [v (subseq vs < mid)] [k v]))
               (vec (i/list-range lmdb dbi [:closed k k] :long
                                  [:less-than mid] :long))))))))

(defn- verify-list-key-range-model!
  [lmdb dbi ref-map]
  (let [visited (volatile! [])]
    (i/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                            [:all] :long :long false)
    (is (= (flatten-list-ref ref-map) @visited)))
  (let [visited (volatile! [])]
    (i/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                            [:all-back] :long :long false)
    (is (= (flatten-list-ref ref-map true false) @visited)))
  (when (>= (count ref-map) 2)
    (let [lo      (first (keys ref-map))
          hi      (last (keys ref-map))
          sub     (into (sorted-map) (subseq ref-map >= lo <= hi))
          visited (volatile! [])]
      (i/visit-list-key-range lmdb dbi (fn [k v] (vswap! visited conj [k v]))
                              [:closed lo hi] :long :long false)
      (is (= (flatten-list-ref sub) @visited)))))

(defn- apply-fuzz-list-row
  [model row]
  (let [[op dbi k v] row
        k (long (or k 0))]
    (if (= "fuzzl" dbi)
      (case op
        :put-list
        (update model k (fn [s] (into (or s (sorted-set)) (map long (or v [])))))

        :del-list
        (if-let [s (get model k)]
          (let [new-s (reduce disj s (map long (or v [])))]
            (if (empty? new-s) (dissoc model k)
                (assoc model k new-s)))
          model)

        :del
        (dissoc model k)

        model)
      model)))

(defn- apply-fuzz-list-rows
  [model rows]
  (reduce apply-fuzz-list-row model rows))

(defn- replay-fuzz-list-model
  [records]
  (reduce (fn [model {:keys [ops]}]
            (apply-fuzz-list-rows model ops))
          (sorted-map)
          records))

(defn- assert-list-txlog-replay!
  [lmdb model]
  (let [records (vec (i/open-tx-log lmdb 0))
        lsns    (mapv (comp long :lsn) records)]
    (is (= model (replay-fuzz-list-model records)))
    (is (:ok? (i/verify-commit-marker! lmdb)))
    (when (seq lsns)
      (is (contiguous-lsns? lsns))
      (is (apply < lsns)))))

(deftest txn-log-fuzz-key-iter-consistency-test
  (doseq [seed [37 73]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 112
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 320)
                (do
                  (verify-key-iter-model! @lmdb-v "fuzz" model)
                  (assert-open-txlog-range-and-replay! @lmdb-v model rng))
                (let [op   (.nextInt rng 100)
                      lmdb @lmdb-v]
                  (cond
                    (< op 68)
                    (let [ops  (random-key-iter-ops rng)
                          rows (key-iter-ops->rows ops)]
                      (i/transact-kv lmdb rows)
                      (recur (inc step) (apply-key-iter-ops model ops)))

                    (< op 78)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))

                    (< op 92)
                    (do
                      (verify-key-iter-model! lmdb "fuzz" model)
                      (assert-open-txlog-range-and-replay! lmdb model rng)
                      (recur (inc step) model))

                    :else
                    (do
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-lmdb dir opts))
                      (verify-key-iter-model! @lmdb-v "fuzz" model)
                      (assert-open-txlog-range-and-replay! @lmdb-v model rng)
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-list-iter-consistency-test
  (doseq [seed [43 89]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 112
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
            (loop [step  (long 0)
                   model (sorted-map)]
              (if (= step 280)
                (do
                  (verify-list-iter-model! @lmdb-v "fuzzl" model)
                  (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                  (assert-list-txlog-replay! @lmdb-v model))
                (let [op   (.nextInt rng 100)
                      lmdb @lmdb-v]
                  (cond
                    (< op 66)
                    (let [ops  (random-list-iter-ops rng)
                          rows (list-iter-ops->rows ops)]
                      (i/transact-kv lmdb rows)
                      (recur (inc step) (apply-list-iter-ops model ops)))

                    (< op 76)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model))

                    (< op 92)
                    (do
                      (verify-list-iter-model! lmdb "fuzzl" model)
                      (verify-list-key-range-model! lmdb "fuzzl" model)
                      (assert-list-txlog-replay! lmdb model)
                      (recur (inc step) model))

                    :else
                    (do
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
                      (verify-list-iter-model! @lmdb-v "fuzzl" model)
                      (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                      (assert-list-txlog-replay! @lmdb-v model)
                      (recur (inc step) model))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-list-miss-heavy-del-batch-test
  (doseq [seed [131 197 281]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 104
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
            (loop [step              (long 0)
                   model             (sorted-map)
                   mixed-del-batches (long 0)]
              (if (= step 240)
                (do
                  (verify-list-iter-model! @lmdb-v "fuzzl" model)
                  (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                  (assert-list-txlog-replay! @lmdb-v model)
                  (is (> mixed-del-batches 20)))
                (let [op-roll (.nextInt rng 100)
                      lmdb    @lmdb-v]
                  (cond
                    (< op-roll 72)
                    (let [ops                 [(random-miss-heavy-list-op rng
                                                                          model)]
                          rows                (list-iter-ops->rows ops)
                          [next-model mixed]  (apply-list-iter-ops+mixed-del-count
                                               model
                                               ops)]
                      (i/transact-kv lmdb rows)
                      (recur (inc step)
                             next-model
                             (long (+ mixed-del-batches mixed))))

                    (< op-roll 80)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model mixed-del-batches))

                    (< op-roll 94)
                    (do
                      (verify-list-iter-model! lmdb "fuzzl" model)
                      (verify-list-key-range-model! lmdb "fuzzl" model)
                      (assert-list-txlog-replay! lmdb model)
                      (recur (inc step) model mixed-del-batches))

                    :else
                    (do
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
                      (verify-list-iter-model! @lmdb-v "fuzzl" model)
                      (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                      (assert-list-txlog-replay! @lmdb-v model)
                      (recur (inc step) model mixed-del-batches))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest txn-log-fuzz-list-churn-mixed-transaction-test
  (doseq [seed [151 239]]
    (testing (str "seed=" seed)
      (with-temp-dir [dir]
        (let [opts {:flags                     (conj c/default-env-flags
                                                    :nosync)
                    :wal?                  true
                    :wal-group-commit      1
                    :wal-segment-max-bytes 104
                    :wal-segment-max-ms    600000}
              ^Random rng (Random. (long seed))
              lmdb-v      (volatile! nil)]
          (try
            (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
            (loop [step              (long 0)
                   model             (sorted-map)
                   mixed-del-batches (long 0)]
              (if (= step 210)
                (do
                  (verify-list-iter-model! @lmdb-v "fuzzl" model)
                  (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                  (assert-list-txlog-replay! @lmdb-v model)
                  (is (> mixed-del-batches 45)))
                (let [op-roll (.nextInt rng 100)
                      lmdb    @lmdb-v]
                  (cond
                    (< op-roll 70)
                    (let [ops                 (random-list-churn-ops rng model)
                          rows                (list-iter-ops->rows ops)
                          [next-model mixed]  (apply-list-iter-ops+mixed-del-count
                                               model
                                               ops)]
                      (i/transact-kv lmdb rows)
                      (recur (inc step)
                             next-model
                             (long (+ mixed-del-batches mixed))))

                    (< op-roll 79)
                    (do
                      (i/force-txlog-sync! lmdb)
                      (recur (inc step) model mixed-del-batches))

                    (< op-roll 94)
                    (do
                      (verify-list-iter-model! lmdb "fuzzl" model)
                      (verify-list-key-range-model! lmdb "fuzzl" model)
                      (assert-list-txlog-replay! lmdb model)
                      (recur (inc step) model mixed-del-batches))

                    :else
                    (do
                      (i/close-kv lmdb)
                      (vreset! lmdb-v nil)
                      (vreset! lmdb-v (open-fuzz-list-lmdb dir opts))
                      (verify-list-iter-model! @lmdb-v "fuzzl" model)
                      (verify-list-key-range-model! @lmdb-v "fuzzl" model)
                      (assert-list-txlog-replay! @lmdb-v model)
                      (recur (inc step) model mixed-del-batches))))))
            (finally
              (when-let [lmdb @lmdb-v]
                (i/close-kv lmdb)))))))))

(deftest classify-record-kind-test
  (testing "vector checkpoint records classify via vec-index/vec-meta DBIs"
    (is (= :vector-checkpoint
           (sut/classify-record-kind
            [[:put c/vec-index-dbi ["d" 0] (byte-array 0) :data :bytes]
             [:put c/vec-meta-dbi "d" {:current-snapshot-lsn 10}
              :string :data]]))))
  (testing "mixed or non-vector ops classify as user"
    (is (= :user
           (sut/classify-record-kind
            [[:put c/vec-meta-dbi "d" {:vec-replay-floor-lsn 5}
              :string :data]
             [:put "d/vec-refs" :ref 1 :data :id]]))))
  (testing "missing/invalid ops classify as unknown"
    (is (= :unknown (sut/classify-record-kind nil)))
    (is (= :unknown (sut/classify-record-kind [])))
    (is (= :unknown (sut/classify-record-kind {:ops []})))))

(deftest record-codec-roundtrip-test
  (let [body (->bytes "hello txlog")
        data (sut/encode-record body {:compressed? true})
        rec  (sut/decode-record-bytes data)]
    (is (= (alength ^bytes body) (:body-len rec)))
    (is (true? (:compressed? rec)))
    (is (= (seq body) (seq ^bytes (:body rec))))))

(deftest append-record-at-roundtrip-test
  (with-temp-dir [dir]
    (let [path (sut/segment-path dir 1)
          body (->bytes "hello append")]
      (with-open [^FileChannel ch (sut/open-segment-channel path)]
        (let [{:keys [offset size checksum]}
              (sut/append-record-at! ch 0 body {:compressed? true})
              {:keys [records partial-tail? valid-end]}
              (sut/scan-segment path)
              rec (first records)]
          (is (= 0 offset))
          (is (= (+ sut/record-header-size (alength ^bytes body)) size))
          (is (= valid-end size))
          (is (false? partial-tail?))
          (is (= checksum (:checksum rec)))
          (is (true? (:compressed? rec)))
          (is (= (seq body) (seq ^bytes (:body rec)))))))))

(deftest commit-payload-parallel-row-encoding-equivalence-test
  (let [rows (vec (for [i (range 128)]
                    [:put "dbi" i (str "v" i)]))
        lsn  42
        tx-time 99
        mode-var #'sut/use-parallel-row-encoding?
        original-mode (var-get mode-var)]
    (try
      (alter-var-root mode-var (constantly (fn [_] false)))
      (let [sequential (sut/encode-commit-row-payload lsn tx-time rows)]
        (alter-var-root mode-var (constantly (fn [_] true)))
        (let [parallel (sut/encode-commit-row-payload lsn tx-time rows)]
          (is (= (seq sequential) (seq parallel)))))
      (finally
        (alter-var-root mode-var (constantly original-mode))))))

(deftest commit-payload-lmdb-row-reuse-test
  (let [rows [(l/kv-tx :put "dbi" :k :v :keyword :keyword)
              (l/kv-tx :del "dbi" :k2 nil :keyword)
              (l/kv-tx :put-list "dbi" :lk [1 2] :keyword :long)]
        {:keys [body lmdb-rows]}
        (#'sut/encode-commit-row-payload+lmdb-rows 7 11 rows)
        decoded (:ops (sut/decode-commit-row-payload body))
        ^datalevin.lmdb.KVTxData put-row  (nth lmdb-rows 0)
        ^datalevin.lmdb.KVTxData del-row  (nth lmdb-rows 1)
        ^datalevin.lmdb.KVTxData list-row (nth lmdb-rows 2)]
    (is (= [[:put "dbi" :k :v :keyword :keyword]
            [:del "dbi" :k2 :keyword]
            [:put-list "dbi" :lk [1 2] :keyword :long]]
           decoded))
    (is (= 3 (count lmdb-rows)))

    (is (= :raw (.-kt put-row)))
    (is (= :raw (.-vt put-row)))
    (is (= :k (b/read-buffer (ByteBuffer/wrap ^bytes (.-k put-row)) :keyword)))
    (is (= :v (b/read-buffer (ByteBuffer/wrap ^bytes (.-v put-row)) :keyword)))

    (is (= :raw (.-kt del-row)))
    (is (= :k2 (b/read-buffer (ByteBuffer/wrap ^bytes (.-k del-row)) :keyword)))

    (is (= :raw (.-kt list-row)))
    (is (= :long (.-vt list-row)))
    (is (= :lk (b/read-buffer (ByteBuffer/wrap ^bytes (.-k list-row)) :keyword)))
    (is (= [1 2] (.-v list-row)))))

(deftest txlog-apply-after-append-retries-resized-test
  (let [state {:fatal-error (volatile! nil)}
        calls (atom 0)]
    (with-redefs [i/transact-kv
                  (fn [& _]
                    (if (= 1 (swap! calls inc))
                      (throw (ex-info "DB resized" {:resized true}))
                      :transacted))]
      (is (= :transacted
             (#'kv/apply-lmdb-after-txlog-append!
              :fake-lmdb state [[:put "dbi" :k :v]])))
      (is (= 2 @calls))
      (is (nil? @(:fatal-error state))))))

(deftest txlog-apply-after-append-failure-marks-fatal-test
  (let [state {:fatal-error (volatile! nil)}]
    (with-redefs [i/transact-kv
                  (fn [& _]
                    (throw (ex-info "apply failed" {})))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (#'kv/apply-lmdb-after-txlog-append!
                    :fake-lmdb state [[:put "dbi" :k :v]])))
      (is (instance? Exception @(:fatal-error state)))
      (is (= "apply failed"
             (.getMessage ^Exception @(:fatal-error state)))))))

(deftest txlog-mark-fatal-skips-resized-test
  (let [state {:fatal-error (volatile! nil)}]
    (#'kv/txlog-mark-fatal! state (ex-info "DB resized" {:resized true}))
    (is (nil? @(:fatal-error state)))
    (#'kv/txlog-mark-fatal! state
                            (ex-info "wrapped"
                                     {}
                                     (ex-info "DB resized" {:resized true})))
    (is (nil? @(:fatal-error state)))))

(deftest retention-state-empty-floors-test
  (let [state (sut/retention-state
               {:segments []
                :total-bytes 0
                :retention-bytes 0
                :retention-ms 0
                :active-segment-id 0
                :newest-segment-id 0
                :floors {}
                :explicit-gc? false})]
    (is (= sut/no-floor-lsn (:required-retained-floor-lsn state)))
    (is (empty? (:floor-limiters state)))
    (is (= [] (:segments state)))))

(deftest segment-summaries-cache-reuses-unchanged-segments-test
  (with-temp-dir [dir]
    (let [seg1      (sut/segment-path dir 1)
          seg2      (sut/segment-path dir 2)
          seg3      (sut/segment-path dir 3)
          append-lsns!
          (fn [path lsns]
            (with-open [^FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                      lsn
                                      (* 1000 lsn)
                                      [[:put "dbi" lsn lsn]])))))
          lsn-calls (atom 0)
          record->lsn
          (fn [record]
            (swap! lsn-calls inc)
            (some-> record
                    :body
                    sut/decode-commit-row-payload
                    :lsn
                    long))
          cache-v   (volatile! {})]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (append-lsns! seg3 [5 6])
      (sut/segment-summaries
        dir
        {:record->lsn record->lsn
         :cache-v     cache-v
         :cache-key   :test-lsn})
      (is (= 6 @lsn-calls))
      (reset! lsn-calls 0)
      (append-lsns! seg3 [7])
      (let [summary (sut/segment-summaries
                      dir
                      {:record->lsn record->lsn
                       :cache-v     cache-v
                       :cache-key   :test-lsn})]
        (is (= 2 @lsn-calls))
        (is (= 7 (-> summary :segments peek :max-lsn)))))))

(deftest segment-summaries-cache-fast-path-skips-metadata-stat-test
  (with-temp-dir [dir]
    (let [seg1      (sut/segment-path dir 1)
          seg2      (sut/segment-path dir 2)
          append-lsns!
          (fn [path lsns]
            (with-open [^FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                      lsn
                                      (* 1000 lsn)
                                      [[:put "dbi" lsn lsn]])))))
          lsn-calls (atom 0)
          record->lsn
          (fn [record]
            (swap! lsn-calls inc)
            (some-> record
                    :body
                    sut/decode-commit-row-payload
                    :lsn
                    long))
          cache-v   (volatile! {})]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (let [active-offset (long (.length (io/file seg2)))]
        (sut/segment-summaries
          dir
          {:record->lsn           record->lsn
           :cache-v               cache-v
           :cache-key             :test-lsn
           :active-segment-id     2
           :active-segment-offset active-offset})
        (is (= 4 @lsn-calls))
        (reset! lsn-calls 0)

        ;; Change only metadata on a closed segment. Cache fast-path should
        ;; still reuse this entry and avoid rescanning.
        (let [f        (io/file seg1)
              touched? (.setLastModified f (+ 1000 (System/currentTimeMillis)))]
          (is touched?)
          (let [summary (sut/segment-summaries
                          dir
                          {:record->lsn           record->lsn
                           :cache-v               cache-v
                           :cache-key             :test-lsn
                           :active-segment-id     2
                           :active-segment-offset active-offset})]
            (is (= 0 @lsn-calls))
            (is (= 2 (count (:segments summary))))
            (is (= 4 (-> summary :segments peek :max-lsn)))))))))

(deftest txlog-records-tail-scan-test
  (with-temp-dir [dir]
    (let [seg1 (sut/segment-path dir 1)
          seg2 (sut/segment-path dir 2)
          seg3 (sut/segment-path dir 3)
          append-lsns!
          (fn [path lsns]
            (with-open [^FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                      lsn
                                      (* 1000 lsn)
                                      [[:put "dbi" lsn lsn]])))))]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (append-lsns! seg3 [5 6])
      (with-open [raf (RandomAccessFile. seg1 "rw")]
        (.seek raf 0)
        (.write raf (int 0x00)))
      (testing "tail scan from high LSN avoids scanning corrupted old segments"
        (is (= [5 6]
               (mapv :lsn (kv/txlog-records {:dir dir} 5))))
        (is (= [3 4 5 6]
               (mapv :lsn (kv/txlog-records {:dir dir} 4)))))
      (testing "full scan still checks historical segments"
        (is (thrown? clojure.lang.ExceptionInfo
                     (kv/txlog-records {:dir dir} 0)))))))

(deftest txlog-records-cache-reuses-unchanged-segments-test
  (with-temp-dir [dir]
    (let [seg1  (sut/segment-path dir 1)
          seg2  (sut/segment-path dir 2)
          state {:dir                 dir
                 :txlog-records-cache (volatile! {})}
          append-lsns!
          (fn [path lsns]
            (with-open [^FileChannel ch
                        (sut/open-segment-channel path)]
              (doseq [lsn lsns]
                (sut/append-record! ch
                                    (sut/encode-commit-row-payload
                                      lsn
                                      (* 1000 lsn)
                                      [[:put "dbi" lsn lsn]])))))]
      (append-lsns! seg1 [1 2])
      (append-lsns! seg2 [3 4])
      (is (= [1 2 3 4]
             (mapv :lsn (kv/txlog-records state 0))))
      (let [cache1       @(:txlog-records-cache state)
            seg1-entry-1 (get cache1 1)
            seg2-entry-1 (get cache1 2)]
        (is (= 2 (count cache1)))

        (is (= [1 2 3 4]
               (mapv :lsn (kv/txlog-records state 0))))
        (let [cache2 @(:txlog-records-cache state)]
          (is (identical? seg1-entry-1 (get cache2 1)))
          (is (identical? seg2-entry-1 (get cache2 2))))

        (append-lsns! seg2 [5])
        (is (= [1 2 3 4 5]
               (mapv :lsn (kv/txlog-records state 0))))
        (let [cache3 @(:txlog-records-cache state)]
          (is (identical? seg1-entry-1 (get cache3 1)))
          (is (not (identical? seg2-entry-1 (get cache3 2))))
          (is (= 5 (-> cache3 (get 2) :records peek :lsn))))))))

(deftest segment-order-and-parse-test
  (with-temp-dir [dir]
    (doseq [f ["segment-0000000000000010.wal"
               "segment-0000000000000002.wal"
               "segment-0000000000000001.wal"
               "segment-0000000000000003.wal.tmp"
               "notes.txt"]]
      (spit (str dir u/+separator+ f) "x"))
    (let [ids (mapv :id (sut/segment-files dir))]
      (is (= [1 2 10] ids)))))

(deftest scan-and-truncate-partial-tail-test
  (with-temp-dir [dir]
    (let [^String path (sut/segment-path dir 1)]
      (with-open [^FileChannel ch (sut/open-segment-channel path)]
        (sut/append-record! ch (->bytes "a"))
        (sut/append-record! ch (->bytes "bb")))
      (Files/write (.toPath (io/file path))
                   (byte-array [(byte 1) (byte 2) (byte 3) (byte 4)])
                   (into-array StandardOpenOption [StandardOpenOption/APPEND]))
      (let [{:keys [records partial-tail? valid-end size]} (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is partial-tail?)
        (is (< valid-end size)))
      (let [{:keys [records truncated? dropped-bytes old-size new-size
                    valid-end partial-tail? preallocated-tail?]}
            (sut/truncate-partial-tail! path)]
        (is (= 2 (count records)))
        (is truncated?)
        (is (pos? dropped-bytes))
        (is (= new-size valid-end))
        (is (= (- old-size new-size) dropped-bytes))
        (is (false? partial-tail?))
        (is (false? preallocated-tail?)))
      (let [{:keys [records partial-tail?]} (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is (false? partial-tail?))))))

(deftest interior-corruption-fails-test
  (with-temp-dir [dir]
    (let [^String path (sut/segment-path dir 2)]
      (with-open [^FileChannel ch (sut/open-segment-channel path)]
        (sut/append-record! ch (->bytes "good-1"))
        (sut/append-record! ch (->bytes "good-2")))
      (with-open [raf (RandomAccessFile. path "rw")]
        (.seek raf sut/record-header-size)
        (.write raf (int 0x7f)))
      (testing "interior corruption is treated as hard corruption"
        (try
          (sut/scan-segment path)
          (is false "Expected corruption exception")
          (catch clojure.lang.ExceptionInfo e
            (is (= :txlog/corrupt (:type (ex-data e))))))))))

(deftest preallocate-and-activate-segment-test
  (with-temp-dir [dir]
    (let [tmp-path   (str dir u/+separator+ "segment-0000000000000009.wal.tmp")
          final-path (sut/segment-path dir 9)]
      (sut/prepare-segment! tmp-path 4096)
      (is (= 4096 (.length (io/file tmp-path))))
      (sut/activate-prepared-segment! tmp-path final-path)
      (is (not (.exists (io/file tmp-path))))
      (is (= 4096 (.length (io/file final-path)))))))

(deftest prepared-segment-list-test
  (with-temp-dir [dir]
    (doseq [f ["segment-0000000000000003.wal.tmp"
               "segment-0000000000000001.wal.tmp"
               "segment-0000000000000010.wal.tmp"
               "segment-0000000000000004.wal"
               "notes.txt"]]
      (spit (str dir u/+separator+ f) "x"))
    (is (= [1 3 10] (mapv :id (sut/prepared-segment-files dir))))))

(deftest preallocated-tail-scan-and-truncate-test
  (with-temp-dir [dir]
    (let [path (sut/segment-path dir 11)]
      (sut/prepare-segment! path 4096)
      (with-open [^FileChannel ch (sut/open-segment-channel path)]
        (let [{:keys [size]} (sut/append-record-at! ch 0 (->bytes "v1"))]
          (sut/append-record-at! ch size (->bytes "v2"))))
      (is (thrown? clojure.lang.ExceptionInfo (sut/scan-segment path)))
      (let [{:keys [records valid-end partial-tail? preallocated-tail? size]}
            (sut/scan-segment path {:allow-preallocated-tail? true})]
        (is (= 2 (count records)))
        (is partial-tail?)
        (is preallocated-tail?)
        (is (< valid-end size)))
      (let [{:keys [records truncated? new-size valid-end partial-tail?
                    preallocated-tail?]}
            (sut/truncate-partial-tail! path {:allow-preallocated-tail? true})]
        (is (= 2 (count records)))
        (is truncated?)
        (is (= new-size (.length (io/file path))))
        (is (= new-size valid-end))
        (is (false? partial-tail?))
        (is (false? preallocated-tail?)))
      (let [{:keys [records partial-tail?]}
            (sut/scan-segment path)]
        (is (= 2 (count records)))
        (is (false? partial-tail?))))))

(deftest next-segment-helpers-test
  (with-temp-dir [dir]
    (let [fallback-path (sut/activate-next-segment! dir 21)]
      (is (= fallback-path (sut/segment-path dir 21)))
      (is (= 0 (.length (io/file fallback-path)))))
    (sut/prepare-next-segment! dir 22 2048)
    (let [tmp-path   (sut/prepared-segment-path dir 22)
          final-path (sut/activate-next-segment! dir 22)]
      (is (not (.exists (io/file tmp-path))))
      (is (= 2048 (.length (io/file final-path)))))))

(deftest init-runtime-state-prepares-next-segment-test
  (with-temp-dir [dir]
    (let [root      (str dir u/+separator+ "native")
          txlog-dir (str root u/+separator+ "txlog")
          {:keys [state]}
          (sut/init-runtime-state
            {:dir                        root
             :wal?                       true
             :wal-segment-prealloc?      true
             :wal-segment-prealloc-mode  :native
             :wal-segment-prealloc-bytes 512}
            {})]
      (try
        (is (= 1 @(:segment-id state)))
        (let [tmp-path (sut/prepared-segment-path txlog-dir 2)]
          (is (.exists (io/file tmp-path)))
          (is (= 512 (.length (io/file tmp-path)))))
        (finally
          (.close ^FileChannel @(:segment-channel state)))))))

(deftest append-durable-rolls-into-preallocated-segment-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (sut/prepare-next-segment! dir 2 2048)
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! 0)
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :relaxed
                   :segment-max-bytes              0
                   :segment-max-ms                 600000
                   :segment-prealloc?              true
                   :segment-prealloc-mode          :native
                   :segment-prealloc-bytes         2048
                   :sync-mode                      :fdatasync
                   :commit-wait-ms                 100
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    1
                                                      :group-commit-ms 0
                                                      :sync-adaptive?  true
                                                      :last-sync-ms    0})}]
        (try
          (let [res (sut/append-durable! state [[:put :k :v]] {})]
            (is (= 2 @(:segment-id state)))
            (is (= 2 (:segment-id res)))
            (is (= 0 (:offset res)))
            (is (pos? @(:segment-offset state)))
            (is (= @(:segment-offset state)
                   (+ (long (:offset res)) (long (:size res)))))
            (is (= 0 (.length (io/file path1))))
            (let [path2 (sut/segment-path dir 2)
                  {:keys [records partial-tail? preallocated-tail?
                          valid-end size]}
                  (sut/scan-segment path2 {:allow-preallocated-tail? true})]
              (is (= 1 (count records)))
              (is partial-tail?)
              (is preallocated-tail?)
              (is (< valid-end size)))
            (let [tmp3 (sut/prepared-segment-path dir 3)]
              (is (.exists (io/file tmp3)))
              (is (= 2048 (.length (io/file tmp3)))))
            (is (= 1 @(:segment-roll-count state)))
            (is (<= 0 @(:segment-roll-duration-ms state)))
            (is (<= 1 @(:segment-prealloc-success-count state)))
            (is (= 0 @(:segment-prealloc-failure-count state))))
          (finally
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-durable-near-roll-latency-metric-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                               dir
                   :segment-id                        (volatile! 1)
                   :segment-created-ms                (volatile! (System/currentTimeMillis))
                   :segment-channel                   (volatile! (sut/open-segment-channel path1))
                   :segment-offset                    (volatile! 9)
                   :append-lock                       (Object.)
                   :next-lsn                          (volatile! 1)
                   :durability-profile                :relaxed
                   :segment-max-bytes                 10
                   :segment-max-ms                    600000
                   :segment-prealloc?                 false
                   :segment-prealloc-mode             :none
                   :segment-prealloc-bytes            0
                   :sync-mode                         :fdatasync
                   :commit-wait-ms                    100
                   :segment-roll-count                (volatile! 0)
                   :segment-roll-duration-ms          (volatile! 0)
                   :segment-prealloc-success-count    (volatile! 0)
                   :segment-prealloc-failure-count    (volatile! 0)
                   :append-near-roll-durations        (volatile! [])
                   :append-near-roll-sorted-durations (volatile! [])
                   :append-p99-near-roll-ms           (volatile! nil)
                   :sync-manager                      (sut/new-sync-manager
                                                        {:group-commit    1
                                                         :group-commit-ms 0
                                                         :sync-adaptive?  true
                                                         :last-sync-ms    0})}]
        (try
          (sut/append-durable! state [[:put :k :v]] {})
          (let [^longs ring  @(:append-near-roll-durations state)
                ^longs stats @(:append-near-roll-sorted-durations state)
                size-idx     (var-get #'sut/append-near-roll-stats-size-idx)]
            (is (instance? (class (long-array 0)) ring))
            (is (instance? (class (long-array 0)) stats))
            (is (= 1 (long (aget stats size-idx)))))
          (is (= 0 @(:segment-roll-count state)))
          (is (number? @(:append-p99-near-roll-ms state)))
          (is (<= 0 (long @(:append-p99-near-roll-ms state))))
          (finally
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-near-roll-p99-sliding-window-test
  (let [max-n (long (var-get #'sut/append-near-roll-sample-max))
        size-idx (var-get #'sut/append-near-roll-stats-size-idx)
        durations (mapv #(mod % 200) (range (+ max-n 64)))
        state {:append-near-roll-durations (volatile! [])
               :append-near-roll-sorted-durations (volatile! [])
               :append-p99-near-roll-ms (volatile! nil)}]
    (doseq [d durations]
      (#'sut/record-append-near-roll-ms! state d))
    (let [^longs stats @(:append-near-roll-sorted-durations state)
          p99 @(:append-p99-near-roll-ms state)
          window (reduce (fn [acc d]
                           (let [acc* (conj acc d)]
                             (if (> (count acc*) max-n)
                               (subvec acc* 1)
                               acc*)))
                         []
                         durations)
          sorted (vec (sort window))
          n (count sorted)
          idx (min (dec n)
                   (int (Math/floor (* 0.99 (dec n)))))
          expected (long (nth sorted idx))]
      (is (= max-n (long (aget stats size-idx))))
      (is (= expected p99)))))

(deftest threadlocal-buffers-shrink-after-large-record-test
  (let [^ThreadLocal commit-tl (var-get #'sut/tl-commit-body-buffer)
        ^ThreadLocal bits-tl   (var-get #'sut/tl-bits-buffer)
        big-cap (* 8 1024 1024)]
    (.set commit-tl (ByteBuffer/allocate big-cap))
    (.set bits-tl (ByteBuffer/allocate big-cap))
    (sut/encode-commit-row-payload
     1
     1
     [[:put "dbi" :k :v]])
    (is (< (.capacity ^ByteBuffer (.get commit-tl)) big-cap))
    (is (< (.capacity ^ByteBuffer (.get bits-tl)) big-cap))))

(deftest threadlocal-buffers-shrink-after-medium-spike-test
  (let [^ThreadLocal commit-tl (var-get #'sut/tl-commit-body-buffer)
        ^ThreadLocal bits-tl   (var-get #'sut/tl-bits-buffer)
        medium-cap (* 2 1024 1024)]
    (.set commit-tl (ByteBuffer/allocate medium-cap))
    (.set bits-tl (ByteBuffer/allocate medium-cap))
    (sut/encode-commit-row-payload
     1
     1
     [[:put "dbi" :k :v]])
    (is (< (.capacity ^ByteBuffer (.get commit-tl)) medium-cap))
    (is (< (.capacity ^ByteBuffer (.get bits-tl)) medium-cap))))

(deftest append-durable-trailing-sync-batch-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! (System/currentTimeMillis))
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :relaxed
                   :segment-max-bytes              1024
                   :segment-max-ms                 600000
                   :segment-prealloc?              false
                   :segment-prealloc-mode          :none
                   :segment-prealloc-bytes         0
                   :sync-mode                      :none
                   :commit-wait-ms                 100
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    3
                                                      :group-commit-ms 100000
                                                      :sync-adaptive?  false
                                                      :last-sync-ms
                                                      (System/currentTimeMillis)})}]
        (try
          (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
          (let [s1 (sut/sync-manager-state (:sync-manager state))]
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 0 (:last-durable-lsn s1)))
            (is (= 1 (:pending-count s1)))
            (is (= 0 (:batched-sync-count s1))))
          (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
          (let [s2 (sut/sync-manager-state (:sync-manager state))]
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 0 (:last-durable-lsn s2)))
            (is (= 2 (:pending-count s2)))
            (is (= 0 (:batched-sync-count s2))))
          (sut/append-durable! state [[:put "dbi" :k3 :v3]] {})
          (let [s3 (sut/sync-manager-state (:sync-manager state))]
            (is (= 3 (:last-appended-lsn s3)))
            (is (= 3 (:last-durable-lsn s3)))
            (is (= 0 (:pending-count s3)))
            (is (= 1 (:batched-sync-count s3)))
            (is (= 1 (get-in s3 [:sync-count-by-reason :batch-count]))))
          (finally
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-durable-relaxed-group-commit-one-durable-per-append-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! (System/currentTimeMillis))
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :relaxed
                   :segment-max-bytes              1024
                   :segment-max-ms                 600000
                   :segment-prealloc?              false
                   :segment-prealloc-mode          :none
                   :segment-prealloc-bytes         0
                   :sync-mode                      :fdatasync
                   :commit-wait-ms                 100
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    1
                                                      :group-commit-ms 100000
                                                      :sync-adaptive?  false
                                                      :last-sync-ms
                                                      (System/currentTimeMillis)})}]
        (try
          (let [r1 (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
                s1 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r1)))
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 1 (:last-durable-lsn s1)))
            (is (= 0 (:pending-count s1)))
            (is (= 1 (get-in s1 [:sync-count-by-reason :batch-count]))))
          (let [r2 (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
                s2 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r2)))
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 2 (:last-durable-lsn s2)))
            (is (= 0 (:pending-count s2)))
            (is (= 2 (get-in s2 [:sync-count-by-reason :batch-count]))))
          (finally
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-durable-relaxed-releases-append-lock-before-sync-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! (System/currentTimeMillis))
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :relaxed
                   :segment-max-bytes              1024
                   :segment-max-ms                 600000
                   :segment-prealloc?              false
                   :segment-prealloc-mode          :none
                   :segment-prealloc-bytes         0
                   :sync-mode                      :invalid
                   :commit-wait-ms                 100
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    1
                                                      :group-commit-ms 100000
                                                      :sync-adaptive?  false
                                                      :last-sync-ms
                                                      (System/currentTimeMillis)})}
            fatal-entered (promise)
            allow-fatal   (promise)]
        (try
          (let [f1 (future
                     (try
                       (sut/append-durable! state
                                            [[:put "dbi" :k1 :v1]]
                                            {:mark-fatal!
                                             (fn [_ _]
                                               (deliver fatal-entered true)
                                               @allow-fatal)})
                       :ok
                       (catch Exception _
                         :failed)))]
            (is (true? (deref fatal-entered 2000 false)))
            (let [f2 (future
                       (try
                         (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
                         :ok
                         (catch Exception _
                           :failed)))
                  r2 (deref f2 3000 ::timeout)]
              (is (not= ::timeout r2))
              (is (= :failed r2))
              (is (= 3 (long @(:next-lsn state))))
              (is (= ::timeout (deref f1 100 ::timeout)))
              (deliver allow-fatal true)
              (is (= :failed (deref f1 2000 ::timeout)))))
          (finally
            (deliver allow-fatal true)
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-durable-relaxed-releases-append-lock-before-sync-request-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! (System/currentTimeMillis))
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :relaxed
                   :segment-max-bytes              1024
                   :segment-max-ms                 600000
                   :segment-prealloc?              false
                   :segment-prealloc-mode          :none
                   :segment-prealloc-bytes         0
                   :sync-mode                      :none
                   :commit-wait-ms                 100
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    1000
                                                      :group-commit-ms 100000
                                                      :sync-adaptive?  false
                                                      :last-sync-ms
                                                      (System/currentTimeMillis)})}
            request-entered (promise)
            allow-request   (promise)
            calls           (atom 0)]
        (try
          (with-redefs [sut/request-sync-on-append!
                        (fn [_ _ _]
                          (if (= 1 (swap! calls inc))
                            (do
                              (deliver request-entered true)
                              @allow-request
                              nil)
                            nil))]
            (let [f1 (future
                       (sut/append-durable! state [[:put "dbi" :k1 :v1]] {}))]
              (is (true? (deref request-entered 2000 false)))
              (let [f2 (future
                         (sut/append-durable! state [[:put "dbi" :k2 :v2]] {}))
                    r2 (deref f2 2000 ::timeout)]
                (is (not= ::timeout r2))
                (is (= 3 (long @(:next-lsn state))))
                (is (= ::timeout (deref f1 100 ::timeout)))
                (deliver allow-request true)
                (is (not= ::timeout (deref f1 2000 ::timeout))))))
          (finally
            (deliver allow-request true)
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest append-durable-strict-single-writer-durable-per-append-test
  (with-temp-dir [dir]
    (let [path1 (sut/segment-path dir 1)]
      (with-open [^FileChannel _ (sut/open-segment-channel path1)])
      (let [state {:dir                            dir
                   :segment-id                     (volatile! 1)
                   :segment-created-ms             (volatile! (System/currentTimeMillis))
                   :segment-channel                (volatile! (sut/open-segment-channel path1))
                   :segment-offset                 (volatile! 0)
                   :append-lock                    (Object.)
                   :next-lsn                       (volatile! 1)
                   :durability-profile             :strict
                   :segment-max-bytes              1024
                   :segment-max-ms                 600000
                   :segment-prealloc?              false
                   :segment-prealloc-mode          :none
                   :segment-prealloc-bytes         0
                   :sync-mode                      :none
                   :commit-wait-ms                 1000
                   :segment-roll-count             (volatile! 0)
                   :segment-roll-duration-ms       (volatile! 0)
                   :segment-prealloc-success-count (volatile! 0)
                   :segment-prealloc-failure-count (volatile! 0)
                   :append-near-roll-durations     (volatile! [])
                   :append-p99-near-roll-ms        (volatile! nil)
                   :sync-manager                   (sut/new-sync-manager
                                                     {:group-commit    1000
                                                      :group-commit-ms 100000
                                                      :sync-adaptive?  false
                                                      :last-sync-ms
                                                      (System/currentTimeMillis)})}]
        (try
          (let [r1 (sut/append-durable! state [[:put "dbi" :k1 :v1]] {})
                s1 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r1)))
            (is (= 1 (:last-appended-lsn s1)))
            (is (= 1 (:last-durable-lsn s1)))
            (is (= 0 (:pending-count s1)))
            (is (= 1 (:forced-sync-count s1))))
          (let [r2 (sut/append-durable! state [[:put "dbi" :k2 :v2]] {})
                s2 (sut/sync-manager-state (:sync-manager state))]
            (is (true? (:synced? r2)))
            (is (= 2 (:last-appended-lsn s2)))
            (is (= 2 (:last-durable-lsn s2)))
            (is (= 0 (:pending-count s2)))
            (is (= 2 (:forced-sync-count s2))))
          (finally
            (.close ^FileChannel @(:segment-channel state))))))))

(deftest sync-manager-trailing-target-batches-multiple-enqueued-lsns-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})
        now (System/currentTimeMillis)]
    (is (nil? (sut/request-sync-on-append! mgr 1 now)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))
    (doseq [lsn (range 2 9)]
      (is (nil? (sut/request-sync-on-append! mgr lsn now))))
    ;; A second leader is blocked while the first fsync is in-flight.
    (is (nil? (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 1 now :forced)
    ;; Trailing sync should pick up the queue tail in one shot.
    (is (= {:target-lsn 8 :reason :forced}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 8 now :forced)
    (let [s (sut/sync-manager-state mgr)]
      (is (= 8 (:last-durable-lsn s)))
      (is (= 0 (:pending-count s)))
      (is (= 2 (:forced-sync-count s))))))

(deftest sync-manager-begin-sync-for-lsn-allows-trailing-target-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms
                                   (System/currentTimeMillis)})
        now (System/currentTimeMillis)]
    (is (nil? (sut/request-sync-on-append! mgr 1 now)))
    (is (nil? (sut/begin-sync! mgr 2)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 now)))
    (is (nil? (sut/begin-sync! mgr 2)))
    (sut/complete-sync-success! mgr 1 now :forced)
    (is (= {:target-lsn 2 :reason :forced}
           (sut/begin-sync! mgr 1)))))

(deftest segment-roll-predicate-test
  (is (false? (sut/should-roll-segment? 10 1000 1010
                                        {:segment-max-bytes 100
                                         :segment-max-ms 1000})))
  (is (true? (sut/should-roll-segment? 100 1000 1010
                                       {:segment-max-bytes 100
                                        :segment-max-ms 1000})))
  (is (true? (sut/should-roll-segment? 10 1000 3001
                                       {:segment-max-bytes 1000
                                        :segment-max-ms 2000}))))

(deftest segment-summaries-test
  (with-temp-dir [dir]
    (let [path1      (sut/segment-path dir 1)
          path2      (sut/segment-path dir 2)
          decode-lsn (fn [{:keys [body]}]
                       (long (aget ^bytes body 0)))
          marker-offset
          (with-open [^FileChannel ch1 (sut/open-segment-channel path1)
                      ^FileChannel ch2 (sut/open-segment-channel path2)]
            (sut/append-record! ch1 (byte-array [(byte 1)]))
            (let [{:keys [offset]} (sut/append-record! ch1 (byte-array [(byte 2)]))]
              (sut/append-record! ch2 (byte-array [(byte 3)]))
              offset))
          {:keys [segments marker-record min-retained-lsn newest-segment-id]}
          (sut/segment-summaries dir
                                 {:record->lsn           decode-lsn
                                  :marker-segment-id     1
                                  :marker-offset         marker-offset
                                  :min-retained-fallback 999})]
      (is (= 2 (count segments)))
      (is (= 1 min-retained-lsn))
      (is (= 2 newest-segment-id))
      (is (= 2 (:lsn marker-record)))
      (is (= 1 (:segment-id marker-record)))))
  (with-temp-dir [dir]
    (is (= 77
           (:min-retained-lsn
            (sut/segment-summaries dir
                                   {:min-retained-fallback 77}))))))

(deftest meta-slot-codec-test
  (let [m {:revision           7
           :last-committed-lsn 100
           :last-durable-lsn   99
           :last-applied-lsn   98
           :segment-id         3
           :segment-offset     4096
           :updated-ms         12345}
        encoded (sut/encode-meta-slot m)
        decoded (sut/decode-meta-slot-bytes encoded)]
    (is (= 7 (:revision decoded)))
    (is (= 100 (:last-committed-lsn decoded)))
    (is (= 99 (:last-durable-lsn decoded)))
    (is (= 98 (:last-applied-lsn decoded)))
    (is (= 3 (:segment-id decoded)))
    (is (= 4096 (:segment-offset decoded)))
    (is (= 12345 (:updated-ms decoded)))))

(deftest meta-file-read-write-and-fallback-test
  (with-temp-dir [dir]
    (let [path (sut/meta-path dir)]
      (sut/write-meta-file! path {:last-committed-lsn 10
                                  :last-durable-lsn 9
                                  :last-applied-lsn 9
                                  :segment-id 1
                                  :segment-offset 100
                                  :updated-ms 1000})
      (sut/write-meta-file! path {:last-committed-lsn 20
                                  :last-durable-lsn 20
                                  :last-applied-lsn 19
                                  :segment-id 2
                                  :segment-offset 200
                                  :updated-ms 2000})
      (let [{:keys [current]} (sut/read-meta-file path)]
        (is (= 1 (:revision current)))
        (is (= 20 (:last-committed-lsn current))))
      ;; Corrupt slot-b payload. Reader should fall back to slot-a.
      (with-open [raf (RandomAccessFile. path "rw")]
        (.seek raf (+ sut/meta-slot-size 8))
        (.write raf (int 0x7f)))
      (let [{:keys [current slot-a slot-b]} (sut/read-meta-file path)]
        (is (some? slot-a))
        (is (nil? slot-b))
        (is (= 0 (:revision current)))
        (is (= 10 (:last-committed-lsn current)))))))

(deftest maybe-publish-meta-best-effort-threshold-test
  (with-temp-dir [dir]
    (let [path (sut/meta-path dir)
          mgr  (sut/new-sync-manager {:last-durable-lsn 2
                                      :last-appended-lsn 2
                                      :group-commit 100
                                      :group-commit-ms 100
                                      :last-sync-ms (System/currentTimeMillis)})
          state {:meta-path path
                 :sync-mode :none
                 :sync-manager mgr
                 :meta-publish-lock (Object.)
                 :meta-flush-max-txs 2
                 :meta-flush-max-ms 100000
                 :meta-last-flush-ms (volatile! (System/currentTimeMillis))
                 :meta-pending-txs (volatile! 0)}
          append-1 {:lsn 1 :segment-id 1 :offset 10}
          append-2 {:lsn 2 :segment-id 1 :offset 20}]
      (sut/maybe-publish-meta-best-effort! state append-1)
      (is (false? (.exists (io/file path))))
      (is (= 1 @(:meta-pending-txs state)))
      (sut/maybe-publish-meta-best-effort! state append-2)
      (is (.exists (io/file path)))
      (is (= 0 @(:meta-pending-txs state)))
      (let [{:keys [current]} (sut/read-meta-file path)]
        (is (= 2 (:last-committed-lsn current)))
        (is (= 2 (:last-durable-lsn current)))))))

(deftest maybe-publish-meta-best-effort-releases-lock-before-publish-test
  (let [state {:meta-publish-lock (Object.)
               :meta-flush-max-txs 1
               :meta-flush-max-ms 100000
               :meta-last-flush-ms (volatile! (System/currentTimeMillis))
               :meta-pending-txs (volatile! 0)}
        append-res {:lsn 1 :segment-id 1 :offset 10}
        first-entered (promise)
        second-entered (promise)
        allow-first (promise)
        calls (atom 0)
        publish!
        (fn [_ _]
          (if (= 1 (swap! calls inc))
            (do
              (deliver first-entered true)
              @allow-first
              nil)
            (do
              (deliver second-entered true)
              nil)))]
    (let [f1 (future (sut/maybe-publish-meta-best-effort! state append-res publish!))]
      (is (true? (deref first-entered 2000 false)))
      (let [f2 (future (sut/maybe-publish-meta-best-effort! state append-res publish!))
            r2 (deref f2 2000 ::timeout)]
        (is (true? (deref second-entered 2000 false)))
        (is (not= ::timeout r2))
        (is (= ::timeout (deref f1 100 ::timeout)))
        (deliver allow-first true)
        (is (not= ::timeout (deref f1 2000 ::timeout)))
        (is (= 2 @calls))))))

(deftest sync-manager-request-and-complete-test
  (let [mgr (sut/new-sync-manager {:group-commit 1
                                   :group-commit-ms 100
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 1 10)))
    (is (nil? (sut/request-sync-on-append! mgr 2 11)))
    (is (= {:target-lsn 2 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 12)
    (let [state0 (sut/sync-manager-state mgr)]
      (is (= 2 (:last-durable-lsn state0)))
      (is (= 1 (:batched-sync-count state0)))
      (is (= 1 (get-in state0 [:sync-count-by-reason :batch-count]))))
    (sut/record-commit-wait-ms! mgr 7 :batch-count)
    (let [state1 (sut/sync-manager-state mgr)]
      (is (= 7.0 (:avg-commit-wait-ms state1)))
      (is (= 7.0 (get-in state1 [:avg-commit-wait-ms-by-mode :batched]))))
    (is (:durable? (sut/await-durable-lsn! mgr 2 100 12)))))

(deftest sync-manager-request-sync-now-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 100000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:request? true :reason :forced}
           (sut/request-sync-now! mgr)))
    (is (nil? (sut/request-sync-now! mgr)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))))

(deftest sync-manager-batch-trigger-test
  (let [mgr (sut/new-sync-manager {:group-commit 3
                                   :group-commit-ms 100
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 2)))
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 3 3)))
    (is (= {:target-lsn 3 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 3 4)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-count])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 5
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-if-needed! mgr 10)))
    (is (= {:target-lsn 1 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 1 11)
    (sut/record-commit-wait-ms! mgr 12 :batch-time)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 1 (:batched-sync-count state)))
      (is (= 12.0 (:avg-commit-wait-ms state)))
      (is (= 12.0 (get-in state [:avg-commit-wait-ms-by-mode :batched]))))))

(deftest sync-manager-n-or-t-only-test
  (let [mgr (sut/new-sync-manager {:group-commit 3
                                   :group-commit-ms 10
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (nil? (sut/request-sync-on-append! mgr 2 5)))
    ;; No adaptive special-case behavior: third append triggers by N only.
    (is (= {:request? true :reason :batch-count}
           (sut/request-sync-on-append! mgr 3 9)))
    (is (= {:target-lsn 3 :reason :batch-count}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 3 10)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-count])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 10
                                   :sync-adaptive? true
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    ;; Trigger by T only.
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-on-append! mgr 2 15)))
    (is (= {:target-lsn 2 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 16)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-time])))))
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 10
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    ;; adaptive knobs do not alter N/T behavior.
    (is (= {:request? true :reason :batch-time}
           (sut/request-sync-on-append! mgr 2 15)))
    (is (= {:target-lsn 2 :reason :batch-time}
           (sut/begin-sync! mgr)))
    (sut/complete-sync-success! mgr 2 16)
    (let [state (sut/sync-manager-state mgr)]
      (is (= 0 (:pending-count state)))
      (is (= 1 (:batched-sync-count state)))
      (is (= 1 (get-in state [:sync-count-by-reason :batch-time]))))))

(deftest sync-manager-trailing-request-survives-inflight-sync-test
  (let [mgr (sut/new-sync-manager {:group-commit 1000
                                   :group-commit-ms 1000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (is (nil? (sut/request-sync-on-append! mgr 1 1)))
    (is (= {:target-lsn 1 :reason :forced}
           (sut/begin-sync! mgr)))
    (is (nil? (sut/request-sync-on-append! mgr 2 2)))
    (sut/complete-sync-success! mgr 1 3 :forced)
    (let [s (sut/sync-manager-state mgr)]
      (is (= 1 (:last-durable-lsn s)))
      (is (= 2 (:last-appended-lsn s))))
    (is (= {:target-lsn 2 :reason :forced}
           (sut/begin-sync! mgr)))))

(deftest sync-manager-begin-sync-clears-stale-request-test
  (let [mgr (sut/new-sync-manager {:last-durable-lsn 2
                                   :last-appended-lsn 2
                                   :group-commit 1000
                                   :group-commit-ms 1000
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (vreset! (:sync-requested? mgr) true)
    (vreset! (:sync-request-reason mgr) :batch-time)
    (is (nil? (sut/begin-sync! mgr)))
    (let [s (sut/sync-manager-state mgr)]
      (is (false? (:sync-requested? s)))
      (is (nil? (:sync-request-reason s))))))

(deftest sync-manager-timeout-marks-unhealthy-test
  (let [mgr (sut/new-sync-manager {:group-commit 100
                                   :group-commit-ms 100
                                   :sync-adaptive? false
                                   :last-sync-ms 0})]
    (sut/request-sync-on-append! mgr 1 1)
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"Timed out waiting for durable LSN"
          (sut/await-durable-lsn! mgr 1 0 1)))
    (is (false? (:healthy? (sut/sync-manager-state mgr))))
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"unhealthy"
          (sut/request-sync-on-append! mgr 2 2)))
    (is (true? (:healthy? (sut/reset-sync-health! mgr))))))
