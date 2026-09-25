(ns ycsb-bench.workload
  "Logical records and seeded YCSB-style request generators."
  (:import [java.util Random]))

(set! *warn-on-reflection* true)

(def workloads
  {:a {:mix [[:read 50] [:update 50]] :distribution :zipfian}
   :b {:mix [[:read 95] [:update 5]] :distribution :zipfian}
   :c {:mix [[:read 100]] :distribution :zipfian}
   :d {:mix [[:read 95] [:insert 5]] :distribution :latest}
   :e {:mix [[:scan 95] [:insert 5]] :distribution :zipfian}
   :f {:mix [[:read 50] [:rmw 50]] :distribution :zipfian}})

(def operations [:read :update :insert :scan :rmw])
(def operation-code (zipmap operations (range)))

(defn fnvhash64
  "YCSB Utils.fnvhash64: eight low-to-high bytes, signed long overflow, abs.
  See https://github.com/brianfrankcooper/YCSB/blob/master/core/src/main/java/site/ycsb/Utils.java"
  ^long [^long value]
  (loop [value value, hash -3750763034362895579, i 0]
    (if (= i 8)
      (Math/abs (long hash))
      (recur (bit-shift-right value 8)
             (unchecked-multiply (bit-xor hash (bit-and value 255)) 1099511628211)
             (inc i)))))

(defn application-key
  "YCSB CoreWorkload's default hashed key name (zeropadding=1)."
  ^String [^long ordinal]
  (str "user" (fnvhash64 ordinal)))

(defn workload-model
  "Report the operation model; all workloads use YCSB string keys."
  [workload]
  (case workload
    :d :application-key-latest-v1
    :e :application-key-range-v1
    :f :ycsb-read-update-v1
    :application-key-point-v1))

;; Upstream ScrambledZipfianGenerator uses an inclusive [0, 10^10] source
;; range with this precomputed zeta, then hashes the draw modulo a fixed
;; destination keyspace. Keep that modulus stable as inserts commit.
(def ^:private scrambled-items 10000000001)
(def ^:private scrambled-zeta 26.46902820178302)
(def ^:private zipf-theta 0.99)
(def ^:private zipf-alpha (/ 1.0 (- 1.0 zipf-theta)))
(def ^:private zipf-second (Math/pow 0.5 zipf-theta))
(def ^:private scrambled-eta
  (/ (- 1.0 (Math/pow (/ 2.0 scrambled-items) (- 1.0 zipf-theta)))
     (- 1.0 (/ (+ 1.0 zipf-second) scrambled-zeta))))

(defn scrambled-rank
  "Upstream Gray/Zipfian inverse approximation for one uniform [0,1) draw."
  ^long [^double u]
  (let [uz (* u scrambled-zeta)]
    (cond (< uz 1.0) 0
          (< uz (+ 1.0 zipf-second)) 1
          :else (long (* scrambled-items
                         (Math/pow (+ (- (* scrambled-eta u) scrambled-eta) 1.0)
                                   zipf-alpha))))))

(defn choose-operation [^Random rng mix]
  (loop [draw (.nextInt rng 100), [[operation weight] & more] mix]
    (when (nil? weight)
      (throw (ex-info "Invalid operation mix: missing weight or weights do not cover the draw"
                      {:mix mix :remaining-draw draw})))
    (if (< draw (long weight))
      operation
      (recur (- draw (long weight)) more))))

(defn random-value
  "ASCII values have exactly field-length bytes in UTF-8."
  [^Random rng field-length]
  (let [chars (char-array field-length)]
    (dotimes [i field-length]
      (aset-char chars i (char (+ 97 (.nextInt rng 26)))))
    (String. chars)))

(defn record-values [^Random rng {:keys [field-count field-length]}]
  (mapv (fn [_] (random-value rng field-length)) (range field-count)))

(defn initial-values [seed id options]
  (record-values (Random. (unchecked-add (long seed) (long id))) options))

(defn zipf-cdf
  "Finite Zipf weights, exponent 0.99. Setup is outside measured phases."
  [capacity]
  (let [cdf (double-array capacity)]
    (loop [i 0, total 0.0]
      (when (< i (long capacity))
        (let [total (+ total (/ 1.0 (Math/pow (double (inc i)) 0.99)))]
          (aset-double cdf i total)
          (recur (inc i) total))))
    cdf))

(defn choose-key
  "Select a committed ordinal. Zipfian uses upstream's scrambled generator
  and rejects uncommitted ordinals without changing its fixed modulus.
  Latest reverses finite Zipf ranks over the committed prefix."
  [^Random rng distribution state visible]
  (let [n (long visible)]
    (case distribution
      :uniform (long (.nextInt rng (int n)))
      :zipfian
      (let [keyspace (long state)]
        (loop []
          (let [id (rem (fnvhash64 (scrambled-rank (.nextDouble rng))) keyspace)]
            (if (< -1 id n) id (recur)))))
      :latest
      (let [^doubles cdf state
            draw (* (.nextDouble rng) (aget cdf (dec n)))
            rank (loop [lo 0, hi (dec n)]
                   (if (= lo hi)
                     lo
                     (let [mid (quot (+ lo hi) 2)]
                       (if (< draw (aget cdf mid))
                         (recur lo mid)
                         (recur (inc mid) hi)))))]
        (- n 1 rank)))))

(defn grow-cdf!
  "Grow a timed phase's Zipf table as inserts extend the visible keyspace.
  Existing prefix weights stay unchanged. Growth is included in phase timing."
  [cdf-ref visible]
  (let [^doubles current @cdf-ref]
    (if (<= (long visible) (alength current))
      current
      (locking cdf-ref
        (let [^doubles current @cdf-ref
              n (alength current)]
          (if (<= (long visible) n)
            current
            (let [capacity (min Integer/MAX_VALUE (max (long visible) (* 2 (long n))))
                  expanded (java.util.Arrays/copyOf current (int capacity))]
              (loop [i n, total (aget current (dec n))]
                (when (< i capacity)
                  (let [total (+ total (/ 1.0 (Math/pow (double (inc i)) 0.99)))]
                    (aset-double expanded i total)
                    (recur (inc i) total))))
              (reset! cdf-ref expanded))))))))

(defn keyspace [records]
  (atom {:next records :visible records :pending (sorted-set)}))

(defn reserve-key! [space]
  (:next (first (swap-vals! space update :next
                            (fn [n]
                              (when (>= (long n) Integer/MAX_VALUE)
                                (throw (ex-info "Benchmark keyspace exceeds the supported range" {})))
                              (inc (long n)))))))

(defn acknowledge-key!
  "Publish inserts after commit, retaining out-of-order completions until
  earlier reserved IDs have committed. Readers cannot observe a hole."
  [space id]
  (swap! space
         (fn [{:keys [visible pending] :as state}]
           (loop [visible visible, pending (conj pending id)]
             (if (contains? pending visible)
               (recur (inc visible) (disj pending visible))
               (assoc state :visible visible :pending pending))))))
