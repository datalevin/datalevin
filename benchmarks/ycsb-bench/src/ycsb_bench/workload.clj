(ns ycsb-bench.workload
  "Logical records and seeded YCSB-style request generators."
  (:require [datalevin.core :as d])
  (:import [datalevin.db DB]
           [datalevin.utl LRUCache]
           [java.util Random]))

(set! *warn-on-reflection* true)

(defn rmw-reader
  "Reuse a prepared pull across transaction views. Execute it with the current
  transaction DB via execute-prepared's explicit-view arity."
  [^DB db attributes]
  ;; Transaction Store wrappers change on each commit. The DB's bounded reader
  ;; cache is shared across those views and belongs to this database's lifetime.
  ;; A namespaced key keeps these wrappers distinct from core pull readers.
  (let [^LRUCache cache (.-pull-readers db)
        k [::rmw-reader attributes]]
    (or (.get cache k)
        (locking cache
          (or (.get cache k)
              (let [reader (d/prepare-pull db attributes)]
                (.put cache k reader)
                reader))))))

(def workloads
  {:a {:mix [[:read 50] [:update 50]] :distribution :zipfian}
   :b {:mix [[:read 95] [:update 5]] :distribution :zipfian}
   :c {:mix [[:read 100]] :distribution :zipfian}
   :d {:mix [[:read 95] [:insert 5]] :distribution :latest}
   :e {:mix [[:scan 95] [:insert 5]] :distribution :zipfian}
   :f {:mix [[:read 50] [:rmw 50]] :distribution :zipfian}})

(def operations [:read :update :insert :scan :rmw])
(def operation-code (zipmap operations (range)))

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

(defn modified-value
  "Derive a replacement from the value read, preserving its byte length."
  [^String value]
  (str (char (+ 97 (mod (inc (- (int (.charAt value 0)) 97)) 26)))
       (.substring value 1)))

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
  "Sample only the contiguous prefix of committed records. Zipfian favors
  low IDs; latest reverses the sampled rank to favor newly committed IDs."
  [^Random rng distribution ^doubles cdf visible]
  (let [n (long visible)]
    (if (= distribution :uniform)
      (long (.nextInt rng (int n)))
      (let [draw (* (.nextDouble rng) (aget cdf (dec n)))
            rank (loop [lo 0, hi (dec n)]
                   (if (= lo hi)
                     lo
                     (let [mid (quot (+ lo hi) 2)]
                       (if (< draw (aget cdf mid))
                         (recur lo mid)
                         (recur (inc mid) hi)))))]
        (if (= distribution :latest) (- n 1 rank) rank)))))

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
