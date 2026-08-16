(ns datom-alloc
  "Per-op alloc for datom-shaped payloads: hako vs nippy.
  Includes reused-Reader hako path for a fair comparison to
  nippy's fast-thaw (which reuses internal state)."
  (:require [datalevin.datom :as d]
            [s-exp.hako :as hako]
            [taoensso.nippy :as nippy])
  (:import (com.s_exp.hako Reader Writer)
           (com.sun.management ThreadMXBean)
           (java.lang.foreign MemorySegment ValueLayout)
           (java.lang.management ManagementFactory)))

(def payloads
  {:datom-small     (d/datom 1 :name "Alice" 100 true)
   :datom-batch-100 (vec (for [i (range 100)]
                           (d/datom i (rand-nth [:name :email :age :status :owner])
                                    (if (odd? i) (str "v-" i) i)
                                    (+ 1000 i) (even? i))))
   :datom-batch-1k  (vec (for [i (range 1000)]
                           (d/datom i (rand-nth [:name :email :age :status :owner])
                                    (if (odd? i) (str "v-" i) i)
                                    (+ 1000 i) (even? i))))})

(defn- ^ThreadMXBean tmx []
  (ManagementFactory/getThreadMXBean))

(defn- allocated-bytes ^long [^ThreadMXBean b]
  (.getThreadAllocatedBytes b (.getId (Thread/currentThread))))

(defn- alloc-per-op [f iters]
  (dotimes [_ 5000] (f))
  (System/gc)
  (let [b (tmx)
        start (allocated-bytes b)
        _ (dotimes [_ iters] (f))
        end (allocated-bytes b)]
    (/ (double (- end start)) iters)))

(defn -main [& _]
  (let [wr ^Writer (hako/writer 65536)
        rd ^Reader (hako/reader (byte-array 8))
        iters 50000]
    (println (format "%-20s %-24s %12s"
                     "payload" "op" "B/op"))
    (println (apply str (repeat 60 "-")))
    (doseq [[label v] payloads]
      (let [h-enc (hako/encode v)
            n-enc (nippy/fast-freeze v)
            nn-enc (nippy/freeze v)]
        (println (format "%-20s %-24s %12.0f" (name label) "hako encode-into!"
                         (alloc-per-op #(hako/encode-into! wr v) iters)))
        (println (format "%-20s %-24s %12.0f" (name label) "nippy fast-freeze"
                         (alloc-per-op #(nippy/fast-freeze v) iters)))
        (println (format "%-20s %-24s %12.0f" (name label) "hako decode (fresh Rd)"
                         (alloc-per-op #(hako/decode h-enc {:cache-idents true}) iters)))
        (println (format "%-20s %-24s %12.0f" (name label) "hako decode-into! (reused)"
                         (alloc-per-op #(hako/decode-into! rd h-enc {:cache-idents true}) iters)))
        (println (format "%-20s %-24s %12.0f" (name label) "nippy fast-thaw"
                         (alloc-per-op #(nippy/fast-thaw n-enc) iters)))
        (println)))))
