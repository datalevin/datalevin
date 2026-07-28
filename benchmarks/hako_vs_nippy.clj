(ns hako-vs-nippy
  "Measure Datom-serialization delta between hako and nippy on
  Datalevin-shaped payloads.

  Run: clj -M:bench -m hako-vs-nippy"
  (:require [criterium.core :as c]
            [datalevin.datom :as d]
            [s-exp.hako :as hako]
            [taoensso.nippy :as nippy])
  (:import (com.s_exp.hako Writer)
           (java.lang.foreign MemorySegment ValueLayout)))

(defn- seg->bytes ^bytes [^MemorySegment seg]
  (let [n (.byteSize seg)
        arr (byte-array n)]
    (MemorySegment/copy seg ValueLayout/JAVA_BYTE 0 arr 0 n)
    arr))

(def payloads
  {:datom-small     (d/datom 1 :name "Alice" 100 true)
   :datom-numeric   (d/datom 42 :age 30 100 true)
   :datom-ref       (d/datom 42 :owner 7 100 true)
   :datom-batch-10  (vec (for [i (range 10)]
                           (d/datom i (rand-nth [:name :email :age :status :owner])
                                    (if (odd? i) (str "v-" i) i)
                                    (+ 1000 i) (even? i))))
   :datom-batch-100 (vec (for [i (range 100)]
                           (d/datom i (rand-nth [:name :email :age :status :owner])
                                    (if (odd? i) (str "v-" i) i)
                                    (+ 1000 i) (even? i))))
   :datom-batch-1k  (vec (for [i (range 1000)]
                           (d/datom i (rand-nth [:name :email :age :status :owner])
                                    (if (odd? i) (str "v-" i) i)
                                    (+ 1000 i) (even? i))))})

(defn -main [& args]
  (let [selected (if (seq args)
                   (select-keys payloads (map keyword args))
                   payloads)
        wr (hako/writer 65536)
        rd (hako/reader (byte-array 8))]
    (doseq [[label payload] selected]
      (println "===" label "===")
      (let [hako-enc       (hako/encode payload)
            nippy-fast-enc (nippy/fast-freeze payload)
            nippy-enc      (nippy/freeze payload)]
        (println "  size — hako:" (alength hako-enc)
                 " nippy-fast:" (alength nippy-fast-enc)
                 " nippy:" (alength nippy-enc))
        (println "  hako encode-into! → seg:")
        (c/quick-bench (hako/encode-into! wr payload))
        (println "  hako encode → byte[]:")
        (c/quick-bench (hako/encode payload))
        (println "  nippy fast-freeze:")
        (c/quick-bench (nippy/fast-freeze payload))
        (println "  nippy freeze:")
        (c/quick-bench (nippy/freeze payload))
        (println "  hako decode (byte[]):")
        (c/quick-bench (hako/decode hako-enc {:cache-idents true}))
        (println "  nippy fast-thaw:")
        (c/quick-bench (nippy/fast-thaw nippy-fast-enc))
        (println "  nippy thaw:")
        (c/quick-bench (nippy/thaw nippy-enc))))
    (.close wr)))



;; │ payload          │ hako-seg encode │ nippy-fast encode │ ratio │ hako-seg decode │ nippy-fast decode │ ratio │
;; ├──────────────────┼─────────────────┼───────────────────┼───────┼─────────────────┼───────────────────┼───────┤
;; │ single datom     │ 141 ns          │ 218 ns            │ 1.5×  │ 203 ns          │ 222 ns            │ 1.1×  │
;; │ 100-datom batch  │ 11.3 µs         │ 19.4 µs           │ 1.7×  │ 18.5 µs         │ 29.5 µs           │ 1.6×  │
;; │ 1000-datom batch │ 103 µs          │ 203 µs            │ 2.0×  │ 136 µs          │ 298 µs            │ 2.2×  │

;; Wire size:

;; │ payload          │ hako    │ nippy-fast │ nippy (Snappy) │ vs nippy-fast │ vs nippy    │
;; ├──────────────────┼─────────┼────────────┼────────────────┼───────────────┼─────────────┤
;; │ single datom     │ 37 B    │ 47 B       │ 51 B           │ 21% smaller   │ 27% smaller │
;; │ 100-datom batch  │ 2574 B  │ 4541 B     │ 4545 B         │ 43% smaller   │ 43% smaller │
;; │ 1000-datom batch │ 26347 B │ 47338 B    │ 13702 B        │ 44% smaller   │ 92% BIGGER  │
