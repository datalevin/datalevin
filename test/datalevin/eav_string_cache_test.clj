(ns datalevin.eav-string-cache-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as b]
            [datalevin.constants :as c])
  (:import [datalevin.cpp EavStringCache]
           [java.nio ByteBuffer ByteOrder]))

(defn- cache ^EavStringCache [capacity]
  (EavStringCache. capacity c/type-string c/false-value))

(defn- encoded ^ByteBuffer [value direct?]
  (let [bytes (b/indexable-bytes (b/indexable nil 7 value :db.type/string c/normal))
        buffer (if direct? (ByteBuffer/allocateDirect (alength bytes))
                           (ByteBuffer/allocate (alength bytes)))]
    (.put buffer ^bytes bytes)
    (.flip buffer)
    buffer))

(defn- decode [^EavStringCache cache ^ByteBuffer buffer]
  (or (.lookup cache buffer)
      (.remember cache (b/avg->inline-value buffer))))

(deftest cached-strings-survive-borrowed-buffer-reuse
  (doseq [direct? [false true]]
    (let [dictionary (cache 32)
          ^ByteBuffer borrowed (encoded "old" direct?)
          old (decode dictionary borrowed)
          replacement (b/indexable-bytes (b/indexable nil 7 "new" :db.type/string c/normal))]
      (.clear borrowed)
      (.put borrowed ^bytes replacement)
      (.flip borrowed)
      (is (= "new" (decode dictionary borrowed)))
      (is (identical? old (decode dictionary (encoded "old" direct?))))
      (is (= 2 (.size dictionary))))))

(deftest complete-keys-preserve-length-unicode-and-buffer-position
  (doseq [direct? [false true]]
    (let [dictionary (cache 128)
          values (vec (concat ["" "a" "a\u0000" "\u0000a" "é" "東京" "😀"]
                              (map #(apply str (repeat % "x")) (range 1 18))))
          originals (mapv #(decode dictionary (encoded % direct?)) values)]
      (doseq [_ (range 3)
              [value original] (map vector values originals)]
        (let [^ByteBuffer buffer (encoded value direct?)]
          (.position buffer 3)
          (is (identical? original (.lookup dictionary buffer)))
          (is (= 3 (.position buffer)))
          (is (= value (decode dictionary buffer)))))
      (is (= (count (distinct values)) (.size dictionary))))))

(deftest unsupported-values-retire-the-dictionary
  (let [^ByteBuffer long-value (encoded (apply str (repeat 100 "x")) false)
        ^ByteBuffer giant (encoded "short" false)
        ^ByteBuffer generic (ByteBuffer/wrap
                              (b/indexable-bytes (b/indexable nil 7 "generic" :data c/normal)))
        ^ByteBuffer little-endian (doto (encoded "endian" false)
                                    (.order ByteOrder/LITTLE_ENDIAN))]
    (.put giant (dec (.limit giant)) (byte c/true-value))
    (doseq [buffer [long-value giant generic little-endian]]
      (let [dictionary (cache 8)]
        (is (nil? (.lookup dictionary buffer)))
        (is (false? (.isEnabled dictionary)))
        (.remember dictionary "not retained")
        (is (= 0 (.size dictionary))))))
  (let [dictionary (cache 8)]
    (is (nil? (.lookup dictionary (encoded "value" false))))
    (is (= 42 (.remember dictionary 42)))
    (is (= 0 (.size dictionary)))
    (is (= "value" (decode dictionary (encoded "value" false))))))

(deftest an-underestimated-domain-remains-bounded-and-correct
  (let [dictionary (cache 3)]
    (doseq [i (range 100)
            value [(str i) "again"]]
      (is (= value (decode dictionary (encoded value false))))
      (is (<= (.size dictionary) 3)))
    (is (thrown? IllegalArgumentException (cache 0)))
    (is (thrown? IllegalArgumentException (cache (inc EavStringCache/MAX_ENTRIES))))))
