(ns datalevin.test.bits
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.bits :as b]
   [datalevin.constants :as c])
  (:import
   [java.nio ByteBuffer]))

(defn- avg-buffer
  ^ByteBuffer [value value-type giant-id]
  (let [bf (ByteBuffer/allocate 1024)]
    (b/put-buffer bf (b/indexable 1 2 value value-type giant-id) :avg)
    (.flip bf)
    bf))

(deftest direct-avg-decode-test
  (let [normal (avg-buffer "inline" :db.type/string 42)
        giant  (avg-buffer (apply str (repeat 1024 "x"))
                           :db.type/string 42)]
    (is (= c/normal (b/avg->giant-id normal)))
    (is (= "inline" (b/avg->inline-value normal)))
    (is (= 42 (b/avg->giant-id giant)))))
