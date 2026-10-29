(ns datalevin.db.result-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.db.result-cache :as cache]
            [datalevin.storage :as storage])
  (:import [java.util Date]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest tuple-copies-own-nested-values-and-preserve-array-types
  (let [bytes (byte-array [1 2])
        instant (Date. 1234)
        row (object-array [7 bytes instant (long-array [3 4])])
        source (doto (FastList.) (.add row))
        entry (cache/snapshot source)]
    (aset-byte bytes 0 (byte 9))
    (.setTime instant 9)
    (.clear source)
    (dotimes [_ 3]
      (let [^FastList result (cache/restore entry)
            ^objects result-row (.get result 0)]
        (is (= 7 (aget result-row 0)))
        (is (= [1 2] (vec (aget result-row 1))))
        (is (= 1234 (.getTime ^Date (aget result-row 2))))
        (is (= [3 4] (vec (aget result-row 3))))
        (aset-byte ^bytes (aget result-row 1) 0 (byte 8))
        (.setTime ^Date (aget result-row 2) 8)
        (aset-long ^longs (aget result-row 3) 0 8)
        (.clear result))))
  (doseq [source [(into-array String ["one" nil])
                  (into-array Date [(Date. 1234) nil])
                  (byte-array [1 2]) (int-array [1 2]) (long-array [1 2])]]
    (let [result (cache/restore (cache/snapshot source))]
      (is (= (class source) (class result)))
      (is (= (vec source) (vec result)))
      (is (not (identical? source result))))))

(deftest incompatible-arrays-and-cycles-bypass-retention
  (is (nil? (cache/snapshot
              (into-array clojure.lang.LazySeq [(lazy-seq (list 1))]))))
  (let [array (object-array 1)
        rows (FastList.)]
    (aset array 0 array)
    (.add rows rows)
    (is (nil? (cache/snapshot array)))
    (is (nil? (cache/snapshot rows)))))

(deftest cache-miss-keeps-fresh-native-tuples-and-detaches-retention
  (with-open [^java.io.Closeable conn
              (d/create-conn nil {:value {}}
                             {:background-sampling? false :kv-opts {:inmemory? true}})]
    (let [store (:store @conn)]
      (doseq [pattern [[:init-tuples :value nil nil true]
                       [:search-tuples nil :value nil]
                       [:eav-scan-v (doto (FastList.) (.add (object-array [1])))
                        0 [[:value {}]]]]]
        (let [source (doto (FastList.) (.add (object-array [(byte-array [1 2])])))
              key (db/index-cache-key store pattern)
              result (db/cache-result store (db/cache-token store) key source)]
          (is (identical? source result))
          (aset-byte ^bytes (aget ^objects (.get ^FastList result 0) 0) 0 (byte 9))
          (.clear ^FastList result)
          (dotimes [_ 2]
            (let [^FastList cached (db/cache-get store key)]
              (is (= [1 2] (vec (aget ^objects (.get cached 0) 0))))
              (aset-byte ^bytes (aget ^objects (.get cached 0) 0) 0 (byte 8))
              (.clear cached)))))
      (testing "presence scans can share input rows and keep the detached return"
        (let [input-row (object-array [:original])
              source (doto (FastList.) (.add input-row))
              key [:eav-scan-v source 0 [[:value {:skip? true}]]]
              ^FastList result (db/cache-result store (db/cache-token store) key source)]
          (is (not (identical? source result)))
          (aset ^objects (.get result 0) 0 :changed)
          (is (= :original (aget input-row 0)))))
      (testing "lazy results remain materialized on their first return"
        (let [source (lazy-seq (list 1 2))
              result (db/cache-result store (db/cache-token store) [:lazy] source)]
          (is (list? result))
          (is (= '(1 2) result))))
      (testing "an older generation cannot publish a fresh list"
        (let [token (db/cache-token store)
              source (doto (FastList.) (.add (object-array [:stale])))
              key [:search-tuples nil :value nil]]
          (db/refresh-cache store)
          (is (identical? source (db/cache-result store token key source)))
          (is (nil? (db/cache-get store key))))))))

(deftest native-reader-misses-and-hits-own-mutable-values
  (with-open [^java.io.Closeable conn
              (d/create-conn nil {:bytes {:db/valueType :db.type/bytes}
                                  :instant {:db/valueType :db.type/instant}}
                             {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :bytes (byte-array [1 2]) :instant (Date. 1234)}])
    (let [view @conn
          ranges [[[:closed c/v0] [:closed c/vmax]]]]
      (doseq [[attr expected value mutate]
              [[:bytes [1 2] vec #(aset-byte ^bytes % 0 (byte 9))]
               [:instant 1234 #(.getTime ^Date %) #(.setTime ^Date % 9)]]
              [read index] [[#(db/-search-tuples view [1 attr nil]) 0]
                            [#(db/-init-tuples-list view attr ranges nil true) 1]]]
        (dotimes [_ 3]
          (let [^FastList rows (read)
                ^objects row (.get rows 0)
                result (aget row index)]
            (is (= 1 (.size rows)))
            (is (= expected (value result)))
            (mutate result)
            (aset row index :changed)
            (.clear rows)
            (is (= expected (value (:v (first (d/datoms view :eav 1 attr))))))))))))

(deftest native-eav-misses-and-hits-own-scalar-many-and-presence-results
  (with-open [^java.io.Closeable conn
              (d/create-conn nil {:bytes {:db/valueType :db.type/bytes}
                                  :instant {:db/valueType :db.type/instant}
                                  :many {:db/valueType :db.type/bytes
                                         :db/cardinality :db.cardinality/many}}
                             {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :bytes (byte-array [1 2]) :instant (Date. 1234)
                       :many [(byte-array [3]) (byte-array [4])]}])
    (let [view @conn
          input (doto (FastList.)
                  (.add (object-array [1 :left]))
                  (.add (object-array [1 :right])))]
      (doseq [[attrs expected decode mutate]
              [[[[:bytes {:skip? false}]] [[1 2]] vec #(aset-byte ^bytes % 0 (byte 9))]
               [[[:bytes {:skip? true}] [:instant {:skip? false}]] [1234]
                #(.getTime ^Date %) #(.setTime ^Date % 9)]
               [[[:many {:skip? false}]] [[3] [4]] vec #(aset-byte ^bytes % 0 (byte 9))]
               [[[:bytes {:skip? true}]] nil nil nil]]
              prepared? [false true]]
        (testing (pr-str {:attrs attrs :prepared? prepared?})
          (db/refresh-cache (:store view))
          (let [reader (storage/prepare-eav-scan-v-list (d/schema conn) attrs)
                read (if prepared?
                       #(db/-eav-scan-v-list view input 0 attrs reader)
                       #(db/-eav-scan-v-list view input 0 attrs))]
            (dotimes [_ 3]
              (let [^FastList rows (read)]
                (if expected
                  (is (= (frequencies (for [marker [:left :right] v expected] [marker v]))
                         (frequencies (map (fn [^objects row] [(aget row 1) (decode (aget row 2))]) rows))))
                  (is (= #{:left :right} (set (map #(aget ^objects % 1) rows)))))
                (doseq [^objects row rows]
                  (is (not (identical? row (.get input 0))))
                  (is (not (identical? row (.get input 1))))
                  (when mutate (mutate (aget row 2)))
                  (aset row 0 :changed)
                  (aset row 1 :changed))
                (.clear rows)
                (is (= [[1 :left] [1 :right]] (mapv vec input)))))))))))
