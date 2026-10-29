(ns datalevin.cache-mutability-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]])
  (:import [java.util Date]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :once server-fixture)

(defn- check-reads [location]
  (with-open [^java.io.Closeable conn
              (d/create-conn location
                             {:bytes {:db/valueType :db.type/bytes}
                              :data {}
                              :instant {:db/valueType :db.type/instant}}
                             {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :bytes (byte-array [1 2])
                       :data (with-meta {:nested [(byte-array [1 2])]}
                               {:bytes (byte-array [1 2])})
                       :instant (Date. 1234)}])
    (let [view @conn
          store (:store view)
          query '[:find ?v . :where [1 :bytes ?v]]
          prepared-query (d/prepare-q view query)
          prepared-pull (d/prepare-pull view [:bytes])]
      (d/datoms view :eav 1 :bytes)
      (is (some? (db/cache-get store
                              (db/index-cache-key store [:datoms :eav 1 :bytes nil]))))
      (doseq [[operation read]
              [[:datoms #(:v (first (d/datoms view :eav 1 :bytes)))]
               [:datoms-all #(:v (first (d/datoms view :ave :bytes)))]
               [:datoms-limited #(:v (first (d/datoms view :eav 1 :bytes nil 1)))]
               [:search #(:v (first (d/search-datoms view 1 :bytes nil)))]
               [:seek #(:v (first (d/seek-datoms view :eav 1 :bytes)))]
               [:rseek #(:v (first (d/rseek-datoms view :eav 1 :bytes)))]
               [:range #(:v (first (d/index-range view :bytes nil nil)))]
               [:first #(:v (db/-first view [1 :bytes nil]))]
               [:pull #(:bytes (d/pull view [:bytes] 1))]
               [:prepared-pull #(:bytes (prepared-pull 1))]
               [:query #(d/q query view)]
               [:prepared-query #(prepared-query [])]]]
        (when-not (and (= operation :datoms-limited) (db/remote-store? store))
          (testing (name operation)
            ;; Exercise both the returned miss and subsequent cache hits.
            (dotimes [_ 3]
              (let [^bytes result (read)]
                (is (= [1 2] (vec result)))
                (aset-byte result 0 (byte 9))
                (is (= [9 2] (vec result)))
                (is (= [1 2] (vec (:v (i/ea-first-datom store 1 :bytes))))))))))
      (testing "mutable values nested in data and metadata"
        (dotimes [_ 3]
          (let [result (:v (first (d/datoms view :eav 1 :data)))
                ^bytes nested (first (:nested result))
                ^bytes metadata (:bytes (meta result))]
            (is (= [1 2] (vec nested) (vec metadata)))
            (aset-byte nested 0 (byte 9))
            (aset-byte metadata 0 (byte 9)))))
      (testing "mutable instants"
        (dotimes [_ 3]
          (let [^Date result (:v (first (d/datoms view :eav 1 :instant)))]
            (is (= 1234 (.getTime result)))
            (.setTime result 9)))))))

(deftest mutations-do-not-poison-local-or-remote-reads
  (check-reads nil)
  (doseq [interval [0 60000]]
    (binding [c/*remote-db-last-modified-check-interval-ms* interval]
      (check-reads (str "dtlv://datalevin:datalevin@localhost/mutable-cache-"
                        (random-uuid))))))

(deftest cached-tuples-own-their-containers-and-array-values
  (with-open [^java.io.Closeable conn
              (d/create-conn nil {:value {}} {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :value [1 2]}])
    (let [view @conn
          read #(db/-search-tuples view [1 :value nil])]
      (dotimes [_ 3]
        (let [^FastList rows (read)
              ^objects row (.get rows 0)]
          (is (= [1 2] (aget row 0)))
          (aset row 0 :changed)
          (.clear rows))))
    (let [store (:store @conn)
          result (object-array [(long-array [1 2])
                                (datom/datom 1 :value (byte-array [1 2]))])]
      (db/cache-put store ::arrays result)
      (aset-long ^longs (aget result 0) 0 9)
      (dotimes [_ 3]
        (let [^objects cached (db/cache-get store ::arrays)]
          (is (= [1 2] (vec (aget cached 0))))
          (is (= [1 2] (vec (:v (aget cached 1)))))
          (aset-long ^longs (aget cached 0) 0 9)
          (aset-byte ^bytes (:v (aget cached 1)) 0 (byte 9)))))
    (testing "immutable results retain identity and opaque values bypass caching"
      (let [store (:store @conn)
            immutable {:nested [1 #{2 3}]}
            opaque (atom :mutable)]
        (db/cache-put store ::immutable immutable)
        (is (identical? immutable (db/cache-get store ::immutable)))
        (is (false? (db/cache-put-if-current store (db/cache-token store)
                                           ::opaque {:nested opaque})))
        (is (nil? (db/cache-get store ::opaque)))))
    (testing "immutable metadata survives copies of mutable contents"
      (let [store (:store @conn)
            result (with-meta [(byte-array [1 2])] {:label :retained})]
        (db/cache-put store ::metadata result)
        (let [cached (db/cache-get store ::metadata)]
          (is (= {:label :retained} (meta cached)))
          (is (not (identical? (first result) (first cached)))))))
    (testing "cyclic mutable containers bypass retention"
      (let [store (:store @conn)
            cyclic (object-array 1)]
        (aset cyclic 0 cyclic)
        (is (false? (db/cache-put-if-current store (db/cache-token store)
                                           ::cyclic cyclic)))
        (is (nil? (db/cache-get store ::cyclic)))))))
