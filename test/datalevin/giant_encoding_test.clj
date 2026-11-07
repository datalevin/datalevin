(ns datalevin.giant-encoding-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.index :as index]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInput DataOutput]
           [java.util Arrays Random]))

(use-fixtures :each db-fixture)

(defrecord CountedPayload [text])
(def ^:private serializations (atom 0))

(nippy/extend-freeze CountedPayload :datalevin.test/giant-encoding
  [value ^DataOutput out]
  (swap! serializations inc)
  (nippy/freeze-to-out! out (:text value)))

(nippy/extend-thaw :datalevin.test/giant-encoding
  [^DataInput in]
  (->CountedPayload (nippy/thaw-from-in! in)))

(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})

(deftest giant-storage-serializes-untyped-payload-once
  (let [payload (->CountedPayload (.repeat "x" 65536))]
    ;; Nippy can replay a custom writer while warming its thread-local buffer.
    (dotimes [_ 3] (b/serialize (datom/datom 1 :payload payload)))
    (doseq [wal? [false true] compressed? [false true] noindex? [false true]]
      (testing (str [wal? compressed? noindex?])
        (binding [c/*giants-zstd-threshold* (if compressed? 0 Long/MAX_VALUE)]
          (with-open [conn (d/create-conn nil {:payload {:db/noindex noindex?}}
                            (assoc options :wal? wal?))]
            ;; Exercise storage directly to exclude independent WAL encoding.
            (reset! serializations 0)
            (i/load-datoms (:store @conn) [(datom/datom 1 :payload payload)])
            (is (= 1 @serializations))
            (is (= payload (:payload (d/entity @conn 1))))
            (is (= 1 (d/entries (d/datalog-kv conn) c/giants)))))))))

(deftest preencoded-giants-retain-the-existing-wire-format
  (let [random-bytes (byte-array 65536)]
    (.nextBytes (Random. 42) random-bytes)
    (doseq [payload [(apply str (repeat 2048 "text"))
                     {:description (.repeat "x" 65536)} random-bytes]
            threshold [0 Long/MAX_VALUE]]
      (let [row (datom/datom 123 :payload payload)
            encoded (b/value-bytes payload :data)
            old (binding [c/*giants-zstd-threshold* threshold]
                  (index/encode-giant-datom row))
            prepared (binding [c/*giants-zstd-threshold* threshold]
                       (index/encode-giant-datom row encoded))]
        (is (= :raw (:vtype prepared)))
        (is (Arrays/equals ^bytes (:value old) ^bytes (:value prepared)))
        (is (= row (index/decode-giant-datom (:value prepared))))))))

(deftest preencoded-giants-preserve-nippy-cache-references
  (let [shared (with-meta [(.repeat "x" 2048) :payload :repeated]
                 {:payload :repeated})
        payload {:payload [:payload :payload :repeated :repeated]
                 :shared [(nippy/cache shared) (nippy/cache shared)]}
        dictionary (nippy/shared-dict [:payload :repeated :shared])]
    (doseq [dict [nil dictionary] threshold [0 Long/MAX_VALUE]]
      (binding [nippy/*shared-dict* dict c/*giants-zstd-threshold* threshold]
        (let [row (datom/datom 1 :payload payload)
              expected (b/deserialize (b/serialize row))
              encoded (b/value-bytes payload :data)
              decoded (index/decode-giant-datom
                        (:value (index/encode-giant-datom row encoded)))
              values (:shared (:v decoded))]
          (is (= expected decoded))
          (is (= (meta shared) (meta (first values))))
          (is (identical? (first values) (second values))))))))

(deftest untyped-prefix-collisions-and-typed-giants-remain-distinct
  (doseq [wal? [false true] compressed? [false true]]
    (binding [c/*giants-zstd-threshold* (if compressed? 0 Long/MAX_VALUE)]
      (with-open [conn (d/create-conn nil {:payload {}
                                         :typed {:db/valueType :db.type/string}}
                        (assoc options :wal? wal?))]
        (let [prefix (.repeat "x" 2048)
              a {:text (str prefix "a")} b {:text (str prefix "b")}
              a-index (b/indexable 1 1 a :data 1)
              b-index (b/indexable 1 1 b :data 1)]
          (is (Arrays/equals (b/indexable-bytes a-index)
                             (b/indexable-bytes b-index)))
          (d/transact! conn [{:db/id 1 :payload a :typed (str prefix "a")}
                             {:db/id 2 :payload b :typed (str prefix "b")}])
          (is (= a (:payload (d/entity @conn 1))))
          (is (= b (:payload (d/entity @conn 2))))
          (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :payload ?v]]
                             @conn a)))
          (d/with-transaction [tx conn]
            (d/transact! tx [[:db/add 1 :payload b]])
            (is (= b (:payload (d/entity @tx 1))))
            (d/abort-transact tx))
          (is (= a (:payload (d/entity @conn 1))))
          (d/transact! conn [[:db/add 1 :payload b]
                             [:db/retract 2 :payload b]])
          (is (= b (:payload (d/entity @conn 1))))
          (is (nil? (:payload (d/entity @conn 2))))
          (is (= (str prefix "b") (:typed (d/entity @conn 2))))
          (is (= 3 (d/entries (d/datalog-kv conn) c/giants))))))))
