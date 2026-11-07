(ns datalevin.db.giant-reference-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.index :as idx]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [taoensso.nippy :as nippy])
  (:import [java.io DataInput DataOutput]))

(use-fixtures :each db-fixture)

(defrecord CountedPayload [text])
(def ^:private decodes (atom 0))

(nippy/extend-freeze CountedPayload :datalevin.test/giant-reference
  [value ^DataOutput out]
  (nippy/freeze-to-out! out (:text value)))

(nippy/extend-thaw :datalevin.test/giant-reference
  [^DataInput in]
  (swap! decodes inc)
  (->CountedPayload (nippy/thaw-from-in! in)))

(def ^:private attrs (mapv #(keyword (str "payload" %)) (range 8)))
(def ^:private schema
  (merge (zipmap attrs (repeat {}))
         {:key {:db/valueType :db.type/string :db/unique :db.unique/identity}
          :text {:db/valueType :db.type/string}
          :number {:db/valueType :db.type/long}
          :many {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}}))
(def ^:private options
  {:background-sampling? false :snapshot-scheduler? false})
(defn- text [suffix] (str (apply str (repeat 2048 "x")) suffix))

(defn- request [path value]
  (case path
    :scalar [[:db/add 1 :payload0 value]]
    :mixed [[:db/add 1 :payload0 value] {:number 2}]
    :identity [{:key "one" :payload0 value}]
    :general [{:db/id 1 :payload0 value}]
    :wide (mapv #(vector :db/add 1 % value) attrs)))

(deftest replacements-decode-the-old-payload-once
  (doseq [wal? [false true] compressed? [false true]]
    (binding [c/*giants-zstd-threshold* (if compressed? 0 Long/MAX_VALUE)]
      (with-open [conn (d/create-conn nil schema (assoc options :wal? wal?))]
        (doseq [path [:scalar :mixed :identity :general :wide]]
          (testing (str [wal? compressed? path])
            (let [before (->CountedPayload (text "before"))
                  after (->CountedPayload (text "after"))]
              (d/transact! conn [(merge {:db/id 1 :key "one"}
                                       (zipmap attrs (repeat before)))])
              (reset! decodes 0)
              (let [report (d/transact! conn (request path after))
                    retractions (remove datom/datom-added (:tx-data report))]
                (is (= (if (= path :wide) 8 1) @decodes))
                (is (every? #(nil? (meta %)) retractions))
                (is (every? #(nil? (datom/storage-reference %)) retractions))
                (is (= after (:payload0 (d/entity @conn 1))))))))))))

(deftest references-preserve-pending-visibility-and-rollback
  (doseq [wal? [false true] prepare? [false true]]
    (binding [c/*use-prepare-path* prepare?]
      (with-open [conn (d/create-conn nil schema (assoc options :wal? wal?))]
        (let [a (text "a") b (text "b") c (text "c")]
          (d/transact! conn [{:db/id 1 :text a :many [a b]}])
          (d/with-transaction [tx conn]
            (d/transact! tx [[:db/add 1 :text b]])
            (d/transact! tx [[:db/add 1 :text c] {:number 3}])
            (d/transact! tx [[:db/retract 1 :many a] [:db/add 1 :many a]
                             [:db/retract 1 :many a] [:db/add 1 :many c]])
            (is (= c (:text (d/entity @tx 1))))
            (is (= #{b c} (:many (d/entity @tx 1))))
            (is (= a (:text (d/entity @conn 1))))
            (d/abort-transact tx))
          (is (= a (:text (d/entity @conn 1))))
          (is (= #{a b} (:many (d/entity @conn 1))))
          (d/transact! conn [[:db/add 1 :text b] [:db/add 1 :text a]
                            [:db/add 1 :text c]])
          (is (= c (:text (d/entity @conn 1))))
          (is (= 3 (d/entries (d/datalog-kv conn) c/giants))))))))

(deftest references-are-scoped-to-the-native-writer
  (with-open [conn (d/create-conn nil schema (assoc options :wal? false))]
    (d/transact! conn [{:db/id 1 :text (text "one")}])
    (let [saved (atom nil)]
      (d/with-transaction [tx conn]
        (let [store (:store @tx)
              old (idx/preparation-first-v store 1 :text)
              retract (idx/retraction 1 :text old 1)]
          (is (some? (datom/storage-reference retract)))
          (is (nil? (meta retract)))
          (reset! saved retract)
          (let [decorated (with-meta retract {:test true})]
            (is (= {:test true} (meta decorated)))
            (is (number? (idx/take-giant-reference! (d/datalog-kv tx) decorated)))
            (is (= {:test true} (meta decorated)))
            (is (nil? (datom/storage-reference decorated))))
          (is (nil? (datom/storage-reference (assoc retract :v "changed"))))
          (is (nil? (datom/storage-reference (nippy/thaw (nippy/freeze retract))))))
        (d/abort-transact tx))
      (d/with-transaction [tx conn]
        (is (nil? (idx/take-giant-reference! (d/datalog-kv tx) @saved)))
        (is (nil? (datom/storage-reference @saved)))))))

(deftest an-obsolete-row-in-the-same-writer-is-not-retracted
  (with-open [conn (d/create-conn nil schema (assoc options :wal? false))]
    (let [before (text "one") after (text "two")]
      (d/transact! conn [{:db/id 1 :text before}])
      (d/with-transaction [tx conn]
        (let [store (:store @tx)
              old (idx/preparation-first-v store 1 :text)
              retract (idx/retraction 1 :text old 1)]
          (d/transact! tx [[:db/add 1 :text after]])
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                               #"Cannot retract missing giant value"
                               (i/load-datoms store [retract])))
          (is (= after (:text (d/entity @tx 1))))
          (is (= 1 (d/entries (d/datalog-kv tx) c/giants))))
        (d/abort-transact tx))
      (is (= before (:text (d/entity @conn 1)))))))

(deftest a-retained-retraction-can-resolve-in-a-later-writer
  (with-open [conn (d/create-conn nil schema (assoc options :wal? false))]
    (d/transact! conn [{:db/id 1 :payload0 (->CountedPayload (text "one"))}])
    (let [saved (atom nil)]
      (d/with-transaction [tx conn]
        (reset! saved (idx/retraction
                       1 :payload0 (idx/preparation-first-v (:store @tx) 1 :payload0) 1))
        (d/abort-transact tx))
      (reset! decodes 0)
      (d/with-transaction [tx conn]
        (i/load-datoms (:store @tx) [@saved])
        (is (= 1 @decodes))
        (is (nil? (:payload0 (d/entity @tx 1))))
        (is (nil? (datom/storage-reference @saved)))
        (d/abort-transact tx)))))

(deftest giant-replacements-survive-map-growth
  (binding [c/*giants-zstd-threshold* Long/MAX_VALUE]
    (doseq [wal? [false true]]
      (with-open [conn (d/create-conn nil schema
                        (assoc options :wal? wal? :kv-opts {:mapsize 1}))]
        (let [before (text "initial")
              after (.repeat "z" (* 2 1024 1024))]
          (d/transact! conn [{:db/id 1 :text before}])
          (d/transact! conn [[:db/add 1 :text after]])
          (is (= after (:text (d/entity @conn 1))))
          (is (= 1 (d/entries (d/datalog-kv conn) c/giants)))
          (d/transact! conn [[:db/add 1 :text before]])
          (is (= before (:text (d/entity @conn 1))))
          (is (= 1 (d/entries (d/datalog-kv conn) c/giants))))))))
