(ns datalevin.storage-independence-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.storage :as s]
            [datalevin.test.core :refer [db-fixture]])
  (:import [datalevin.storage WriteGroup]
           [datalevin.utl WriterContext]))

(use-fixtures :each db-fixture)

(def ^:private attrs (mapv #(keyword (str "field" %)) (range 16)))
(def ^:private schema
  (merge (zipmap attrs (repeat {:db/valueType :db.type/long}))
         {:text {:db/valueType :db.type/string}
          :tags {:db/valueType :db.type/long :db/cardinality :db.cardinality/many}}))
(def ^:private options
  {:wal? true :snapshot-scheduler? false :background-sampling? false})

(defn- changes [before after]
  (vec (mapcat (fn [attr]
                 [(datom/datom 1 attr before c/tx0 false)
                  (datom/datom 1 attr after c/tx0 true)]) attrs)))

(defn- independent? [^WriteGroup group]
  (true? (aget ^objects (.-storage-context group) 2)))

(defn- drain! [group writer frozen?]
  (if frozen?
    ((:encode (s/freeze-group-storage-datoms! group writer)))
    (s/take-group-storage-rows! group writer)))

(defn- with-staging [f]
  (with-open [conn (d/create-conn nil schema (assoc options :wal? false))]
    (d/transact! conn [(assoc (zipmap attrs (repeat 0)) :db/id 1)])
    (l/with-transaction-kv [writer (kv/raw-lmdb (d/datalog-kv conn))]
      (let [store (s/transfer (:store @conn) writer)
            group (s/write-group writer)
            ^WriterContext context (l/writer-context writer)]
        (.setRequestContext context {:datalog-prepare? true})
        (.setCallbacks context nil (fn []) nil nil nil)
        (binding [s/*write-group* group c/*ordered-datom-writes?* false]
          (f store group writer))))))

(deftest planner-proof-reaches-scalar-and-mixed-storage-segments
  (doseq [mixed? [false true]]
    (with-open [conn (d/create-conn nil schema options)]
      (d/transact! conn [(assoc (zipmap attrs (repeat 0)) :db/id 1)])
      (d/with-transaction [tx conn]
        (d/transact! tx [])
        (let [forms (mapv #(vector :db/add 1 % 1) attrs)
              report (d/transact! tx (cond-> forms mixed? (conj {:text "new"})))
              group s/*write-group*]
          (is (= (if mixed? 33 32) (count (:tx-data report))))
          (is (independent? group))
          (is (= (mapv (juxt :e :a :v datom/datom-added)
                       (cond-> (changes 0 1) mixed? (conj (datom/datom 2 :text "new"))))
                 (mapv (juxt :e :a :v datom/datom-added) (:tx-data report))))
          (is (= 1 ((first attrs) (d/entity @tx 1))))
          (is (not (independent? group)))))
      (is (= (zipmap attrs (repeat 1)) (d/pull @conn attrs 1))))))

(deftest nonempty-requests-drop-the-proof-before-overlapping-cycles
  (doseq [frozen? [false true]]
    (with-staging
      (fn [store group writer]
        (s/stage-group-datoms! store (changes 0 1) true)
        (is (independent? group))
        (s/stage-group-datoms! store (changes 1 0) true)
        (is (not (independent? group)))
        (let [rows (drain! group writer frozen?)]
          (is (zero? (count rows)))
          (is (not (independent? group)))
          (i/transact-kv writer rows)
          (is (= (zipmap attrs (repeat 0))
                 (into {} (map #(vector % (i/ea-first-v store 1 %)) attrs)))))))))

(deftest unproven-requests-cannot-inherit-or-establish-the-proof
  (doseq [frozen? [false true] unproven-first? [false true]]
    (with-staging
      (fn [store group writer]
        (let [cycle [(datom/datom 1 :tags 9) (datom/datom 1 :tags 9 c/tx0 false)]]
          (if unproven-first?
            (do (s/stage-group-datoms! store cycle)
                (s/stage-group-datoms! store (changes 0 1) true))
            (do (s/stage-group-datoms! store (changes 0 1) true)
                (s/stage-group-datoms! store cycle)))
          (is (not (independent? group)))
          (let [rows (drain! group writer frozen?)]
            (is (= 32 (count rows)))
            (i/transact-kv writer rows)
            (is (nil? (i/ea-first-v store 1 :tags)))))))))

(deftest empty-and-rejected-input-preserves-only-the-accepted-proof
  (doseq [frozen? [false true]]
    (with-staging
      (fn [store group writer]
        (s/stage-group-datoms! store (changes 0 1) true)
        (s/stage-group-datoms! store [])
        (is (independent? group))
        (is (nil? (s/stage-group-datoms! store
                    [(datom/datom 2 (first attrs) 1)
                     (datom/datom 2 :text (.repeat "x" 101))])))
        (is (independent? group))
        (is (thrown? clojure.lang.ExceptionInfo
                     (s/stage-group-datoms! store
                       (lazy-seq
                         (cons (datom/datom 2 (first attrs) 1)
                               (lazy-seq (throw (ex-info "stream failed" {}))))))))
        (is (independent? group))
        (let [rows (drain! group writer frozen?)]
          (is (= 32 (count rows)))
          (is (not (independent? group)))
          (i/transact-kv writer rows))
        ;; A later segment must not inherit the first segment's proof.
        (s/stage-group-datoms! store (into (changes 1 2) (changes 2 1)))
        (is (zero? (count (drain! group writer frozen?))))))))

(deftest frozen-proven-segments-keep-their-own-datoms-and-proof
  (with-staging
    (fn [store group writer]
      (s/stage-group-datoms! store (changes 0 1) true)
      (let [first-plan (s/freeze-group-storage-datoms! group writer)]
        (is (not (independent? group)))
        (s/stage-group-datoms! store (changes 1 0) true)
        (is (independent? group))
        (let [second-plan (s/freeze-group-storage-datoms! group writer)]
          (is (= 32 (:row-count first-plan) (:row-count second-plan)))
          (i/transact-kv writer ((:encode first-plan)))
          (is (= 1 (i/ea-first-v store 1 (first attrs))))
          (i/transact-kv writer ((:encode second-plan)))
          (is (= 0 (i/ea-first-v store 1 (first attrs)))))))))
