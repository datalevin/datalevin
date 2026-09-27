(ns datalevin.db.tx.execute-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.db.tx.common :as common]
   [datalevin.db.tx.execute :as execute]
   [datalevin.interface :as i]
   [datalevin.storage.schema :as schema])
  (:import
   [java.util Comparator]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- execute-with-reads
  [schema stored entities & [cached-max-eid initial-tx-data]]
  (let [reads   (atom [])
        rschema (schema/schema->rschema schema)
        store   (reify i/IStore
                  (opts [_] {})
                  (schema [_] schema)
                  (rschema [_] rschema)
                  (attrs [_] {})
                  (init-max-eid [_] (reduce max c/e0 (map :e stored)))
                  (ea-first-datom [_ e a]
                    (swap! reads conj [:ea e a])
                    (some #(when (and (= e (:e %)) (= a (:a %))) %)
                          stored))
                  (fetch [_ datom]
                    (swap! reads conj [:eav (:e datom) (:a datom) (:v datom)])
                    (filter #(= datom %) stored))
                  (slice [_ index low high]
                    (assert (= :eav index))
                    (assert (= [(:e low) (:a low)] [(:e high) (:a high)]))
                    (filter #(and (= (:e low) (:e %)) (= (:a low) (:a %)))
                            stored))
                  (e-datoms [_ e] (filter #(= e (:e %)) stored))
                  (v-datoms [_ e]
                    (filter #(and (= e (:v %))
                                  (= :db.type/ref (get-in schema [(:a %) :db/valueType])))
                            stored))
                  (av-first-e [_ a v]
                    (swap! reads conj [:av a v])
                    (some #(when (and (= a (:a %)) (= v (:v %))) (:e %))
                          stored)))
        db      {:store   store
                 :max-eid (or cached-max-eid (reduce max c/e0 (map :e stored)))
                 :max-tx  c/tx0
                 :eavt    (TreeSortedSet. ^Comparator d/cmp-datoms-eavt)
                 :avet    (TreeSortedSet. ^Comparator d/cmp-datoms-avet)}
        report  (execute/execute-tx-loop
                  {:db-before db :db-after db :tx-data (or initial-tx-data [])
                   :tempids {}}
                  entities 0)]
    [report @reads]))

(deftest new-entity-skips-persisted-value-lookups
  (let [fields         (mapv #(keyword (str "field" %)) (range 10))
        schema         (assoc (zipmap fields (repeat {:db/noindex true}))
                              :ycsb/key {:db/unique :db.unique/value})
        entity         (assoc (zipmap fields (repeat "value"))
                              :db/id -1 :ycsb/key "new")
        [report reads] (execute-with-reads schema [] [entity])]
    (is (= 11 (count (:tx-data report))))
    (is (= 11 (count (get-in report [:db-after :eavt]))))
    (is (= [[1 :ycsb/key "new"]]
           (mapv d/datom-eav (get-in report [:db-after :avet]))))
    (is (= [[:av :ycsb/key "new"]] reads)
        "The unique key still checks other entities; no EAV reads are needed")))

(deftest new-entity-keeps-transaction-local-values
  (let [[report reads]
        (execute-with-reads
          {:value {:db/noindex true}
           :tags {:db/cardinality :db.cardinality/many :db/noindex true}} []
          [[:db/add -1 :value "first"]
           [:db/add -1 :value "second"]
           [:db/add -1 :value "second"]
           [:db/retract 1 :value "second"]
           [:db/add -1 :value "third"]
           [:db/add -1 :tags "a"]
           [:db/add -1 :tags "a"]
           [:db/add -1 :tags "b"]
           [:db/retract 1 :tags "a"]
           [:db/add -1 :tags "a"]])]
    (is (empty? reads))
    (is (empty? (get-in report [:db-after :avet])))
    (is (= [[1 :value "first" true]
            [1 :value "first" false]
            [1 :value "second" true]
            [1 :value "second" false]
            [1 :value "third" true]
            [1 :tags "a" true]
            [1 :tags "b" true]
            [1 :tags "a" false]
            [1 :tags "a" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
    (is (= #{[1 :value "third"] [1 :tags "a"] [1 :tags "b"]}
           (set (map d/datom-eav (get-in report [:db-after :eavt])))))))

(deftest redundant-adds-preserve-retract-and-readd-semantics
  (doseq [retract [[:db/retract 1 :value false]
                   [:db/retract 1 :value]
                   [:db/retractEntity 1]]
          add [[:db/add 1 :value false]
               [:db/cas 1 :value false false]]]
    (let [[report _]
          (execute-with-reads
            {:value {}} [(d/datom 1 :value false)]
            [add retract (with-meta add {:readded true}) add])]
      (is (= [[1 :value false false]
              [1 :value false true]
              [1 :value false true]]
             (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
      (is (= {:readded true} (meta (second (:tx-data report)))))
      (is (not (contains? report ::execute/tx-retracted)))))
  (testing "implicit replacement also records the old value"
    (doseq [add [[:db/add 1 :value "old"]
                 [:db/cas 1 :value "old" "old"]]]
      (let [[report _]
            (execute-with-reads
              {:value {}} [(d/datom 1 :value "old")]
              [[:db/add 1 :value "new"]
               [:db/add 1 :value "old"] add])]
        (is (= [["old" false] ["new" true] ["new" false]
                ["old" true] ["old" true]]
               (mapv (juxt :v d/datom-added) (:tx-data report))))))))

(deftest retraction-membership-distinguishes-entity-attribute-and-value
  (let [[report _]
        (execute-with-reads
          {:tags {:db/cardinality :db.cardinality/many}
           :other {}}
          [(d/datom 1 :tags "removed") (d/datom 1 :tags "kept")
           (d/datom 1 :other "removed") (d/datom 2 :tags "removed")]
          [[:db/retract 1 :tags "removed"]
           [:db/add 1 :tags "kept"]
           [:db/add 1 :other "removed"]
           [:db/add 2 :tags "removed"]
           [:db/add 1 :tags "removed"]
           [:db/add 1 :tags "removed"]])]
    (is (= [[1 :tags "removed" false]
            [1 :tags "removed" true]
            [1 :tags "removed" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))))

(deftest retraction-membership-is-local-to-each-attempt
  (testing "upsert retry discards the abandoned attempt's retractions"
    (doseq [prepare? [false true]]
      (binding [c/*use-prepare-path* prepare?]
        (let [attempt (atom 0)
              [report _]
              (execute-with-reads
                {:name {:db/unique :db.unique/identity} :value {}}
                [(d/datom 10 :name "existing") (d/datom 10 :value "old")]
                [[:db/add -1 :value "old"]
                 [:db.fn/call (fn [_]
                                (when (= 1 (swap! attempt inc))
                                  [[:db/retract 10 :value "old"]]))]
                 [:db/add 10 :value "old"]
                 [:db/add -1 :name "existing"]
                 [:db/add 10 :value "old"]])]
          (is (= 2 @attempt))
          (is (empty? (:tx-data report)))
          (is (not (contains? report ::execute/tx-retracted)))))))
  (testing "a supplied transaction prefix seeds the membership"
    (let [retracted (d/datom 1 :value 1 c/tx0 false)
          [report _] (execute-with-reads
                       {:value {}} [(d/datom 1 :value 1)]
                       [[:db/add 1 :value 1N]] nil [retracted])]
      (is (= [[1 false] [1 true]]
             (mapv (juxt :v d/datom-added) (:tx-data report)))))))

;; Count value equality checks without relying on wall-clock timing. Tree index
;; comparisons use compareTo, while the former retraction scan uses equals.
(deftype EqualityCountedValue [^long n comparisons]
  Object
  (equals [_ other]
    (swap! comparisons inc)
    (and (instance? EqualityCountedValue other)
         (== n (.-n ^EqualityCountedValue other))))
  (hashCode [_] (hash n))
  Comparable
  (compareTo [_ other] (Long/compare n (.-n ^EqualityCountedValue other))))

(deftest unchanged-values-do-not-rescan-transaction-history
  (doseq [op [:db/add :db/cas]]
    (let [n 256
          comparisons (atom 0)
          values (mapv #(EqualityCountedValue. % comparisons) (range n))
          value (peek values)
          changes (mapv #(vector :db/add 1 :value %) values)
          no-op (if (= op :db/add)
                  [:db/add 1 :value value]
                  [:db/cas 1 :value value value])
          [report _] (execute-with-reads
                       {:value {}} [] (into changes (repeat n no-op)))]
      (is (= (dec (* 2 n)) (count (:tx-data report))))
      (is (< @comparisons (* 8 n))
          (str op " compared values " @comparisons " times")))))

(deftest existing-entities-still-read-persisted-values
  (let [[report reads]
        (execute-with-reads
          {:value {} :tags {:db/cardinality :db.cardinality/many}}
          [(d/datom 5 :tags "a") (d/datom 10 :value "old")]
          [[:db/add 20 :value "new"]
           [:db/add 10 :value "updated"]
           [:db/add 5 :tags "a"]])]
    (is (= [[:ea 10 :value] [:eav 5 :tags "a"]] reads))
    (is (= [[20 :value "new" true]
            [10 :value "old" false]
            [10 :value "updated" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))))

(deftest stale-entity-boundary-still-reads-persisted-values
  (let [[report reads]
        (execute-with-reads
          {:value {} :tags {:db/cardinality :db.cardinality/many}}
          [(d/datom 5 :tags "a") (d/datom 10 :value "old")]
          [[:db/add 10 :value "updated"]
           [:db/add 5 :tags "a"]]
          0)]
    (is (= [[:ea 10 :value] [:eav 5 :tags "a"]] reads))
    (is (= [[10 :value "old" false] [10 :value "updated" true]]
           (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
    (is (= 0 (get-in report [:db-before :max-eid])))))

(deftest stale-entity-boundary-allocates-above-persisted-entities
  (let [[report reads]
        (execute-with-reads
          {:value {}}
          [(d/datom 10 :value "old")]
          [[:db/add -1 :value "new"]]
          0)]
    (is (empty? reads))
    (is (= 11 (get-in report [:tempids -1])))
    (is (= [[11 :value "new"]] (mapv d/datom-eav (:tx-data report))))))

(deftest entity-boundary-preserves-uncommitted-allocations
  (let [[report reads]
        (execute-with-reads
          {:value {}}
          [(d/datom 10 :value "old")]
          [[:db/add -1 :value "new"]]
          20)]
    (is (empty? reads))
    (is (= 21 (get-in report [:tempids -1])))
    (is (= 21 (get-in report [:db-after :max-eid])))))

(deftest new-tempid-can-upsert-an-existing-entity
  (doseq [prepare? [false true]]
    (testing (str "prepare path " prepare?)
      (binding [c/*use-prepare-path* prepare?]
        (let [[report reads]
              (execute-with-reads
                {:name {:db/unique :db.unique/identity} :value {}}
                [(d/datom 10 :name "existing") (d/datom 10 :value "old")]
                [[:db/add -1 :value "updated"]
                 [:db/add -1 :name "existing"]])]
          (is (= 10 (get-in report [:tempids -1])))
          (is (= [[10 :value "old" false]
                  [10 :value "updated" true]]
                 (mapv (juxt :e :a :v d/datom-added) (:tx-data report))))
          (is (= [[:ea 10 :value] [:ea 10 :name]]
                 (filterv #(= :ea (first %)) reads))))))))

(deftest new-entity-still-enforces-uniqueness
  (is (thrown-with-msg? clojure.lang.ExceptionInfo #"unique constraint"
        (execute-with-reads
          {:key {:db/unique :db.unique/value}}
          [(d/datom 10 :key "existing")]
          [[:db/add -1 :key "existing"]]))))

(deftest repeated-lookup-refs-read-the-identity-once
  (doseq [entity [[:key "existing"] :existing]]
    (let [fields (mapv #(keyword (str "field" %)) (range 10))
          [report reads]
          (execute-with-reads
            (assoc (zipmap fields (repeat {}))
                   :key {:db/unique :db.unique/identity}
                   :db/ident {:db/unique :db.unique/identity})
            [(d/datom 10 :key "existing") (d/datom 10 :db/ident :existing)]
            (mapv #(vector :db/add entity % "updated") fields))]
      (is (= 10 (count (:tx-data report))))
      (is (every? #(= 10 (:e %)) (:tx-data report)))
      (is (= 1 (count (filter #(= :av (first %)) reads)))))))

(deftest lookup-cache-preserves-overlays-and-attempt-boundaries
  (let [reads (atom [])
        store (reify i/IStore
                (rschema [_] {:db/unique #{:key :db/ident}})
                (av-first-e [_ attr value]
                  (swap! reads conj [attr value])
                  (when (= "stored" value) 1)))
        db {:store store :avet (TreeSortedSet. ^Comparator d/cmp-datoms-avet)}]
    (common/with-lookup-ref-cache db
      (dotimes [_ 2]
        (is (= 1 (common/entid db [:key "stored"])))
        (is (nil? (common/entid db [:key "new"]))))
      (is (= [[:key "stored"] [:key "new"]] @reads))
      ;; Cached storage hits and misses must both yield to the current overlay.
      (.add ^TreeSortedSet (:avet db) (d/datom 2 :key "stored"))
      (.add ^TreeSortedSet (:avet db) (d/datom 3 :key "new"))
      (is (= 2 (common/entid db [:key "stored"])))
      (is (= 3 (common/entid db [:key "new"])))
      (.clear ^TreeSortedSet (:avet db))
      (is (= 1 (common/entid db [:key "stored"])))
      (is (nil? (common/entid db [:key "new"])))
      (is (= 2 (count @reads)))
      (common/with-lookup-ref-cache db
        (is (= 1 (common/entid db [:key "stored"]))))
      (is (= 3 (count @reads))))
    (is (= 1 (common/entid db [:key "stored"])))
    (is (= 4 (count @reads)))
    (common/with-lookup-ref-cache db
      (is (= 1 (common/entid db [:key "stored"]))))
    (is (= 5 (count @reads)))))

(deftest lookup-cache-does-not-cross-stores-threads-or-mutable-values
  (let [reads (atom 0)
        store (fn [eid]
                (reify i/IStore
                  (rschema [_] {:db/unique #{:key}})
                  (av-first-e [_ _ _] (swap! reads inc) eid)))
        db {:store (store 1) :avet (TreeSortedSet. ^Comparator d/cmp-datoms-avet)}
        other (assoc db :store (store 2))]
    (common/with-lookup-ref-cache db
      (is (= 1 (common/entid db [:key "same"])))
      (is (= 2 (common/entid other [:key "same"])))
      (is (= 2 (common/entid other [:key "same"])))
      (is (= [1 1] @(future [(common/entid db [:key "same"])
                             (common/entid db [:key "same"])])))
      (is (= 5 @reads))
      (let [value (byte-array [1 2])]
        (is (= 1 (common/entid db [:key value])))
        (aset-byte value 0 (byte 3))
        (is (= 1 (common/entid db [:key value])))
        (is (= 7 @reads))))))
