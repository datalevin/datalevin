(ns datalevin.storage.fulltext-plan-test
  (:require [clojure.set :as set]
            [clojure.test :refer [deftest is use-fixtures]]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [datalevin.analyzer :as analyzer]
            [datalevin.built-ins :as bi]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.datom :as datom]
            [datalevin.interface :as i]
            [datalevin.storage :as storage]
            [datalevin.storage.indexing :as indexing]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.tx-group.batch.embedded :as embedded]
            [datalevin.tx-group.compat :as group]
            [datalevin.udf :as udf])
  (:import [datalevin.search SearchEngine]
           [datalevin.storage Store]
           [java.util.concurrent.atomic AtomicInteger]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each db-fixture)

(def ^:private schema
  {:text {:db/valueType :db.type/string :db/fulltext true}
   :ordinary {:db/valueType :db.type/long}})

(defn- options [calls]
  (let [document {:udf/lang :test :udf/kind :analyzer :udf/id :counted-fulltext}
        query {:udf/lang :test :udf/kind :query-analyzer :udf/id :fulltext-query}
        registry (doto (udf/create-registry)
                   (udf/register! document
                                  (fn [text] (swap! calls conj text) (analyzer/en-analyzer text)))
                   (udf/register! query analyzer/en-analyzer))]
    {:runtime-opts {:udf-registry registry}
     :search-opts {:analyzer document :query-analyzer query}
     :validate-data? true :background-sampling? false :snapshot-scheduler? false}))

(defn- engine
  ([connection] (engine connection c/default-domain))
  ([connection domain]
   (get (.-search-engines ^Store (:store @connection)) domain)))

(defn- snapshot [connection text]
  (let [^SearchEngine engine (engine connection)
        aid (get-in (i/schema (:store @connection)) [:text :db/aid])]
    {:max-doc (.get ^AtomicInteger (.-max-doc engine))
     :doc-id (i/doc-indexed? engine [1 aid text])
     :count (i/doc-count engine)}))

(defn- report-values [report]
  (mapv (juxt :v datom/datom-added) (:tx-data report)))

(deftest restoring-text-retains-document-id-without-tokenization
  (doseq [wal? [false true] positions? [false true] collected? [false true]]
    (binding [embedded/*defer-attachment?* (not collected?) group/*enabled?* collected?]
      (let [calls (atom [])]
        (with-open [connection (d/create-conn nil schema
                                              (-> (options calls) (assoc :wal? wal?)
                                                  (assoc-in [:search-opts :index-position?] positions?)))]
          (d/transact! connection [{:db/id 1 :text "original text" :ordinary 1}])
          (let [before (snapshot connection "original text")]
            (reset! calls [])
            (let [report (d/transact! connection
                                      [[:db/add 1 :text "intermediate text"]
                                       [:db/add 1 :text "original text"]]
                                      {:segment :restore})]
              (is (= [["original text" false] ["intermediate text" true]
                      ["intermediate text" false] ["original text" true]]
                     (report-values report)))
              (is (= {:segment :restore} (:tx-meta report)))
              (is (empty? @calls))
              (is (= before (snapshot connection "original text")))
              (is (= #{[1 :text "original text"]} (set (map vec (bi/fulltext @connection "original")))))
              (is (empty? (bi/fulltext @connection "intermediate"))))
            (is (thrown? Exception
                         (d/transact! connection [[:db/add 1 :text "cancelled"]
                                                  [:db/cas 1 :ordinary 9 2]
                                                  [:db/add 1 :text "original text"]])))
            (is (thrown? Exception
                         (d/transact! connection [[:db/add 1 :text 42]
                                                  [:db/add 1 :text "original text"]])))
            (is (empty? @calls))
            (is (= before (snapshot connection "original text")))))))))

(deftest overwrites-and-cardinality-many-cancel-only-matching-references
  (let [calls (atom [])]
    (with-open [connection (d/create-conn nil
                                          (assoc-in schema [:text :db/cardinality] :db.cardinality/many)
                                          (options calls))]
      (d/transact! connection [{:db/id 1 :text ["original text" "kept text"]}])
      (let [before (snapshot connection "original text")]
        (reset! calls [])
        (let [report (d/transact! connection
                                  [[:db/retract 1 :text "original text"]
                                   [:db/add 1 :text "discarded text"]
                                   [:db/retract 1 :text "discarded text"]
                                   [:db/add 1 :text "original text"]
                                   [:db/add 2 :text "original text"]])]
          (is (= 5 (count (:tx-data report))))
          (is (= ["original text"] @calls))
          (is (= (:doc-id before) (:doc-id (snapshot connection "original text"))))
          (is (= 3 (i/doc-count (engine connection))))
          (is (= #{[1 :text "original text"] [2 :text "original text"]}
                 (set (map vec (bi/fulltext @connection "original")))))
          (is (empty? (bi/fulltext @connection "discarded"))))))))

(deftest giant-restoration-retains-required-physical-reference-updates
  (doseq [wal? [false true]]
    (let [calls (atom []) original (str "original " (.repeat "padding " 300))
          intermediate (str "intermediate " (.repeat "padding " 300))]
      (with-open [connection (d/create-conn nil schema (assoc (options calls) :wal? wal?))]
        (d/transact! connection [{:db/id 1 :text original}])
        (reset! calls [])
        (let [report (d/transact! connection
                                  [[:db/add 1 :text intermediate]
                                   [:db/add 1 :text original]])]
          (is (= [[original false] [intermediate true] [intermediate false] [original true]]
                 (report-values report)))
          (is (= [original] @calls))
          (is (= 1 (i/doc-count (engine connection))))
          (is (= #{[1 :text original]} (set (map vec (bi/fulltext @connection "original")))))
          (is (empty? (bi/fulltext @connection "intermediate"))))))))

(deftest cancellation-precedes-search-batch-splitting-and-handles-empty-text
  (let [calls (atom [])]
    (with-open [connection (d/create-conn nil schema (options calls))]
      (d/transact! connection [{:db/id 1 :text "original text"}])
      (let [before (snapshot connection "original text")
            forms (conj (mapv #(vector :db/add 1 :text (str "transient text " %))
                              (range 600))
                        [:db/add 1 :text "original text"])]
        (reset! calls [])
        (is (= 1202 (count (:tx-data (d/transact! connection forms)))))
        (is (empty? @calls))
        (is (= before (snapshot connection "original text")))
        (d/transact! connection [[:db/add 1 :text ""] [:db/add 1 :text "original text"]])
        (d/transact! connection [[:db/add 2 :text "temporary text"]
                                [:db/retract 2 :text "temporary text"]])
        (is (empty? @calls))
        (is (= before (snapshot connection "original text")))))))

(deftest explicit-requests-remain-separate-visible-segments
  (let [calls (atom [])]
    (with-open [connection (d/create-conn nil schema (options calls))]
      (d/transact! connection [{:db/id 1 :text "original text"}])
      (reset! calls [])
      (d/with-transaction [tx connection]
        (d/transact! tx [[:db/add 1 :text "intermediate text"]])
        (is (= ["intermediate text"] @calls))
        (is (= #{[1 :text "intermediate text"]} (set (map vec (bi/fulltext @tx "intermediate")))))
        (d/transact! tx [[:db/add 1 :text "original text"]])
        (is (= ["intermediate text" "original text"] @calls)))
      (is (= #{[1 :text "original text"]} (set (map vec (bi/fulltext @connection "original"))))))))

(deftest asynchronous-domains-omit-cancelled-jobs-before-ordinal-assignment
  (let [calls (atom [])
        base (options calls)
        opts (assoc base :search-domains
                    (zipmap ["first" "second"]
                            (repeat (assoc (:search-opts base) :indexing-mode :async))))]
    (with-open [connection (d/create-conn nil
                                          (assoc-in schema [:text :db.fulltext/domains] ["first" "second"])
                                          opts)]
      (d/transact! connection [{:db/id 1 :text "original text"}])
      (is (:caught-up? (d/wait-for-secondary-index connection {:timeout-ms 10000})))
      (let [before (count (storage/secondary-index-jobs (:store @connection)))]
        (reset! calls [])
        (d/transact! connection [[:db/add 1 :text "intermediate text"]
                                [:db/add 1 :text "original text"]])
        (is (= before (count (storage/secondary-index-jobs (:store @connection)))))
        (is (empty? @calls))
        (let [report (d/transact! connection
                                  [[:db/add 1 :text "discarded text"]
                                   [:db/add 1 :text "final text"]])
              jobs (->> (storage/secondary-index-jobs (:store @connection))
                        (filter #(= (:max-tx (:db-after report)) (:job/tx %)))
                        (sort-by :job/ordinal))]
          (is (= [:delete :delete :add :add] (mapv :job/op jobs)))
          (is (= [0 1 2 3] (mapv :job/ordinal jobs)))
          (is (= #{"original text" "final text"} (set (map :job/value jobs))))
          (is (= {"first" 2 "second" 2} (frequencies (map :job/domain jobs))))
          (is (:caught-up? (d/wait-for-secondary-index connection {:timeout-ms 10000})))
          (is (= ["final text" "final text"] @calls))
          (is (= 1 (i/doc-count (engine connection "first"))))
          (is (= 1 (i/doc-count (engine connection "second")))))))))

(deftest unchecked-bulk-input-retains-its-addition-before-deletion
  (let [calls (atom [])]
    (with-open [connection (d/create-conn nil schema (options calls))]
      (d/transact! connection [{:db/id 1 :text "original text"}])
      (reset! calls [])
      (let [addition (datom/datom 1 :text "original text")
            store (:store @connection)
            aid (get-in (i/schema store) [:text :db/aid])]
        (i/load-datoms store [addition (datom/delete addition)])
        (is (= ["original text"] @calls))
        (is (nil? (i/doc-indexed? (engine connection) [1 aid "original text"])))
        (is (empty? (i/e-datoms store 1)))))))

(deftest repeated-same-kind-operations-retain-downstream-validation
  (doseq [kinds [[:add :add :delete] [:delete :delete :add]
                 [:add :add] [:delete :delete]]]
    (let [operations (FastList/newList
                       ^java.util.Collection
                       (mapv #(object-array [% [1 2 "text"] "text"]) kinds))]
      (is (identical? operations
                      (indexing/coalesce-fulltext-operations
                        operations #(aget ^objects % 0) #(aget ^objects % 1)))))))

(deftest resolved-coalescing-agrees-with-independent-set-transitions
  (let [refs (vec (for [domain ["one" "two"] e [1 2] text ["alpha" "beta"]]
                    [domain e text]))
        result
        (tc/quick-check
         300
         (prop/for-all [initial (gen/set (gen/elements refs))
                        commands (gen/vector (gen/tuple gen/boolean (gen/elements refs)) 0 100)]
                       (let [[final ops]
                             (reduce (fn [[present ops] [add? ref]]
                                       (if (= add? (contains? present ref))
                                         [present ops]
                                         [(if add? (conj present ref) (disj present ref))
                                          (conj ops (object-array [(if add? :add :delete) ref nil]))]))
                                     [initial []] commands)
                             compacted (indexing/coalesce-fulltext-operations
                                        (FastList/newList ^java.util.Collection ops)
                                        #(aget ^objects % 0) #(aget ^objects % 1))
                             replayed (reduce (fn [present ^objects op]
                                                (if (= :add (aget op 0))
                                                  (conj present (aget op 1))
                                                  (disj present (aget op 1)))) initial compacted)
                             changed (count (set/union (set/difference initial final)
                                                       (set/difference final initial)))]
                         (and (= final replayed) (= changed (.size ^FastList compacted)))))
         :seed 418061)]
    (is (:pass? result) (pr-str result))))
