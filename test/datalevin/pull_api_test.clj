(ns datalevin.pull-api-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.protocol :as p]
   [datalevin.pull-api :as pull]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u])
  (:import [java.nio ByteBuffer]
           [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- general-pull [db pattern id]
  ;; A visitor requires the general interpreter, giving an independent path
  ;; through the same public API for comparison with the scalar fast path.
  (d/pull db pattern id {:visitor (fn [& _])}))

(defn- encoded-pull
  ([db pattern id] (encoded-pull db pattern id nil))
  ([db pattern id opts]
   (let [buffer (ByteBuffer/allocate 65536)]
     (p/write-message-bf buffer {:result (pull/read-result db pattern id opts)})
     (:result (first (p/receive-one-message buffer))))))

(deftest scalar-pull-matches-general-interpreter
  (let [dir (u/tmp-dir (str "flat-pull-" (UUID/randomUUID)))
        fields [:z :a :m :c :q :b]
        schema (into {:key {:db/unique :db.unique/identity}}
                     (map #(vector % {}) fields))
        conn (d/create-conn dir schema)
        rows (mapv (fn [id]
                     (reduce-kv (fn [row index attr]
                                  (if (zero? (mod (+ (long id) (long index)) 3))
                                    row
                                    (assoc row attr
                                           (if (= attr :b) false [id attr]))))
                                {:db/id id :key (str id)} fields))
                   (range 1 9))]
    (try
      (d/transact! conn rows)
      (doseq [mask (range 64)
              with-id? [false true]
              id [1 2 8 99 [:key "3"] [:key "absent"]]]
        (let [pattern (cond-> (into [] (keep-indexed #(when (bit-test mask %1) %2))
                                     (reverse fields))
                        with-id? (conj :db/id))]
          (is (= (general-pull @conn pattern id) (d/pull @conn pattern id))
              (str pattern " " id))
          (is (= (general-pull @conn pattern id) (encoded-pull @conn pattern id))
              (str "encoded " pattern " " id))))
      (is (= (mapv #(general-pull @conn [:z :a :db/id] %) [1 2 99])
             (d/pull-many @conn [:z :a :db/id] [1 2 99])))
      (testing "transaction-local changes and giant values remain visible"
        (d/with-transaction [tx conn]
          (d/transact! tx [[:db/add 1 :z (.repeat "x" 3000)]
                          [:db/retract 1 :a (:a (first rows))]])
          (is (= {:z (.repeat "x" 3000) :db/id 1}
                 (d/pull @tx [:z :a :db/id] 1)))
          (is (= (general-pull @tx fields 1) (d/pull @tx fields 1)))
          (is (= (general-pull @tx fields 1) (encoded-pull @tx fields 1)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest pull-plan-respects-schema-options-and-deadlines
  (let [dir (u/tmp-dir (str "flat-pull-options-" (UUID/randomUUID)))
        conn (d/create-conn dir {:name {} :value {}
                                  :friend {:db/valueType :db.type/ref}
                                  :tags {:db/cardinality :db.cardinality/many}})]
    (try
      (d/transact! conn [{:db/id 1 :name "one" :value 1 :friend 2 :tags [:a :b]}
                        {:db/id 2 :name "two"}])
      (is (= {:value 1} (d/pull @conn [:value] 1)))
      (is (= {:value 1} (encoded-pull @conn [:value] 1)))
      (d/update-schema conn {:value {:db/cardinality :db.cardinality/many}})
      (d/transact! conn [[:db/add 1 :value 2]])
      (is (= {:value [1 2]} (d/pull @conn [:value] 1)))
      (is (= {:value [1 2]} (encoded-pull @conn [:value] 1)))
      (doseq [pattern ['[*] [:unknown] []
                       '[[:name :as :label]]
                       '[[:unknown :default false]]
                       '[[:name :xform count]]
                       '[:friend :tags]
                       '[{:friend [:name]}]
                       '[{:_friend [:name]}]
                       '[{:friend ...}]]]
        (is (= (general-pull @conn pattern 1) (d/pull @conn pattern 1))
            (str pattern))
        (is (= (general-pull @conn pattern 1) (encoded-pull @conn pattern 1))
            (str "encoded " pattern)))
      (let [visits (atom [])]
        (is (= {:name "one"}
               (d/pull @conn [:name :unknown] 1
                       {:visitor #(swap! visits conj [%1 %2 %3 %4])})))
        (is (= #{:name :unknown} (set (map #(nth % 2) @visits)))))
      (is (thrown? clojure.lang.ExceptionInfo
                   (d/pull @conn [:name] 1 {:timeout -1})))
      ;; A pull without a local timeout currently clears an outer deadline.
      ;; Preserve that behavior and restore the caller's binding afterward.
      (binding [timeout/*deadline* 1]
        (is (= {:name "one"} (d/pull @conn [:name] 1)))
        (is (= 1 timeout/*deadline*)))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
