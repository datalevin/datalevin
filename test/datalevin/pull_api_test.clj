(ns datalevin.pull-api-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.interface :as i]
   [datalevin.protocol :as p]
   [datalevin.pull-api :as pull]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u])
  (:import [datalevin.db DB]
           [java.nio ByteBuffer]
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

(deftest cursor-pull-many-preserves-pull-semantics
  (let [conn (d/create-conn nil
                           {:key {:db/unique :db.unique/identity}
                            :a {} :z {} :flag {} :absent {}
                            :friend {:db/valueType :db.type/ref}
                            :tags {:db/cardinality :db.cardinality/many}}
                           {:kv-opts {:inmemory? true}})
        ids [3 1 [:key "two"] 99 [:key "absent"] 1]
        patterns [[:z :a] [:flag :absent] [:db/id :a :z] [] [:db/id]
                  [:unknown] '[*] '[:tags {:friend [:a]}]
                  '[[:absent :default false]]]]
    (try
      (d/transact! conn [{:db/id 1 :key "one" :a 10 :z (.repeat "x" 3000)
                         :flag false :friend 2 :tags [:a :b]}
                        {:db/id 2 :key "two" :a 20}
                        {:db/id 3 :key "three" :z [1 2 3]}])
      (doseq [pattern patterns]
        (is (= (mapv #(general-pull @conn pattern %) ids)
               (d/pull-many @conn pattern ids)) (str pattern))
        (is (= [] (d/pull-many @conn pattern []))))
      (let [pending (:db-after (d/tx-data->simulated-report
                                @conn [[:db/retractEntity 2]
                                       [:db/add 1 :a 11]]))]
        (doseq [pattern patterns]
          (is (= (mapv #(general-pull pending pattern %) ids)
                 (d/pull-many pending pattern ids)) (str "pending " pattern))))
      (finally (d/close conn)))))

(deftest pull-patterns-survive-data-transactions
  (doseq [wal? [false true]]
    (testing (str "WAL enabled: " wal?)
      (let [dir (u/tmp-dir (str "pull-cache-" (random-uuid)))
            conn (d/create-conn dir {:name {} :value {}}
                                {:kv-opts {:wal? wal?}})]
        (try
          (d/transact! conn [{:db/id 1 :name "one" :value 0}])
          (let [^DB before @conn
                cache (.-pull-patterns before)
                parsed (pull/parse-opts before [:name :value])
                inside-plan (volatile! nil)]
            (doseq [value (range 1 4)]
              (d/transact! conn [[:db/add 1 :value value]])
              (is (= {:name "one" :value value}
                     (d/pull @conn [:name :value] 1)))
              (let [current (pull/parse-opts @conn [:name :value])]
                (is (identical? (:pattern parsed) (:pattern current)))
                (is (identical? (:flat parsed) (:flat current)))))
            (d/with-transaction [tx conn]
              (d/transact! tx [[:db/add 1 :value 4]])
              (is (= {:name "one" :value 4}
                     (d/pull @tx [:name :value] 1)))
              (vreset! inside-plan (pull/parse-opts @tx [:value]))
              (d/with-transaction [nested tx]
                (d/transact! nested [[:db/add 1 :value 5]])))
            (is (identical? cache (.-pull-patterns ^DB @conn)))
            (is (identical? (:pattern @inside-plan)
                            (:pattern (pull/parse-opts @conn [:value]))))
            (is (identical? (:pattern parsed)
                            (:pattern (pull/parse-opts @conn [:name :value]))))
            (let [visits (atom [])]
              (is (= {:name "one" :value 5}
                     (d/pull @conn [:name :value] 1
                             {:visitor #(swap! visits conj [%1 %2 %3 %4])})))
              (is (= #{:name :value} (set (map #(nth % 2) @visits)))))
            (is (not (identical? (.-eavt before) (.-eavt ^DB @conn))))
            (is (not (identical? (.-avet before) (.-avet ^DB @conn)))))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

(deftest shared-pull-cache-rechecks-schema-after-commit-and-abort
  (let [dir (u/tmp-dir (str "pull-cache-schema-" (random-uuid)))
        conn (d/create-conn dir {:value {}})]
    (try
      (d/transact! conn [{:db/id 1 :value 1}])
      (let [cache (.-pull-patterns ^DB @conn)
            original (pull/parse-opts @conn [:value])
            aborted (volatile! nil)
            committed (volatile! nil)]
        (is (some? (:flat original)))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"abort schema change"
              (d/with-transaction [tx conn]
                (d/update-schema tx {:value {:db/cardinality :db.cardinality/many}})
                (d/transact! tx [[:db/add 1 :value 2]])
                (is (= {:value [1 2]} (d/pull @tx [:value] 1)))
                (vreset! aborted (pull/parse-opts @tx [:value]))
                (throw (ex-info "abort schema change" {})))))
        (is (= {:value 1} (d/pull @conn [:value] 1)))
        (is (not (identical? (:pattern @aborted)
                            (:pattern (pull/parse-opts @conn [:value])))))
        (is (some? (:flat (pull/parse-opts @conn [:value]))))
        (d/with-transaction [tx conn]
          (d/update-schema tx {:value {:db/cardinality :db.cardinality/many}})
          (d/transact! tx [[:db/add 1 :value 3]])
          (vreset! committed (pull/parse-opts @tx [:value]))
          (is (nil? (:flat @committed))))
        (is (identical? cache (.-pull-patterns ^DB @conn)))
        (is (identical? (:pattern @committed)
                        (:pattern (pull/parse-opts @conn [:value]))))
        (is (= {:value [1 3]} (d/pull @conn [:value] 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest fill-db-retains-pull-patterns
  (let [dir (u/tmp-dir (str "pull-cache-fill-" (random-uuid)))
        initial (d/empty-db dir {:value {}})]
    (try
      (let [before (d/fill-db initial [(d/datom 1 :value 1)])
            parsed (pull/parse-opts before [:value])
            after (d/fill-db before [(d/datom 2 :value 2)])]
        (is (identical? (.-pull-patterns ^DB before)
                        (.-pull-patterns ^DB after)))
        (is (identical? (:pattern parsed)
                        (:pattern (pull/parse-opts after [:value]))))
        (is (= {:value 2} (d/pull after [:value] 2)))
        (is (= 2 (:max-eid after)))
        (is (= after (get @db/dbs (i/db-name (:store after))))))
      (finally
        (d/close-db initial)
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
