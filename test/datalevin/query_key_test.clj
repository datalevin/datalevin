(ns datalevin.query-key-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [datalevin.built-ins :as built-ins]
            [datalevin.core :as d]
            [datalevin.query :as query]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source-test :refer [row-info]]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value]
            [datalevin.rules.relation :as rule-relation]
            [datalevin.relation :as relation]
            [datalevin.util :as u])
  (:import [datalevin.utl QueryKey QueryKey$KeyMap QueryKey$KeySet]
           [java.io ByteArrayInputStream ObjectInputStream]
           [java.util AbstractList Arrays Base64 Map Objects Random Set]
           [org.eclipse.collections.impl.list.mutable FastList]))

(deftest binary-keys-share-equality-hash-and-lookup
  (doseq [x [(byte-array 0) (byte-array [0 -1 2])
             [(byte-array [1]) 1.0M nil]
             (list (byte-array [1]) :end)
             {:value [(byte-array [1 2])]}
             (hash-map (byte-array [1]) :a (byte-array [1]) :b)
             (hash-set (byte-array [1]) (byte-array [1]) (byte-array [2]))
             {:nested #{[(byte-array [2])] [(byte-array [3])]}}
             (with-meta [(byte-array [3])] {:ignored (Object.)})]
          :let [y (if (meta x) (with-meta (value/owned-copy (with-meta x nil)) nil)
                      (value/owned-copy x))
                m (QueryKey$KeyMap.)
                s (QueryKey$KeySet.)]]
    (is (QueryKey/equals x y))
    (is (QueryKey/equals y x))
    (is (QueryKey/equiv x y))
    (is (= (QueryKey/hash x) (QueryKey/hash y)))
    (.put m x :found)
    (.add s x)
    (is (= :found (.get m y)))
    (is (not (.add s y)))
    (let [stored (relation/wrap-array (object-array [x]))
          lookup (relation/reset-array-lookup! (relation/array-lookup) (object-array [y]))]
      (is (= stored lookup))
      (is (= lookup stored))
      (is (= (hash stored) (hash lookup))))))

(deftest collisions-and-unordered-multiplicity
  (let [a (byte-array [0 31]) b (byte-array [1 0])
        m (QueryKey$KeyMap.)]
    (is (= (Arrays/hashCode a) (Arrays/hashCode b)))
    (is (not (QueryKey/equals a b)))
    (.put m a :a)
    (.put m b :b)
    (is (= :a (.get m (aclone a))))
    (is (= :b (.get m (aclone b)))))
  (let [a (hash-set (byte-array [1]) (byte-array [1]))
        b #{(byte-array [1]) (byte-array [2])}]
    (is (not (QueryKey/equals a b)))
    (is (not (QueryKey/equals b a))))
  (is (not (QueryKey/equals (hash-map (byte-array [1]) :a (byte-array [1]) :a)
                           (hash-map (byte-array [1]) :a (byte-array [1]) :b)))))

(deftest nonbinary-semantics-and-predicate-numeric-equivalence
  (let [values [nil true false 1 (int 1) 1N 1.0 1.0M 1.00M -0.0 0.0 Double/NaN Float/NaN Double/POSITIVE_INFINITY
                :a 'a "a" [1.0M] [1.00M] '(1 2) [1 2]
                {:a 1.0M} {:a 1.00M} #{1 2} (sorted-set 1 2)]]
    (doseq [x values]
      (is (= (Objects/hashCode x) (QueryKey/hash x))))
    (doseq [x values y values]
      (is (= (Objects/equals x y) (QueryKey/equals x y)))
      (is (= (= x y) (QueryKey/equiv x y)))))
  (doseq [wrap [#(vector (byte-array [1]) %)
                #(hash-map :bytes (byte-array [1]) :n %)
                #(hash-set [(byte-array [1]) %])]]
    (is (not (QueryKey/equals (wrap 1.0M) (wrap 1.00M))))
    (is (built-ins/= (wrap 1.0M) (wrap 1.00M)))
    (is (not (built-ins/not= (wrap 1.0M) (wrap 1.00M))))))

(deftest ordered-keys-preserve-container-and-numeric-equality
  (let [leaves [nil (int 1) 1 1N 1.0 1.0M 1.00M -0.0 0.0
                {:n 1.0M} {:n 1.00M} #{1} #{1N}]
        containers [vector list #(subvec [:prefix % :suffix] 1 2)
                    #(vector (list %)) #(list (vector %))]]
    (doseq [wrap-left containers wrap-right containers x leaves y leaves
            :let [left (wrap-left x) right (wrap-right y)]]
      (is (= (Objects/equals left right) (QueryKey/equals left right)))
      (is (= (= left right) (QueryKey/equiv left right))))))

(deftest tuple-byte-inspection-keeps-nested-and-opaque-values
  (let [tuples (FastList. [(object-array [1 "ordinary"])
                          (object-array [2 {:nested [(byte-array [3])]}])])]
    (is (QueryKey/tuplesContainBytes tuples))
    (is (not (QueryKey/tuplesContainBytes
               (FastList. [(object-array [1 [2 {:n 3}]])]))))))

(deftest unordered-key-workspace-is-bounded-and-released
  (let [x (into {} (map #(vector % (byte-array [1 2]))) (range 100))
        y (value/owned-copy x)
        captured (atom nil)]
    (is (thrown? clojure.lang.ExceptionInfo
          (rt/with-runtime {:limits {:retained-bytes 4096}}
            (fn [runtime]
              (reset! captured (:control runtime))
              (QueryKey/equals x y)))))
    (is (every? zero? (vals (control/usage @captured))))
    (is (QueryKey/equals x y) "The per-query budget is restored after failure")
    (is (thrown? clojure.lang.ExceptionInfo
          (rt/with-runtime {}
            (fn [{:keys [control]}]
              (is (QueryKey/equals x y))
              (is (every? zero? (vals (control/usage control))))
              (control/cancel! control {:error :query-source/cancelled})
              (QueryKey/hash x)))))
    (is (QueryKey/equals x y))))

(deftest key-budgets-remain-thread-local-through-nested-runtime-exits
  (let [ready (promise)
        proceed (promise)
        hash-error (promise)
        worker (future
                 (try
                   (rt/with-runtime {}
                     (fn [{:keys [control]}]
                       (control/cancel! control {:error :query-source/cancelled})
                       (deliver ready true)
                       @proceed
                       (deliver hash-error
                         (try (QueryKey/hash [1 2])
                              (catch clojure.lang.ExceptionInfo e
                                (:error (ex-data e)))))))
                   (catch clojure.lang.ExceptionInfo e
                     (:error (ex-data e)))))]
    (try
      @ready
      (rt/with-runtime {}
        (fn [_]
          (rt/with-runtime {} (fn [_] (is (QueryKey/equals [1] '(1)))))
          (is (QueryKey/equals [1] '(1)))))
      ;; Clearing this thread's nested budgets must not disable the worker's
      ;; cancellation check, or apply its cancelled budget to this thread.
      (is (= (Objects/hashCode [1 2]) (QueryKey/hash [1 2])))
      (finally (deliver proceed true)))
    (is (= :query-source/cancelled @worker))
    (is (= :query-source/cancelled @hash-error))))

(deftest custom-collection-equality-remains-opaque
  (let [make-value (fn [data]
                     (proxy [AbstractList] []
                       (get [_] data)
                       (size [] 1)
                       (equals [other] (= (class this) (class other)))
                       (hashCode [] 42)))
        left (make-value (byte-array [1]))
        right (make-value (byte-array [2]))
        type {:kind :custom :name :test/opaque-list :version 1 :fingerprint "v1"}]
    (extend (class left) value/IQuerySourceValue
      {:-logical-type (fn [_] type) :-retained-bytes (fn [_] 256)})
    (is (value/matches-type? type left))
    (is (identical? left (value/owned-copy left)))
    (is (QueryKey/equals left right))
    (is (QueryKey/equiv left right))
    (is (= 42 (QueryKey/hash left)))
    (is (= (Objects/hashCode [left]) (QueryKey/hash [left])))
    (let [rows [[1 left]]
          source (fake/source (assoc-in row-info [:pattern-contract :attributes :row/value :value-type] type) (fake/row-facts rows))
          q '[:find ?id :in $s ?value :where [$s ?id :row/value ?value]]]
      (is (= #{[1]} (d/q q (fake/row-facts rows) right) (d/q q source right))))
    (let [index (QueryKey$KeyMap.)]
      (.put index left :found)
      (is (= :found (.get index right))))))

(deftest key-map-retains-java-map-iteration
  (let [m (QueryKey$KeyMap.)]
    (.put m :first 1)
    (.put m :second 2)
    (is (= {:first 1 :second 2} (into {} m)))
    (is (= #{[:first 1] [:second 2]} (set (for [[k v] m] [k v]))))
    (is (= #{:first :second} (set (keys m))))
    (is (= #{1 2} (set (vals m))))))

(deftest serialized-key-containers-retain-strategy-compatibility
  ;; Recorded before the class cache was introduced. Adding helper classes
  ;; must not change the serialized names of the existing hashing strategies.
  (let [fixtures (edn/read-string
                   (slurp (io/resource "data/query-key-serialization.edn")))
        key [:binary (byte-array [1 2])]]
    (doseq [[kind encoded] fixtures]
      (with-open [input (ObjectInputStream.
                          (ByteArrayInputStream.
                            (.decode (Base64/getDecoder) ^String encoded)))]
        (let [container (.readObject input)]
          (if (= kind :map)
            (is (= :found (.get ^Map container key)))
            (is (.contains ^Set container key))))))))

(deftest typed-native-probes-preserve-bound-key-types
  (let [dir (u/tmp-dir (str "typed-key-probes-" (random-uuid)))
        conn (d/create-conn dir
               {:item/value {:db/valueType :db.type/long}
                :item/ref {:db/valueType :db.type/ref}
                :item/flag {:db/valueType :db.type/boolean}}
               {:kv-opts {:wal? false}})
        entities (vec (range 1 1025))]
    (try
      (d/transact! conn (mapv (fn [e] {:db/id e :item/value e
                                      :item/ref (inc (mod e 1024))
                                      :item/flag (even? e)}) entities))
      (binding [query/*cache?* false]
        (doseq [attr [:item/value :item/ref :item/flag]
                size [255 256 1024]
                :let [facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                                  (d/datoms @conn :ave attr))
                      q {:find '[?e ?v] :in '[$ [?e ...]]
                         :where [['?e attr '?v]]}
                      domain (subvec entities 0 size)]]
          (is (= (d/q q facts domain) (d/q q @conn domain))))
        (doseq [[attr values] [[:item/value [1 (int 1) 1N 1024 1025]]
                              [:item/ref [1 (int 1) 1N 1024 1025]]
                              [:item/flag [false true]]]
                value values
                :let [facts (mapv (fn [datom] [(:e datom) (:a datom) (:v datom)])
                                  (d/datoms @conn :ave attr))
                      scalar {:find '[?e] :in '[$ [?e ...] ?v]
                              :where [['?e attr '?v]]}
                      multiple {:find '[?e ?tag] :in '[$ [[?v ?tag]]]
                                :where [['?e attr '?v]]}
                      input [[value :first] [value :second]]]]
          (is (= (d/q scalar facts entities value)
                 (d/q scalar @conn entities value)))
          (is (= (d/q multiple facts input)
                 (d/q multiple @conn input)))))
      (finally (d/close conn) (u/delete-files dir)))))

(deftest rule-constants-use-byte-aware-equality-at-every-head-position
  (let [a (byte-array [1 2]) b (byte-array [1 2])
        body (relation/relation! {'?a 0 '?b 1 '?c 2} (FastList. [(object-array [42 :tag a])]))
        result (rule-relation/map-rule-result body ['?a '?b '?c] [42 '?x b])]
    (is (= [[:tag]] (mapv vec (:tuples result))))))

(deftest generated-rule-arguments-agree-with-eager-byte-and-numeric-unification
  (let [random (Random. 142893)
        bank [nil 1 1N 2 :tag "value" (byte-array [1]) (byte-array [1]) (byte-array [2])
              [1 (byte-array [1])] [1N (byte-array [1])] {:key (byte-array [2])}]
        choose #(nth bank (.nextInt random (count bank)))
        head ['?a '?b '?c '?d]
        var? #(and (symbol? %) (.startsWith (name %) "?"))]
    (dotimes [_ 100]
      (let [input (repeatedly 12 #(vec (repeatedly 4 choose)))
            input (vec input)
            args (mapv (fn [i]
                         (if (.nextBoolean random) (nth ['?x '?y '?x '?z] i)
                             (value/owned-copy (nth (first input) i)))) (range 4))
            variables (vec (distinct (filter var? args)))
            expected
            (reduce (fn [output row]
                      (let [bound (reduce (fn [bound [argument x]]
                                            (when bound
                                              (if (var? argument)
                                                (if (contains? bound argument)
                                                  (when (QueryKey/equiv (get bound argument) x) bound)
                                                  (assoc bound argument x))
                                                (when (QueryKey/equiv argument x) bound))))
                                          {} (map vector args row))]
                        (if bound
                          (let [row (mapv bound variables)]
                            (if (some #(QueryKey/equals % row) output) output (conj output row))) output)))
                    [] input)
            body (relation/relation! (zipmap head (range)) (FastList. (mapv object-array input)))
            result (rule-relation/map-rule-result body head args)
            actual (mapv (fn [tuple] (mapv #(aget ^objects tuple (long (get (:attrs result) %))) variables))
                         (:tuples result))]
        (is (QueryKey/equals expected actual))))))
