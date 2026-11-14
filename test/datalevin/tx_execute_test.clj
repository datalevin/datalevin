(ns datalevin.tx-execute-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.datom :as datom]
            [datalevin.db.tx.common :as common]
            [datalevin.db.tx.compile :as compile]
            [datalevin.db.tx.execute :as execute]
            [datalevin.db.tx.bind :as binder]
            [datalevin.interface :as i]
            [datalevin.prepare :as prepare]))

(deftest patch-worker-consumes-owned-operands-without-publication-or-early-demand
  (let [events (atom [])
        ctx (prepare/->PrepareCtx {:doc {:db/valueType :db.type/idoc}} {} {})
        operations (lazy-seq
                     (swap! events conj :head)
                     (cons [:set [:n] 2]
                           (lazy-seq (swap! events conj :tail) nil)))
        source [:db.fn/patchIdoc 1 :doc operations]
        plan (compile/compile-patch-region ctx source 1 :doc operations)
        before {:n 1 :kept [1 2]}
        store (reify i/IStore
                (opts [_] (swap! events conj :options) {})
                (ea-first-datom [_ _ _] (throw (AssertionError. "Document reread"))))]
    (is (empty? @events))
    (let [addition (execute/prepare-patch-addition ctx (first (:entries plan)) before 91 store)]
      (is (= [:head :tail :options] @events))
      (is (= [1 :doc {:n 2 :kept [1 2]} 91 true]
             ((juxt :e :a :v :tx datom/datom-added) addition)))
      (is (= {:idoc/patch {:paths [[:n]]}} (meta addition)))
      (is (= {:n 1 :kept [1 2]} before))
      (is (identical? operations (nth (first (:entries plan)) 2))))))

(deftest caller-patch-binding-uses-compiler-eligibility-without-forcing-operations
  (doseq [[props opts accepted?]
          [[{:db/valueType :db.type/idoc} {} true]
           [{:db/valueType :db.type/string} {} false]
           [{:db/valueType :db.type/idoc :db/cardinality :db.cardinality/many} {} false]
           [{:db/valueType :db.type/idoc :db/unique :db.unique/value} {} false]
           [{:db/valueType :db.type/idoc :db.attr/preds ['valid-doc?]} {} false]
           [{:db/valueType :db.type/idoc} {:auto-entity-time? true} false]]]
    (let [ctx (prepare/->PrepareCtx {:doc props} {} opts)
          operations (lazy-seq (throw (AssertionError. "Early patch demand")))
          source (list :db.fn/patchIdoc (int 1) :doc operations)
          compiled (compile/compile-patch-region ctx source 1 :doc operations)
          bound (binder/prepare-patch-operands ctx (list source))]
      (is (= accepted? (some? compiled)))
      (is (= accepted? (some? bound)))
      (when accepted?
        (is (= :patch (:strategy bound)))
        (is (instance? Long (ffirst (:entries bound))))
        (is (identical? operations (nth (first (:entries bound)) 2)))
        (is (identical? source (nth (first (:entries bound)) 3)))))))

(deftest transition-emission-shares-equality-and-order-across-output-carriers
  (doseq [[old value expected]
          [[nil false [[false true]]]
           [false true [[false false] [true true]]]
           [0 1 [[0 false] [1 true]]]
           [1 (int 1) []]
           [1 1N []]
           [1.00M 1.0M []]
           [-0.0 0.0 []]
           [(byte-array [1 2]) (byte-array [1 2]) []]
           [{:nested [(byte-array [1 2])]} {:nested [(byte-array [1 2])]} []]
           [(byte-array [1]) (byte-array [2]) [[(byte-array [1]) false]
                                             [(byte-array [2]) true]]]]
          transient? [false true]]
    (let [prefix (datom/datom 9 :prefix "retained" 6)
          out (cond-> [prefix] transient? transient)
          result (common/emit-transition (if transient? conj! conj) out 1 :value old value 7)
          _ (when (empty? expected) (is (identical? out result)))
          result (cond-> result transient? persistent!)
          emitted (subvec result 1)]
      (is (identical? prefix (first result)))
      (is (= (count expected) (count emitted)))
      (is (= (mapv second expected) (mapv datom/datom-added emitted)))
      (is (every? #(= [1 :value 7 nil] ((juxt :e :a :tx meta) %)) emitted))
      (doseq [[[value _] actual] (map vector expected emitted)]
        (is (datom/value-equal? value (:v actual)))))))

(deftest transition-emission-retains-the-supplied-addition
  (let [annotations {:source :application :user/nested ["opaque"]}
        addition (with-meta (datom/datom 1 :value 2 91) annotations)]
    (doseq [old [nil 0]]
      (let [result (persistent! (common/emit-transition conj! (transient []) old addition))]
        (is (identical? addition (peek result)))
        (is (identical? annotations (meta (peek result))))
        (is (= (if (nil? old) [[2 91 true]] [[0 91 false] [2 91 true]])
               (mapv (juxt :v :tx datom/datom-added) result)))
        (when (some? old) (is (nil? (meta (first result)))))))
    (let [out []]
      (is (identical? out (common/emit-transition conj out 2 addition))))
    (let [result (common/emit-transition conj [] 1 :value 0 2 91 annotations)]
      (is (= [[0 91 false] [2 91 true]]
             (mapv (juxt :v :tx datom/datom-added) result)))
      (is (nil? (meta (first result))))
      (is (identical? annotations (meta (peek result)))))
    ;; No-op equality precedes addition construction and metadata application.
    (is (= [] (common/emit-transition conj [] 1 :value 2 2 91 annotations)))))

(deftest blind-preparation-normalizes-with-only-captured-context
  (let [calls (atom [])
        host (proxy [Object] [] (toString [] (swap! calls conj :correct) "normalized"))
        ctx (prepare/->PrepareCtx
              {:text {:db/aid 1 :db/valueType :db.type/string}
               :numbers {:db/aid 2 :db/valueType :db.type/long
                         :db/cardinality :db.cardinality/many}}
              {} {:validate-data? false})
        forms [(array-map :db/id -1 :text host :numbers [1.2 1.8 2.9])]
        kinds (binder/blind-entity-kinds forms nil)
        parts (binder/prepare-blind-operands ctx forms true true kinds)
        entity (first (:entries parts))]
    (is (= 1 kinds))
    (is (= -1 (.-tempid ^datalevin.db.tx.plan.BlindEntity entity)))
    (is (= [:text "normalized" :numbers 1 :numbers 2]
           (vec (.-attrs ^datalevin.db.tx.plan.BlindEntity entity))))
    (is (true? (:has-many? parts)))
    (is (false? (:has-unique? parts)))
    (is (not (contains? parts :ctx)))
    (is (= [:correct] @calls))
    (is (identical? host (:text (first forms))))
    (is (= [1.2 1.8 2.9] (:numbers (first forms))))))

(deftest blind-id-proof-precedes-normalization-and-retains-classified-input
  (let [input (lazy-seq (throw (AssertionError. "Demand after classified proof")))]
    (is (= 0 (binder/blind-entity-kinds input 0)))
    (is (= 1 (binder/blind-entity-kinds input 1))))
  (is (false? (binder/blind-entity-kinds [{:db/id nil :value 1}] nil)))
  (is (false? (binder/blind-entity-kinds [{:value 1} {:db/id 1 :value 2}] nil))))

(deftest scalar-preparation-owns-correction-and-accepts-associative-views
  (let [calls (atom [])
        host (proxy [Object] []
               (hashCode [] (swap! calls conj :hash) 0)
               (toString [] (swap! calls conj :correct) "normalized"))
        context (prepare/->PrepareCtx
                  {:text {:db/aid 1 :db/valueType :db.type/string}
                   :value {:db/aid 2 :db/valueType :db.type/long}}
                  {} {:validate-data? false})
        database {:max-eid 2 :store (reify i/IStore
                                     (ea-first-v [_ _ _]
                                       (throw (AssertionError. "Preparation read old values"))))}
        forms [{:db/id (int 1) :text host} [:db/add 2 :value 1.9]]
        entries (binder/prepare-scalar-operands database context forms false)]
    (is (= [[1 :text "normalized"] [2 :value 1]] entries))
    (is (every? #(instance? Long (first %)) entries))
    (is (= [:correct] @calls))
    (is (identical? host (:text (first forms))))
    (is (instance? Integer (:db/id (first forms))))
    (is (= 1.9 (nth (second forms) 3)))
    (doseq [head [[:db/add 1 :text host 123] [:db/retract 1 :text "old"]]]
      (let [input (lazy-seq
                    (cons head (lazy-seq (throw (AssertionError. "Demand after invalid form")))))]
        (is (nil? (binder/prepare-scalar-operands database context input false)))))
    (is (= [:correct] @calls))
    (is (nil? (binder/prepare-scalar-operands
                database (assoc-in context [:opts :auto-entity-time?] true)
                (lazy-seq (throw (AssertionError. "Input demand after context decline"))) false)))))

(deftest scalar-report-worker-preserves-the-adapter-attempt-and-both-layouts
  (doseq [offset [0 1] specialize? [false true]]
    (let [reads (atom [])
          store (reify i/IStore
                  (ea-first-v [_ e a] (swap! reads conj [e a]) (if (= e 1) 0 2)))
          database {:store store :max-eid 2 :max-tx 6}
          initial {:db-before database :db-after database :tx-data [] :tempids {}
                   :tx-meta {:source :scalar} :retained :adapter-owned}
          entries (if (zero? offset) [[1 :value 7] [2 :other 8]]
                      [[:db/add 1 :value 7] [:db/add 2 :other 8]])
          result (execute/execute-scalar-report initial entries nil 7 specialize? offset)]
      (is (identical? database (:db-before result)))
      (is (= {:source :scalar} (:tx-meta result)))
      (is (= :adapter-owned (:retained result)))
      (is (= {:db/current-tx 7} (:tempids result)))
      (is (= 7 (:max-tx (:db-after result))))
      (is (= 2 (:max-eid (:db-after result))))
      (is (= [[1 :value] [2 :other]] @reads))
      (is (= [[1 :value 0 7 false] [1 :value 7 7 true]
              [2 :other 2 7 false] [2 :other 8 7 true]]
             (mapv (juxt :e :a :v :tx datom/datom-added) (:tx-data result)))))))

(deftest scalar-report-worker-declines-prefixes-before-demand-or-reads
  (let [reads (atom [])
        database {:max-tx 6 :max-eid 2
                  :store (reify i/IStore (ea-first-v [_ _ _] (swap! reads conj :old) 0))}
        initial {:db-before database :db-after database :tx-data [] :tempids {}}
        entries (lazy-seq (throw (AssertionError. "Operand demand after prefix")))]
    (doseq [prefix [(assoc initial :tx-data [(datom/datom 1 :value 1 6)])
                    (assoc initial :tempids {"prefix" 2})]]
      (is (nil? (execute/execute-scalar-report prefix entries nil 7 true))))
    (is (empty? @reads))
    (is (= 6 (:max-tx database)))))
