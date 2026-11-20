(ns ^:no-doc datalevin.query.source.grouped
  "Admission and column layout for independent grouped source-query relations."
  (:require [datalevin.parser :as parser]
            [datalevin.query.source.context :as context]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.expression :as expression]
            [datalevin.query.source.input-program :as input-program]
            [datalevin.query.source.normalize :as normalize]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :refer [fail!]])
  (:import [java.math MathContext RoundingMode]
           [datalevin.parser Aggregate BindColl BindTuple Constant
            DefaultSrc FindRel Function Pattern Predicate SrcVar Variable]))

(defn call? [node]
  (and (instance? Function node) (= 'q (get-in node [:fn :symbol]))))

(defn- require-shape! [valid?]
  (when-not valid? (fail! :query-source/unsupported :planning)))

(defn- compatible-context? [runtime options timeout outer-timeout]
  ;; The child uses the parent's control. A tighter cap or a separate result
  ;; quota would require another enforcement boundary.
  (let [limits (control/limits (:control runtime))]
    (and (every? #{:limits} (keys options))
         (or (nil? (:limits options)) (map? (:limits options)))
         (every? (fn [[k n]]
                   (and (contains? limits k) (not (#{:result-rows :result-bytes} k))
                        (integer? n) (<= (limits k) n Long/MAX_VALUE)))
                 (:limits options))
         (or (nil? timeout) (and outer-timeout (<= (long outer-timeout) (long timeout)))))))

(defn- projection? [element]
  (or (instance? Variable element)
      (and (instance? Aggregate element)
           (#{'sum 'count 'count-distinct 'avg} (get-in element [:fn :symbol]))
           (= 1 (count (:args element))) (instance? Variable (first (:args element))))))

(defn capture!
  "Resolve source parameters by position. Inner logic variables are private;
  only the positional relation binding is visible to the caller."
  [runtime node outer-timeout sources]
  (let [[literal & actuals] (:args node)
        form (:value literal)
        binding (:binding node)
        check! #(control/check-active! (:control runtime))]
    (require-shape! (and (instance? Constant literal) (or (vector? form) (map? form))
                         (every? #(instance? SrcVar %) actuals)
                         (instance? BindColl binding) (instance? BindTuple (:binding binding))))
    (let [reservation (rt/charge! runtime form 2048 8)
          _ (rt/own! runtime #(control/release! reservation))
          {:keys [options]} (context/read! runtime form nil)
          parsed (input-program/parse! parser/parse-query (normalize/normalize runtime form check!))
          elements (parser/find-elements (:qfind parsed))
          inputs (mapv :variable (:qin parsed))
          outputs (mapv :variable (get-in binding [:binding :bindings]))
          aliases (zipmap (map :symbol inputs) (map :symbol actuals))]
      (require-shape!
        (and (compatible-context? runtime options (:qtimeout parsed) outer-timeout)
             (instance? FindRel (:qfind parsed)) (nil? (:qreturn-map parsed))
             (nil? (:qhaving parsed)) (empty? (:qorder parsed))
             (nil? (:qlimit parsed)) (nil? (:qoffset parsed))
             (= (count inputs) (count actuals) (count aliases))
             (every? #(instance? SrcVar %) inputs) (every? sources (vals aliases))
             (= (count outputs) (count elements) (count (set outputs)))
             (every? #(instance? Variable %) outputs)
             (some #(instance? Variable %) elements) (some parser/aggregate? elements)
             (every? projection? elements)))
      (let [clauses
            (mapv (fn [clause]
                    (check!)
                    (cond
                      (instance? Pattern clause)
                      (let [src (:source clause)
                            actual (aliases (if (instance? DefaultSrc src) '$ (:symbol src)))]
                        (require-shape! actual)
                        (assoc clause :source (parser/->SrcVar actual)))
                      (and (or (instance? Predicate clause) (instance? Function clause))
                           (expression/flat-scalar? clause)) clause
                      :else (require-shape! false))) (:qwhere parsed))]
        {:query parsed :clauses clauses :outputs (mapv :symbol outputs)}))))

(defn columns [site schema]
  (mapv (fn [element variable]
          (if (instance? Variable element)
            (assoc (get schema (:symbol element) {:type :data :nullable? true}) :variable variable)
            {:variable variable :nullable? false
             :type (if (#{'count 'count-distinct} (get-in element [:fn :symbol])) :long :data)}))
        (parser/find-elements (:qfind (:query site))) (:outputs site)))

(defn- total-aggregate? [element schema]
  (let [type (get-in schema [(get-in element [:args 0 :symbol]) :type])]
    (case (get-in element [:fn :symbol])
      (count count-distinct) true
      (sum avg)
      ;; Checked long addition may overflow. Exact decimal division may throw
      ;; even in a group which the outer join would eventually discard.
      (or (#{:bigint :double :float} type)
          (and (= :bigdec type) *math-context*
               (pos? (.getPrecision ^MathContext *math-context*))
               (not= RoundingMode/UNNECESSARY (.getRoundingMode ^MathContext *math-context*))))
      false)))

(defn lookup-key
  "A mandatory literal EAV restriction can supply the single grouping key.
  Match the caller's binding by position. Aggregate results and :with witnesses
  never become lookup keys. The cost comparison still needs an actual lookup."
  [site parent child schema]
  (let [elements (parser/find-elements (:qfind (:query site)))
        keys (keep-indexed #(when (instance? Variable %2) %1) elements)]
    (when (and (= 1 (count keys)) (seq (:patterns child)) (empty? (:core-clauses child))
               (every? #(total-aggregate? % schema) (filter parser/aggregate? elements)))
      (let [i (first keys) inner (:symbol (nth elements i)) outer (nth (:outputs site) i)
            column (schema inner)]
        (when (and column (not= :data (:type column))
                   (some (fn [{:keys [clause columns]}]
                           (let [[e _ v] (:terms clause)]
                             (and (= e [:var outer]) (= :const (first v))
                                  (= (:type column) (:type (first columns))))))
                         (:patterns parent)))
          {:inner column :outer (assoc column :variable outer)})))))
