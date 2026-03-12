(ns hooks.datalevin
  (:require [clj-kondo.hooks-api :as api]))

(def ^:private extended-query-clauses
  #{:having :limit :offset :order-by :timeout})

(def ^:private relation-built-ins
  '#{q fulltext idoc-match vec-neighbors embedding-neighbors})

(def ^:private value-built-ins
  '#{idoc-get like not-like})

(defn- quoted-form?
  [form]
  (and (seq? form)
       (= 'quote (first form))
       (= 2 (count form))))

(defn- coll-like
  [original items]
  (cond
    (vector? original) (vec items)
    (list? original)   (apply list items)
    :else              items))

(defn- query-vector->map
  [query]
  (loop [parsed {} key nil xs query]
    (if-let [x (first xs)]
      (if (keyword? x)
        (recur parsed x (next xs))
        (recur (update parsed key (fnil conj []) x) key (next xs)))
      parsed)))

(defn- ordered-query-entries
  [query-map]
  (let [known-order [:find :with :keys :strs :syms :in :where]
        seen        (set known-order)]
    (concat
      (keep (fn [k]
              (when-let [v (get query-map k)]
                [k v]))
            known-order)
      (remove (fn [[k _]] (contains? seen k)) query-map))))

(defn- query-map->vector
  [query-map]
  (reduce (fn [acc [k forms]]
            (into (conj acc k) forms))
          []
          (ordered-query-entries query-map)))

(defn- postwalk-form
  [f form]
  (let [form' (cond
                (map? form)
                (into (empty form)
                      (map (fn [[k v]]
                             [(postwalk-form f k) (postwalk-form f v)]))
                      form)

                (vector? form)
                (mapv #(postwalk-form f %) form)

                (set? form)
                (set (map #(postwalk-form f %) form))

                (seq? form)
                (doall (map #(postwalk-form f %) form))

                :else
                form)]
    (f form')))

(defn- dummy-binding-value
  [binding]
  (cond
    (= '_ binding)
    nil

    (symbol? binding)
    nil

    (vector? binding)
    (cond
      (and (= 2 (count binding)) (= '... (second binding)))
      [(dummy-binding-value (first binding))]

      (and (= 1 (count binding)) (sequential? (first binding)))
      [(dummy-binding-value (first binding))]

      :else
      (mapv (fn [_] nil) binding))

    (sequential? binding)
    (mapv (fn [_] nil) binding)

    :else
    nil))

(defn- normalize-call-form
  [form]
  (if (and (seq? form)
           (symbol? (first form))
           (contains? value-built-ins (first form)))
    (apply list 'vector (rest form))
    form))

(defn- normalize-where-clause
  [clause]
  (let [call    (first clause)
        clause' (if (and (sequential? clause)
                         (sequential? call)
                         (seq call)
                         (symbol? (first call))
                         (contains? relation-built-ins (first call))
                         (second clause))
                  (coll-like clause
                             [(list 'ground
                                    (dummy-binding-value (second clause)))
                              (second clause)])
                  clause)]
    (postwalk-form normalize-call-form clause')))

(defn- normalize-query-map
  [query]
  (cond-> (apply dissoc query extended-query-clauses)
    (contains? query :where)
    (update :where #(mapv normalize-where-clause %))))

(defn- normalize-query-form
  [query]
  (cond
    (quoted-form? query)
    (list 'quote (normalize-query-form (second query)))

    (map? query)
    (normalize-query-map query)

    (vector? query)
    (-> query
        query-vector->map
        normalize-query-map
        query-map->vector)

    :else
    query))

(defn- form->node
  [form]
  (cond
    (map? form)
    (api/map-node
      (mapcat (fn [[k v]]
                [(form->node k) (form->node v)])
              form))

    (vector? form)
    (api/vector-node (map form->node form))

    (set? form)
    (api/set-node (map form->node form))

    (seq? form)
    (api/list-node (map form->node form))

    :else
    (api/token-node form)))

(defn- rewrite-q-like-call
  [node query-index]
  (let [children    (:children node)
        query-node  (nth children query-index nil)
        inputs      (drop (inc query-index) children)
        query-form  (some-> query-node api/sexpr normalize-query-form)]
    {:node
     (api/list-node
       (concat
         [(api/token-node 'datascript.core/q)
          (if query-node
            (form->node query-form)
            (api/token-node nil))]
         inputs))}))

(defn q
  [{:keys [node]}]
  (rewrite-q-like-call node 1))

(defn explain
  [{:keys [node]}]
  (rewrite-q-like-call node 2))
