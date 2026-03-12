(ns clj-kondo.hooks-api)

(declare coerce)

(defn token-node
  [value]
  {:tag :token
   :value value})

(defn list-node
  [children]
  {:tag :list
   :children (vec children)})

(defn vector-node
  [children]
  {:tag :vector
   :children (vec children)})

(defn set-node
  [children]
  {:tag :set
   :children (vec children)})

(defn map-node
  [children]
  {:tag :map
   :children (vec children)})

(defn sexpr
  [node]
  (case (:tag node)
    :token  (:value node)
    :list   (apply list (map sexpr (:children node)))
    :vector (vec (map sexpr (:children node)))
    :set    (set (map sexpr (:children node)))
    :map    (into {}
                   (map (fn [[k v]] [(sexpr k) (sexpr v)]))
                   (partition 2 (:children node)))
    node))

(defn coerce
  [form]
  (cond
    (map? form)
    (map-node
      (mapcat (fn [[k v]]
                [(coerce k) (coerce v)])
              form))

    (vector? form)
    (vector-node (map coerce form))

    (set? form)
    (set-node (map coerce form))

    (seq? form)
    (list-node (map coerce form))

    :else
    (token-node form)))
