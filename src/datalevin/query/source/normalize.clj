(ns ^:no-doc datalevin.query.source.normalize
  "Lower bounded expressions before source-query variable and scope analysis."
  (:require [datalevin.parser :as parser]
            [datalevin.query-util :as qu]
            [datalevin.query.source :as source]
            [datalevin.query.source.expression :as expression]
            [datalevin.query.source.runtime :as rt]))

(defn- membership [clause admit!]
  (let [[call] clause
        terms (next call)
        variable (when (list? (first terms)) (second (first terms)))]
    (when (and (= 'or (first call)) (<= 1 (count terms) 256)
               (qu/free-var? variable)
               (every? #(and (list? %) (= 3 (count %)) (= '= (first %))
                             (= variable (second %))
                             (or (string? (nth % 2)) (int? (nth % 2)))) terms))
      (admit!)
      (let [values (into #{} (map #(nth % 2)) terms)]
        ;; in returns its member. A bound disjunction of equalities returns
        ;; a boolean, including false on a miss.
        (assoc clause 0 (if (= 1 (count clause))
                          (list 'in variable values)
                          (list 'contains? values variable)))))))

(defn- arithmetic-tree? [form]
  (or (number? form) (qu/free-var? form)
      (and (list? form) (expression/arithmetic-functions (first form))
           (every? arithmetic-tree? (rest form)))))

(defn- flatten-call [call output fresh]
  (let [steps (volatile! [])]
    (letfn [(emit [call output]
              (let [args (mapv (fn [arg]
                                 (if (list? arg)
                                   (let [v (fresh)] (emit arg v) v)
                                   arg)) (rest call))]
                (vswap! steps conj [(apply list (first call) args) output])))]
      (emit call output)
      @steps)))

(defn normalize
  "Own any new syntax and temporary variable index under the query budget.
  Keep caller data, rule arguments and quoted subqueries opaque."
  [runtime query check!]
  (let [qm (if (sequential? query) (parser/query->map query) query)
        where (:where qm)
        ;; Reserve before building the set index, fresh symbols or clauses.
        ;; The original syntax bounds the number and size of generated nodes.
        budget (delay (let [token (rt/charge! runtime qm 2048 8)]
                        (rt/own! runtime #(source/release! token))))
        admit! #(force budget)
        symbols (delay (into #{} (filter symbol?) (tree-seq coll? seq qm)))
        fresh (fn [] (loop [v (gensym "?scalar__")]
                       (if (contains? @symbols v) (recur (gensym "?scalar__")) v)))]
    (letfn [(rewrite [c root?]
              (check!)
              (cond
                (and (vector? c) (<= 1 (count c) 2) (list? (first c)))
                (if (and root? (= 2 (count c)) (qu/free-var? (second c))
                         (some list? (rest (first c))) (arithmetic-tree? (first c)))
                  (do (admit!) (flatten-call (first c) (second c) fresh))
                  (when-let [normalized (membership c admit!)] [normalized]))

                (and (list? c) (#{'and 'or 'not 'or-join 'not-join} (first c)))
                (let [n (if (#{'or-join 'not-join} (first c)) 2 1)]
                  (when-let [body (clauses (drop n c) false)]
                    [(apply list (concat (take n c) body))]))))
            (clauses [original root?]
              ;; Allocate an output prefix only when a clause changes.
              (loop [cs (seq original) i 0 out nil]
                (if cs
                  (let [replacement (rewrite (first cs) root?)]
                    (recur (next cs) (inc i)
                           (cond replacement (into (or out (vec (take i original))) replacement)
                                 out (conj out (first cs)))))
                  out)))]
      (if-let [rewritten (when (sequential? where) (clauses where true))]
        (assoc qm :where rewritten)
        qm))))
