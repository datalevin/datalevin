;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.parser
  "Datalog parser"
  (:refer-clojure :exclude [distinct?])
  (:require
   [clojure.set :as set]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [raise conjv]]))

;; utils

(declare collect-vars-acc parse-clause parse-clauses parse-binding parse-aggregate)

(defprotocol ITraversable
  (-collect      [_ pred acc])
  (-collect-vars [_ acc])
  (-postwalk     [_ f]))

(defprotocol Traversable
  (-traversable? [_]))

(extend-type Object
  Traversable
  (-traversable? [_] false))

(extend-type nil
  Traversable
  (-traversable? [_] false))

(defmacro deftrecord
  "Augment all datalevin.parser/ records with default implementation of
  ITraversable"
  [tagname fields & rest]
  (let [f    (gensym "f")
        pred (gensym "pred")
        acc  (gensym "acc")]
    `(defrecord ~tagname ~fields
       ITraversable
       (~'-postwalk [this# ~f]
        (let [new# (new ~tagname
                        ~@(map #(list 'datalevin.parser/postwalk % f)
                               fields))]
          (if-let [meta# (meta this#)]
            (with-meta new# meta#)
            new#)))
       (~'-collect [_# ~pred ~acc]
        ;; [x y z] -> (collect pred z (collect pred y (collect pred x acc)))
        ~(reduce #(list 'datalevin.parser/collect pred %2 %1) acc fields))
       (~'-collect-vars [_# ~acc]
        ;; [x y z] -> (collect-vars-acc (collect-vars-acc (collect-vars-acc acc x) y) z)
        ~(reduce #(list 'datalevin.parser/collect-vars-acc %1 %2) acc fields))

       Traversable
       (~'-traversable? [_#] true)

       ~@rest)))

(defn of-size? [form size]
  (and (sequential? form)
       (= (count form) size)))

(defn parse-seq [parse-el form]
  (when (sequential? form)
    (unreduced
      (reduce #(if-let [parsed (parse-el %2)]
                 (conj %1 parsed)
                 (reduced nil))
              [] form))))

(defn collect
  ([pred form] (collect pred form []))
  ([pred form acc]
   (cond
     (pred form)          (conj acc form)
     (-traversable? form) (-collect form pred acc)
     (u/seqable? form)    (reduce (fn [acc form] (collect pred form acc))
                                  acc form)
     :else                acc)))

(defn distinct? [coll]
  (or (empty? coll)
      (apply clojure.core/distinct? coll)))

(defn postwalk [form f]
  (cond
    ;; additional handling for maps and records that keeps structure type
    (-traversable? form) (f (-postwalk form f))
    (map? form)          (f (reduce (fn [form [k v]]
                                      (assoc form k (postwalk v f)))
                                    form form))
    ;; rest comes from clojure.core
    (seq? form)          (f (map #(postwalk % f) form))
    (coll? form)         (f (into (empty form) (map #(postwalk % f) form)))
    :else                (f form)))

(defn with-source [obj source]
  (with-meta obj {:source source}))

(defn source [obj]
  (or (:source (meta obj)) obj))

;; placeholder    = the symbol '_'
;; variable       = symbol starting with "?"
;; src-var        = symbol starting with "$"
;; rules-var      = the symbol "%"
;; constant       = any non-variable data literal
;; plain-symbol   = symbol that does not begin with "$" or "?"

(deftrecord Placeholder [])
(deftrecord Variable    [symbol])
(deftrecord SrcVar      [symbol])
(deftrecord DefaultSrc  [])
(deftrecord RulesVar    [])
(deftrecord Constant    [value])
(deftrecord PlainSymbol [symbol])


(defn parse-placeholder [form]
  (when (u/sym-name-eqs form "_")
    (Placeholder.)))

(defn parse-variable [form]
  (when (qu/free-var? form)
    (Variable. form)))

(defn parse-var-required [form]
  (or (parse-variable form)
      (raise "Cannot parse var, expected symbol starting with ?, got: " form
             {:error :parser/rule-var, :form form})))

(defn parse-src-var [form]
  (when (qu/source? form)
    (SrcVar. form)))

(defn parse-rules-var [form]
  (when (u/sym-name-eqs form "%")
    (RulesVar.)))

(defn parse-constant [form]
  (when-not (qu/free-var? form)
    (Constant. form)))

(defn parse-plain-symbol [form]
  (when (and (symbol? form)
             (not (parse-variable form))
             (not (parse-src-var form))
             (not (parse-rules-var form))
             (not (parse-placeholder form)))
    (PlainSymbol. form)))

(defn parse-plain-variable [form]
  (when (parse-plain-symbol form)
    (Variable. form)))



;; fn-arg = (variable | constant | src-var)

(defn parse-fn-arg [form]
  (or (parse-variable form)
      (parse-src-var form)
      (parse-constant form)))


;; rule-vars = [ variable+ | ([ variable+ ] variable*) ]

(deftrecord RuleVars [required free])

(defn parse-rule-vars [form]
  (if (sequential? form)
    (let [[required rest] (if (sequential? (first form))
                            [(first form) (next form)]
                            [nil form])
          required*       (parse-seq parse-var-required required)
          free*           (parse-seq parse-var-required rest)]
      (when (and (empty? required*) (empty? free*))
        (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
               {:error :parser/rule-vars, :form form}))
      (when-not (distinct? (u/concatv required* free*))
        (raise "Rule variables should be distinct"
               {:error :parser/rule-vars, :form form}))
      (RuleVars. required* free*))
    (raise "Cannot parse rule-vars, expected [ variable+ | ([ variable+ ] variable*) ]"
           {:error :parser/rule-vars, :form form})))

(defn flatten-rule-vars [rule-vars]
  (u/concatv
    (when (:required rule-vars)
      [(mapv :symbol (:required rule-vars))])
    (mapv :symbol (:free rule-vars))))

(defn rule-vars-arity [rule-vars]
  [(count (:required rule-vars)) (count (:free rule-vars))])


;; binding        = (bind-scalar | bind-tuple | bind-coll | bind-rel)
;; bind-scalar    = variable
;; bind-tuple     = [ (binding | '_')+ ]
;; bind-coll      = [ binding '...' ]
;; bind-rel       = [ [ (binding | '_')+ ] ]

(deftrecord BindIgnore [])
(deftrecord BindScalar [variable])
(deftrecord BindTuple  [bindings])
(deftrecord BindColl   [binding])

(defn parse-bind-ignore [form]
  (when (u/sym-name-eqs form "_")
    (with-source (BindIgnore.) form)))

(defn parse-bind-scalar [form]
  (when-let [var (parse-variable form)]
    (with-source (BindScalar. var) form)))

(defn parse-bind-coll [form]
  (when (and (of-size? form 2)
             (= (second form) '...))
    (if-let [sub-bind (parse-binding (first form))]
      (with-source (BindColl. sub-bind) form)
      (raise "Cannot parse collection binding"
             {:error :parser/binding, :form form}))))

(defn parse-tuple-el [form]
  (or (parse-bind-ignore form)
      (parse-binding form)))

(defn parse-bind-tuple [form]
  (when-let [sub-bindings (parse-seq parse-tuple-el form)]
    (if-not (empty? sub-bindings)
      (with-source (BindTuple. sub-bindings) form)
      (raise "Tuple binding cannot be empty"
             {:error :parser/binding, :form form}))))

(defn parse-bind-rel [form]
  (when (and (of-size? form 1)
             (sequential? (first form)))
    ;; relation is just a sequence of tuples
    (with-source (BindColl. (parse-bind-tuple (first form))) form)))

(defn parse-binding [form]
  (or (parse-bind-coll form)
      (parse-bind-rel form)
      (parse-bind-tuple form)
      (parse-bind-ignore form)
      (parse-bind-scalar form)
      (raise "Cannot parse binding, expected
(bind-scalar | bind-tuple | bind-coll | bind-rel)"
             {:error :parser/binding, :form form})))


;; find-spec        = ':find' (find-rel | find-coll | find-tuple | find-scalar)
;; find-rel         = find-elem+
;; find-coll        = [ find-elem '...' ]
;; find-scalar      = find-elem '.'
;; find-tuple       = [ find-elem+ ]
;; find-elem        = (variable | pull-expr | aggregate | custom-aggregate)
;; pull-expr        = [ 'pull' src-var? variable pull-pattern ]
;; pull-pattern     = (constant | variable | plain-symbol)
;; aggregate        = [ aggregate-fn fn-arg+ ]
;; aggregate-fn     = plain-symbol
;; custom-aggregate = [ 'aggregate' variable fn-arg+ ]

(defprotocol IFindVars
  (-find-vars [this]))

(extend-protocol IFindVars
  Variable
  (-find-vars [this] [(.-symbol this)]))

(deftrecord Aggregate [fn args]
  IFindVars (-find-vars [this] (-find-vars (last (.-args this)))))

(deftrecord Pull [source variable pattern]
  IFindVars (-find-vars [this] (-find-vars (.-variable this))))

(defn- collect-expr-vars
  "Collect variables from a FindExpr's arguments recursively."
  [args]
  (mapcat (fn [arg]
            (cond
              (instance? Aggregate arg) (-find-vars arg)
              (and (map? arg) (:args arg)) (collect-expr-vars (:args arg))
              :else []))
          args))

(deftrecord FindExpr [fn args]
  ;; FindExpr contributes vars from its inner aggregates
  IFindVars (-find-vars [this] (collect-expr-vars (.-args this))))

(defprotocol IFindElements
  (find-elements [this]))

(deftrecord FindRel [elements]
  IFindElements (find-elements [_] elements))

(deftrecord FindColl [element]
  IFindElements (find-elements [_] [element]))

(deftrecord FindScalar [element]
  IFindElements (find-elements [_] [element]))

(deftrecord FindTuple [elements]
  IFindElements (find-elements [_] elements))

(defn find-vars [find]
  (mapcat -find-vars (find-elements find)))

(defn aggregate? [element]
  (instance? Aggregate element))

(defn pull? [element]
  (instance? Pull element))

(defn find-expr? [element]
  (instance? FindExpr element))

(def ^:private find-expr-ops
  "Supported operators in find expressions"
  '#{+ - * / mod rem quot})

(defn parse-find-expr [form]
  (when (and (sequential? form)
             (>= (count form) 2))
    (let [[op & args] form]
      (when (find-expr-ops op)
        (let [parsed-args (mapv (fn [arg]
                                  (or (parse-find-expr arg)
                                      (parse-aggregate arg)
                                      (parse-constant arg)))
                                args)]
          (when (every? some? parsed-args)
            (FindExpr. (PlainSymbol. op) parsed-args)))))))

(defn parse-aggregate [form]
  (when (and (sequential? form)
             (>= (count form) 2))
    (let [[fn & args] form
          fn*   (parse-plain-symbol fn)
          args* (parse-seq parse-fn-arg args)]
      (when (and fn* args*)
        (Aggregate. fn* args*)))))

(defn parse-aggregate-custom [form]
  (when (and (sequential? form)
             (u/sym-name-eqs (first form) "aggregate"))
    (if (>= (count form) 3)
      (let [[_ fn & args] form
            fn*           (parse-variable fn)
            args*         (parse-seq parse-fn-arg args)]
        (if (and fn* args*)
          (Aggregate. fn* args*)
          (raise "Cannot parse custom aggregate call, expect ['aggregate' variable fn-arg+]"
                 {:error :parser/find, :fragment form})))
      (raise "Cannot parse custom aggregate call, expect ['aggregate' variable fn-arg+]"
             {:error :parser/find, :fragment form}))))

(defn parse-pull-expr [form]
  (when (and (sequential? form)
             (u/sym-name-eqs (first form) "pull"))
    (if (<= 3 (count form) 4)
      (let [long?         (= (count form) 4)
            src           (if long? (nth form 1) '$)
            [var pattern] (if long? (nnext form) (next form))
            src*          (parse-src-var src)
            var*          (parse-variable var)
            pattern*      (or (parse-variable pattern)
                              (parse-plain-variable pattern)
                              (parse-constant pattern))]
        (if (and src* var* pattern*)
          (Pull. src* var* pattern*)
          (raise "Cannot parse pull expression, expect ['pull' src-var? variable (constant | variable | plain-symbol)]"
                 {:error :parser/find, :fragment form})))
      (raise "Cannot parse pull expression, expect ['pull' src-var? variable (constant | variable | plain-symbol)]"
             {:error :parser/find, :fragment form}))))

(defn parse-find-elem [form]
  (or (parse-variable form)
      (parse-pull-expr form)
      (parse-aggregate-custom form)
      (parse-find-expr form)
      (parse-aggregate form)))

(defn parse-find-rel [form]
  (some->
    (parse-seq parse-find-elem form)
    (FindRel.)))

(defn parse-find-coll [form]
  (when (and (sequential? form)
             (= (count form) 1))
    (let [inner (first form)]
      (when (and (sequential? inner)
                 (= (count inner) 2)
                 (= (second inner) '...))
        (some-> (parse-find-elem (first inner))
                (FindColl.))))))

(defn parse-find-scalar [form]
  (when (and (sequential? form)
             (= (count form) 2)
             (= (second form) '.))
    (some-> (parse-find-elem (first form))
            (FindScalar.))))

(defn parse-find-tuple-scalar
  [form]
  (when (and (sequential? form)
             (> (count form) 2)
             (= (last form) '.))
    (some-> (parse-seq parse-find-elem (butlast form))
            (FindTuple.))))

(defn parse-find-tuple [form]
  (when (and (sequential? form)
             (= (count form) 1))
    (let [inner (first form)]
      (some->
        (parse-seq parse-find-elem inner)
        (FindTuple.)))))

(defn parse-find [form]
  (or (parse-find-rel form)
      (parse-find-coll form)
      (parse-find-scalar form)
      (parse-find-tuple-scalar form)
      (parse-find-tuple form)
      (raise "Cannot parse :find, expected: (find-rel | find-coll | find-tuple | find-scalar)"
             {:error :parser/find, :fragment form})))


;; return-map  = (return-keys | return-syms | return-strs)
;; return-keys = ':keys' symbol+
;; return-syms = ':syms' symbol+
;; return-strs = ':strs' symbol+

(deftrecord ReturnMap [type symbols])

(defn parse-return-map [type form]
  (when (and (seq form)
             (every? symbol? form))
    (case type
      :keys (ReturnMap. type (mapv keyword form))
      :syms (ReturnMap. type (vec form))
      :strs (ReturnMap. type (mapv str form))
      nil)))

;; with = [ variable+ ]

(defn parse-with [form]
  (or
    (parse-seq parse-variable form)
    (raise "Cannot parse :with clause, expected [ variable+ ]"
           {:error :parser/with, :form form})))


;; having = [ [(pred-fn aggregate-or-find-expr+)]+ ]
;; Predicates over aggregates, evaluated after grouping

(deftrecord HavingPred [fn args])

(defn- parse-having-arg
  "Parse an argument in a having predicate - can be aggregate, find-expr, or constant."
  [form]
  (or (parse-find-expr form)
      (parse-aggregate form)
      (parse-constant form)))

(defn- parse-having-pred
  "Parse a single having predicate like [(pos? (sum ?x))]."
  [form]
  (when (and (sequential? form)
             (= (count form) 1)
             (sequential? (first form)))
    (let [[fn-sym & args] (first form)
          fn*   (parse-plain-symbol fn-sym)
          args* (parse-seq parse-having-arg args)]
      (when (and fn* args*)
        (HavingPred. fn* args*)))))

(defn parse-having
  "Parse the :having clause containing predicates over aggregates."
  [form]
  (when form
    (parse-seq parse-having-pred form)))


;; in = [ (src-var | rules-var | plain-symbol | binding)+ ]

(defn- parse-in-binding [form]
  (if-let [var (or (parse-src-var form)
                   (parse-rules-var form)
                   (parse-plain-variable form))]
    (with-source (BindScalar. var) form)
    (parse-binding form)))

(defn parse-in [form]
  (or
    (parse-seq parse-in-binding form)
    (raise "Cannot parse :in clause, expected (src-var | % | plain-symbol | bind-scalar | bind-tuple | bind-coll | bind-rel)"
           {:error :parser/in, :form form})))


;; clause          = (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)
;; data-pattern    = [ src-var? (variable | constant | '_')+ ]
;; pred-expr       = [ [ pred fn-arg+ ] ]
;; pred            = (plain-symbol | variable)
;; fn-expr         = [ [ fn fn-arg+ ] binding ]
;; fn              = (plain-symbol | variable)
;; rule-expr       = [ src-var? rule-name (variable | constant | '_')+ ]
;; not-clause      = [ src-var? 'not' clause+ ]
;; not-join-clause = [ src-var? 'not-join' [ variable+ ] clause+ ]
;; or-clause       = [ src-var? 'or' (clause | and-clause)+ ]
;; or-join-clause  = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
;; and-clause      = [ 'and' clause+ ]

(deftrecord Pattern   [source pattern])
(deftrecord Predicate [fn args])
(deftrecord Function  [fn args binding])
(deftrecord RuleExpr  [source name args]) ;; TODO rule with constant or '_' as argument
(deftrecord Not       [source vars clauses])
(deftrecord Or        [source rule-vars clauses])
(deftrecord And       [clauses])


(defn parse-pattern-el [form]
  (or (parse-placeholder form)
      (parse-variable form)
      (parse-constant form)))

(defn take-source [form]
  (when (sequential? form)
    (if-let [source* (parse-src-var (first form))]
      [source* (next form)]
      [(DefaultSrc.) form])))

(defn parse-pattern [form]
  (when-let [[source* next-form] (take-source form)]
    (when-let [pattern* (parse-seq parse-pattern-el next-form)]
      (if (seq pattern*)
        (with-source (Pattern. source* pattern*) form)
        (raise "Pattern could not be empty"
               {:error :parser/where, :form form})))))

(defn parse-call [form]
  (when (sequential? form)
    (let [[fn & args] form
          args  (if (nil? args) [] args)
          fn*   (or (parse-plain-symbol fn)
                    (parse-variable fn))
          args* (parse-seq parse-fn-arg args)]
      (when (and fn* args*)
        [fn* args*]))))

(defn parse-pred [form]
  (when (of-size? form 1)
    (when-let [[fn* args*] (parse-call (first form))]
      (-> (Predicate. fn* args*)
          (with-source form)))))

(defn parse-fn [form]
  (when (of-size? form 2)
    (let [[call binding] form]
      (when-let [[fn* args*] (parse-call call)]
        (when-let [binding* (parse-binding binding)]
          (-> (Function. fn* args* binding*)
              (with-source form)))))))

(defn parse-rule-expr [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[name & args] next-form
          name* (parse-plain-symbol name)
          args* (parse-seq parse-pattern-el args)]
      (when name*
        (cond
          (empty? args)
            (raise "rule-expr requires at least one argument"
                   {:error :parser/where, :form form})
          (nil? args*)
            (raise "Cannot parse rule-expr arguments, expected [ (variable | constant | '_')+ ]"
                   {:error :parser/where, :form form})
          :else
            (RuleExpr. source* name* args*)
          )))))

(defn- collect-vars-acc [acc form]
  (cond
    (instance? Variable form)
      (conj acc form)
    (instance? Not form)
      (into acc (:vars form))
    (instance? Or form)
      (collect-vars-acc acc (:rule-vars form))
    (-traversable? form)
      (-collect-vars form acc)
    (sequential? form)
      (reduce collect-vars-acc acc form)
    :else acc))

(defn- collect-vars [form]
  (collect-vars-acc [] form))

(defn collect-vars-distinct [form]
  (vec (distinct (collect-vars form))))

(defn- validate-join-vars [required free form]
  (when (and (empty? required) (empty? free))
    (raise "Join variables should not be empty"
      {:error :parser/where, :form form})))

(defn- validate-not [clause form]
  (validate-join-vars nil (:vars clause) form)
  clause)

(defn parse-not [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym & clauses] next-form]
      (when (u/sym-name-eqs sym "not")
        (if-let [clauses* (parse-clauses clauses)]
          (-> (Not. source* (collect-vars-distinct clauses*) clauses*)
              (with-source form)
              (validate-not form))
          (raise "Cannot parse 'not' clause, expected [ src-var? 'not' clause+ ]"
                 {:error :parser/where, :form form}))))))

(defn parse-not-join [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym vars & clauses] next-form]
      (when (u/sym-name-eqs sym "not-join")
        (let [vars*    (parse-seq parse-variable vars)
              clauses* (parse-clauses clauses)]
          (if (and vars* clauses*)
            (-> (Not. source* vars* clauses*)
                (with-source form)
                (validate-not form))
            (raise "Cannot parse 'not-join' clause, expected [ src-var? 'not-join' [variable+] clause+ ]"
                   {:error :parser/where, :form form})))))))

(defn validate-or [clause form]
  (let [{{required :required
          free     :free} :rule-vars} clause]
    (validate-join-vars required free form)
    clause))

(defn parse-and [form]
  (when (and (sequential? form)
             (u/sym-name-eqs (first form) "and"))
    (let [clauses* (parse-clauses (next form))]
      (if (not-empty clauses*)
        (And. clauses*)
        (raise "Cannot parse 'and' clause, expected [ 'and' clause+ ]"
               {:error :parser/where, :form form})))))

(defn parse-or [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym & clauses] next-form]
      (when (u/sym-name-eqs sym "or")
        (if-let [clauses* (parse-seq (some-fn parse-and parse-clause) clauses)]
          (-> (Or. source* (RuleVars. nil (collect-vars-distinct clauses*)) clauses*)
              (with-source form)
              (validate-or form))
          (raise "Cannot parse 'or' clause, expected [ src-var? 'or' clause+ ]"
                 {:error :parser/where, :form form}))))))

(defn parse-or-join [form]
  (when-let [[source* next-form] (take-source form)]
    (let [[sym vars & clauses] next-form]
      (when (u/sym-name-eqs sym "or-join")
        (let [vars*    (parse-rule-vars vars)
              clauses* (parse-seq (some-fn parse-and parse-clause) clauses)]
          (if (and vars* clauses*)
            (-> (Or. source* vars* clauses*)
                (with-source form)
                (validate-or form))
            (raise "Cannot parse 'or-join' clause, expected [ src-var? 'or-join' [variable+] clause+ ]"
                   {:error :parser/where, :form form})))))))

(defn parse-clause [form]
  (or
    (parse-not       form)
    (parse-not-join  form)
    (parse-or        form)
    (parse-or-join   form)
    (parse-pred      form)
    (parse-fn        form)
    (parse-rule-expr form)
    (parse-pattern   form)
    (raise "Cannot parse clause, expected (data-pattern | pred-expr | fn-expr | rule-expr | not-clause | not-join-clause | or-clause | or-join-clause)"
           {:error :parser/where, :form form} )))

(defn parse-clauses [clauses]
  (parse-seq parse-clause clauses))

(defn parse-where [form]
  (or (parse-clauses form)
      (raise "Cannot parse :where clause, expected [clause+]"
             {:error :parser/where, :form form})))

;; rule-branch = [rule-head clause+]
;; rule-head   = [rule-name rule-vars]
;; rule-name   = plain-symbol

(deftrecord RuleBranch [vars clauses])
(deftrecord Rule [name branches])

(defn parse-rule [form]
  (if (sequential? form)
    (let [[head & clauses] form]
      (if (sequential? head)
        (let [[name & vars] head
              name*    (or (parse-plain-symbol name)
                           (raise "Cannot parse rule name, expected plain-symbol"
                                  {:error :parser/rule, :form form}))
              vars*    (parse-rule-vars vars)
              clauses* (or (not-empty (parse-clauses clauses))
                           (raise "Rule branch should have clauses"
                                  {:error :parser/rule, :form form}))]
            {:name    name*
             :vars    vars*
             :clauses clauses*})
        (raise "Cannot parse rule head, expected [rule-name rule-vars], got: " head
               {:error :parser/rule, :form form})))
    (raise "Cannot parse rule, expected [rule-head clause+]"
           {:error :parser/rule, :form form})))

(defn validate-arity [name branches]
  (let [vars0  (:vars (first branches))
        arity0 (rule-vars-arity vars0)]
    (doseq [b    (next branches)
            :let [vars (:vars b)]]
      (when (not= arity0 (rule-vars-arity vars))
        (raise "Arity mismatch for rule '" (:symbol name) "': "
               (flatten-rule-vars vars0) " vs. " (flatten-rule-vars vars)
         {:error :parser/rule, :rule name})))))

(defn parse-rules [form]
  (vec
    ;; group rule branches by name
    (for [[name branches] (group-by :name (parse-seq parse-rule form))
          :let [branches (mapv #(RuleBranch. (:vars %) (:clauses %)) branches)]]
      (do
        (validate-arity name branches)
        (Rule. name branches)))))


;; query

;; q* prefix because of https://dev.clojure.org/jira/browse/CLJS-2237
(deftrecord Query
  [qfind qorig-find qwith qreturn-map qin qwhere qorig-where qhaving
   qtimeout qorder qlimit qoffset])

(defn query->map [query]
  (loop [parsed {}, key nil, qs query]
    (if-let [q (first qs)]
      (if (keyword? q)
        (recur parsed q (next qs))
        (recur (update-in parsed [key] conjv q) key (next qs)))
      parsed)))

(defn explicit-input [parsed]
  (let [source (:source parsed)]
    (if (instance? Pattern parsed)
      source
      (when (some? source)
        (when (not (instance? DefaultSrc source))
          source)))))

(defn default-in [qwhere]
  (if (seq (collect explicit-input qwhere))
    '[$]
    (let [srcs (collect #(instance? SrcVar %) qwhere)]
      (if (some #(= '$ (:symbol %)) srcs) '[$] []))))

(defn validate-query [q form form-map]
  (let [find-vars  (set (collect-vars (:qfind q)))
        with-vars  (set (:qwith q))
        in-vars    (set (collect-vars (:qin q)))
        where-vars (set (collect-vars (:qwhere q)))
        unknown    (set/difference (set/union find-vars with-vars)
                                   (set/union where-vars in-vars))
        shared     (set/intersection find-vars with-vars)]
    (when (seq unknown)
      (raise "Query for unknown vars: " (mapv :symbol unknown)
             {:error :parser/query, :vars unknown, :form form}))
    (when (seq shared)
      (raise ":find and :with should not use same variables: " (mapv :symbol shared)
             {:error :parser/query, :vars shared, :form form})))

  (when-some [order-spec (:qorder q)]
    (let [find-elems  (find-elements (:qfind q))
          find-vars   (set (map :symbol (collect-vars (:qfind q))))
          order-vars  (filterv symbol? order-spec)
          order-idxs  (filterv nat-int? order-spec)
          num-entries (/ (count order-spec) 2)]
      (when (not= (+ (count (set order-vars)) (count (set order-idxs)))
                  num-entries)
        (raise "Repeated :order-by variables"
               {:error :parser/query, :form order-spec}))
      (when-not (set/subset? (set order-vars) find-vars)
        (raise "There are :order-by variable that is not in :find spec"
               {:error :parser/query, :form order-spec}))
      (when-not (every? #(< ^long % (count find-elems)) order-idxs)
        (raise ":order-by column index out of bounds"
               {:error :parser/query, :form order-spec}))))

  (when-some [return-map (:qreturn-map q)]
    (when (instance? FindScalar (:qfind q))
      (raise (:type return-map) " does not work with single-scalar :find"
             {:error :parser/query, :form form}))
    (when (instance? FindColl (:qfind q))
      (raise (:type return-map) " does not work with collection :find"
             {:error :parser/query, :form form})))

  (when-some [return-symbols (:symbols (:qreturn-map q))]
    (let [find-elements (find-elements (:qfind q))]
      (when-not (= (count return-symbols) (count find-elements))
        (raise "Count of " (:type (:qreturn-map q)) " must match count of :find"
               {:error      :parser/query
                :return-map (cons (:type (:qreturn-map q)) return-symbols)
                :find       find-elements
                :form       form}))))

  (when (< 1 (->> [(:keys form-map) (:syms form-map) (:strs form-map)]
                  (filter some?)
                  (count)))
    (raise "Only one of :keys/:syms/:strs must be present"
           {:error :parser/query, :form form}))

  (let [in-vars    (collect-vars (:qin q))
        in-sources (collect #(instance? SrcVar %) (:qin q))
        in-rules   (collect #(instance? RulesVar %) (:qin q))]
    (when-not (and (distinct? in-vars)
                   (distinct? in-sources)
                   (distinct? in-rules))
      (raise "Vars used in :in should be distinct"
             {:error :parser/query, :form form})))

  (let [with-vars (collect-vars (:qwith q))]
    (when-not (distinct? with-vars)
      (raise "Vars used in :with should be distinct"
             {:error :parser/query, :form form})))

  (let [in-sources    (collect #(instance? SrcVar %) (:qin q) #{})
        where-sources (collect #(instance? SrcVar %) (:qwhere q) #{})
        unknown       (set/difference where-sources in-sources)]
    (when (seq unknown)
      (raise "Where uses unknown source vars: " (mapv :symbol unknown)
             {:error :parser/query, :vars unknown, :form form})))

  (let [rule-exprs (collect #(instance? RuleExpr %) (:qwhere q))
        rules-vars (collect #(instance? RulesVar %) (:qin q))]
    (when (and (seq rule-exprs)
               (empty? rules-vars))
      (raise "Missing rules var '%' in :in"
             {:error :parser/query, :form form}))))

(defn parse-timeout [t]
  (cond
    (nil? t)        nil
    (pos-int? t)    t
    (sequential? t) (recur (first t))
    :else           (raise "Unsupported timeout format"
                           {:error :parser/query :form t})))

(defn parse-limit [t]
  (cond
    (nil? t)        nil
    (pos-int? t)    t
    (sequential? t) (recur (first t))
    (neg-int? t)    -1
    :else           (raise "Unsupported limit format"
                           {:error :parser/query :form t})))

(defn parse-offset [t]
  (cond
    (nil? t)        nil
    (pos-int? t)    t
    (= t 0)         t
    (sequential? t) (recur (first t))
    :else           (raise "Unsupported offset format"
                           {:error :parser/query :form t})))

(defn- var-or-idx? [x]
  (or (symbol? x) (nat-int? x)))

(defn- parse-order-vec [ob]
  (loop [res [] in? false vs ob]
    (if (seq vs)
      (let [cv (first vs)]
        (cond
          (var-or-idx? cv)
          (if in?
            (recur (-> res (conj :asc) (conj cv)) true (rest vs))
            (recur (conj res cv) true (rest vs)))
          (#{:asc :desc} cv)
          (if in?
            (recur (conj res cv) false (rest vs))
            (raise "Incorrect order-by format" {:error :parser/query :form ob}))
          :else
          (raise "Incorrect order-by format" {:error :parser/query :form ob})))
      (if (var-or-idx? (peek res))
        (conj res :asc)
        res))))

(defn parse-order [[ob]]
  (cond
    (symbol? ob)  [ob :asc]
    (nat-int? ob) [ob :asc]
    (vector? ob)  (parse-order-vec ob)
    (nil? ob)     nil
    :else         (raise "Unsupported order-by format"
                         {:error :parser/query :form ob})))

(defn- qwhere-qualified-fns [qwhere]
  (into #{}
        (comp (map #(-> % :fn :symbol))
           (filter qualified-symbol?))
        qwhere))

(defn parse-query [q]
  (let [qm     (cond
                 (map? q)        q
                 (sequential? q) (query->map q)
                 :else           (raise "Query should be a vector or a map"
                                        {:error :parser/query, :form q}))
        find   (:find qm)
        where  (vec (distinct (:where qm [])))
        qwhere (parse-where where)
        res    (map->Query
                 {:qfind       (parse-find find)
                  :qorig-find  find
                  :qwith       (when-let [with (:with qm)]
                                 (parse-with with))
                  :qreturn-map (or (parse-return-map :keys (:keys qm))
                                   (parse-return-map :syms (:syms qm))
                                   (parse-return-map :strs (:strs qm)))
                  :qin         (parse-in
                                 (or (:in qm) (default-in qwhere)))
                  :qwhere      qwhere
                  :qorig-where where
                  :qhaving     (parse-having (:having qm))
                  :qtimeout    (parse-timeout (:timeout qm))
                  :qorder      (parse-order (:order-by qm))
                  :qoffset     (parse-offset (:offset qm))
                  :qlimit      (parse-limit (:limit qm))
                  ;; These are for result cache invalidation when
                  ;; function implementations change.
                  :qwhere-qualified-fns (qwhere-qualified-fns qwhere)})]
    (validate-query res q qm)
    res))
