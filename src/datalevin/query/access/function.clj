;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.access.function
  "Compilation shared by function-backed query access methods."
  (:require
   [datalevin.query.access :as access]
   [datalevin.query.tuple :as qtuple])
  (:import
   [datalevin.parser BindColl BindTuple Constant Function PlainSymbol SrcVar
    Variable]))

(defrecord FunctionAccessSpec
    [function backend clause-idx clause original-clause source source-value args
     input-values requires produces cols projection binding])

(def default-function-backends
  {'fulltext            :fulltext
   'idoc-match          :idoc
   'vec-neighbors       :vector
   'embedding-neighbors :vector})

(defn- function-backend
  [supported function]
  (cond
    (map? supported) (get supported function)
    (set? supported) (when (contains? supported function) function)
    :else            (when (supported function) function)))

(defn compile-function-access
  "Compile a tuple-producing function clause into its backend-independent
  access specification.

  The function must be named, take a source as its first argument, and use a
  collection-of-tuples result binding. Returns nil when the clause is not an
  applicable access-function shape. Scalar query inputs are captured in
  `input-values`; remaining argument variables become correlated requirements."
  [{:keys [parsed-q inputs input-values]} clause-idx supported]
  (let [clause (nth (:qwhere parsed-q) clause-idx nil)]
    (when (instance? Function clause)
      (let [fn-form    (:fn ^Function clause)
            function   (when (instance? PlainSymbol fn-form)
                         (:symbol ^PlainSymbol fn-form))
            source-arg (first (:args ^Function clause))
            binding    (:binding ^Function clause)
            tuple-bind (when (and (instance? BindColl binding)
                                  (instance? BindTuple (:binding binding)))
                         (:binding binding))
            projection (when tuple-bind
                         (qtuple/tuple-binding-projection tuple-bind))
            backend    (when function
                         (function-backend supported function))]
        (when (and backend
                   (instance? SrcVar source-arg)
                   projection)
          (let [source       (:symbol ^SrcVar source-arg)
                input-values (access/planning-input-values
                               {:parsed-q parsed-q
                                :inputs inputs
                                :input-values input-values})
                args         (vec (next (:args ^Function clause)))
                arg-vars     (into #{}
                                   (keep (fn [arg]
                                           (when (instance? Variable arg)
                                             (:symbol ^Variable arg))))
                                   args)
                requires     (into #{}
                                   (remove #(contains? input-values %))
                                   arg-vars)
                cols         (:cols projection)]
            (->FunctionAccessSpec
              function
              backend
              clause-idx
              clause
              (nth (:qorig-where parsed-q) clause-idx)
              source
              (get input-values source)
              args
              (select-keys input-values arg-vars)
              requires
              (set cols)
              cols
              projection
              binding)))))))

(defn compile-function-accesses
  "Compile every applicable function clause in a planning context."
  [{:keys [parsed-q inputs] :as planning-context} supported]
  (let [planning-context
        (if (contains? planning-context :input-values)
          planning-context
          (assoc planning-context
                 :input-values
                 (delay (access/scalar-input-values parsed-q inputs))))]
    (into []
          (keep-indexed
            (fn [clause-idx _]
              (compile-function-access
                planning-context clause-idx supported)))
          (:qwhere parsed-q))))

(defn resolve-arguments
  "Resolve compiled function arguments with scalar query inputs and optional
  correlated bindings."
  ([spec]
   (resolve-arguments spec nil))
  ([{:keys [args input-values] :as spec} bindings]
   (let [values (merge input-values bindings)]
     (mapv
       (fn [arg]
         (cond
           (instance? Constant arg)
           (:value ^Constant arg)

           (instance? Variable arg)
           (let [sym (:symbol ^Variable arg)]
             (if (contains? values sym)
               (get values sym)
               (throw
                 (ex-info "Access function argument is not bound"
                          {:function (:function spec)
                           :variable sym
                           :requires (:requires spec)}))))

           :else
           (throw
             (ex-info "Unsupported access function argument"
                      {:function (:function spec)
                       :argument arg}))))
       args))))

(defn access-expr
  "Create the logical access expression shared by concrete implementations of
  a compiled function access."
  [spec method join-vars]
  (assoc
    (access/map->AccessExpr
      {:method    method
       :covers    #{(:clause spec)}
       :requires  (:requires spec)
       :produces  (:produces spec)
       :join-vars (set join-vars)
       :cols      (:cols spec)
       :source    (:source spec)})
    :covered-originals #{(:original-clause spec)}))

(defprotocol IFunctionAccessBackend
  (-function-access-plans [backend planning-context spec]
    "Return concrete access plans for one compiled function access."))

(defrecord FunctionAccessDispatcher
    [registry backends]

  access/IAccessMethod
  (-access-plans [_ planning-context]
    (let [registry (into {}
                         (filter (fn [[_ backend]]
                                   (contains? backends backend)))
                         registry)]
      (into []
            (mapcat
              (fn [spec]
                (-function-access-plans
                  (get backends (:backend spec))
                  planning-context
                  spec)))
            (compile-function-accesses planning-context registry))))

  (-open-access [_ _path _demand _bounds _work _source]
    (throw
      (ex-info "Function access dispatcher cannot open a concrete path" {})))

  (-frontier-satisfies? [_ _path _demand _frontier _cutoff]
    false))

(defn access-method
  "Build one access method that compiles supported function clauses once and
  dispatches each resulting spec to its concrete backend."
  ([backends]
   (access-method default-function-backends backends))
  ([registry backends]
   (->FunctionAccessDispatcher registry backends)))
