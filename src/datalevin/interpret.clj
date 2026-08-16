;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.interpret
  "Code interpreter, including functions and macros useful for command line
  and query/transaction functions."
  (:require
   [clojure.walk :as w]
   [clojure.set :as set]
   [clojure.pprint :as p]
   [clojure.java.io :as io]
   [sci.core :as sci]
   [s-exp.hako.ext :as hext]
   [datalevin.query-util :as qu]
   [datalevin.util :as u]
   [datalevin.core]
   [datalevin.analyzer]
   [datalevin.bits]
   [datalevin.stem]
   [datalevin.client]
   [datalevin.constants]
   [datalevin.lmdb]
   [clojure.string :as s])
  (:import
   [clojure.lang AFn]
   [java.io DataInput DataOutput Writer]))

(def ^:no-doc user-facing-ns
  #{'datalevin.core 'datalevin.client 'datalevin.interpret
    'datalevin.constants})

(def ^:no-doc additional-ns
  #{'datalevin.analyzer})

(defn- user-facing? [v]
  (let [m (meta v)
        d (m :doc)]
    (and d
         (if-let [p (:protocol m)]
           (and (not (:no-doc (meta p)))
                (not (:no-doc m)))
           (not (:no-doc m)))
         (not (s/starts-with? d "Positional factory function for class")))))

(defn ^:no-doc available-map [ns var-map pred]
  (let [sci-ns (sci/create-ns ns)]
    (reduce
     (fn [m [k v]]
       (assoc m k (sci/new-var (symbol v) v (assoc (meta v)
                                                   :sci.impl/built-in true
                                                   :ns sci-ns))))
     {}
     (select-keys var-map (keep (fn [[k v]] (when (pred v) k)) var-map)))))

(defn ^:no-doc user-facing-map [ns var-map]
  (available-map ns var-map user-facing?))

(defn ^:no-doc additional-map [ns var-map]
  (available-map ns var-map (constantly true)))

(defn- user-facing-vars []
  (reduce
   (fn [m ns]
     (assoc m ns (user-facing-map ns (ns-publics ns))))
   {}
   user-facing-ns))

(defn- additional-vars []
  (reduce
   (fn [m ns]
     (assoc m ns (additional-map ns (ns-publics ns))))
   {}
   additional-ns))

(defn ^:no-doc resolve-var [s]
  (when (symbol? s)
    (some #(ns-resolve % s) (conj user-facing-ns *ns*))))

(defn- qualify-static-class-call
  [f]
  (when-let [class-part (and (symbol? f) (namespace f))]
    (let [resolved (resolve (symbol class-part))]
      (when (instance? Class resolved)
        (symbol (.getName ^Class resolved) (name f))))))

(defn- qualify-fn [x]
  (if (list? x)
    (let [[f & args] x]
      (cond
        (qu/rule-head f)
        x

        :else
        (if-let [var (resolve-var f)]
          (apply list (symbol var) args)
          (if-let [class-call (qualify-static-class-call f)]
            (apply list class-call args)
            x))))
    x))

(declare ctx inter-fn-ctx inter-fn-namespaces inter-fn-classes)

(defn ^:no-doc eval-fn [ctx form]
  (sci/eval-form ctx (if (coll? form)
                       (w/postwalk qualify-fn form)
                       form)))

(defn load-edn
  "Same as [`clojure.core/load-file`](https://clojuredocs.org/clojure.core/load-file),
   useful for e.g. loading schema from a file"
  [f]
  (let [f (io/file f)
        s (slurp f)]
    (sci/with-bindings {sci/ns   @sci/ns
                        sci/file (.getAbsolutePath f)}
      (sci/eval-string* ctx s))))

(defn exec-code
  "Execute code and print results. `code` is a string. Acceptable code includes
  Datalevin functions and some Clojure core functions."
  [code]
  (let [reader (sci/reader code)]
    (sci/with-bindings {sci/ns @sci/ns}
      (loop []
        (let [next-form (sci/parse-next ctx reader)]
          (when-not (= ::sci/eof next-form)
            (prn (eval-fn ctx next-form))
            (recur)))))))

;; inter-fn

(def ^:private disallowed-inter-fn-core-symbols
  '#{agent
     alias
     all-ns
     await
     await-for
     bean
     alter-var-root
     binding
     bound-fn
     bound-fn*
     create-ns
     declare
     def
     defmacro
     defmethod
     defmulti
     defn
     defonce
     defrecord
     deftype
     deftype*
     extend
     extend-protocol
     extend-type
     eval
     find-ns
     find-var
     future
     future-call
     get-thread-bindings
     import
     import*
     in-ns
     intern
     load
     load-file
     load-reader
     load-string
     loaded-libs
     locking
     macroexpand
     macroexpand-1
     memfn
     ns
     ns-aliases
     ns-imports
     ns-interns
     ns-map
     ns-publics
     ns-refers
     ns-resolve
     ns-unalias
     ns-unmap
     pcalls
     pmap
     promise
     proxy
     pvalues
     push-thread-bindings
     read
     read-line
     read-string
     refer
     reify
     reify*
     remove-ns
     require
     requiring-resolve
     resolve
     set!
     send
     send-off
     shutdown-agents
     slurp
     spit
     the-ns
     use
     var
     var-get
     var-set
     with-bindings
     with-bindings*
     with-open})

(def ^:private disallowed-inter-fn-core-symbol-set
  (into disallowed-inter-fn-core-symbols
        (map #(symbol "clojure.core" (name %)))
        disallowed-inter-fn-core-symbols))

(def ^:private allowed-inter-fn-core-api
  '#{add
     cardinality
     count-datoms
     datom
     datom-a
     datom-e
     datom-v
     datom?
     datoms
     db?
     entid
     entity
     entity-db
     explain
     max-eid
     pull
     pull-many
     q
     k
     read-buffer
     resolve-tempid
     retract
     rseek-datoms
     schema
     seek-datoms
     squuid
     squuid-time-millis
     tempid
     touch
     tx-data->simulated-report
     v})

(def ^:private allowed-inter-fn-bits-api
  '#{read-buffer})

(def ^:private allowed-inter-fn-lmdb-api
  '#{has-next-val
     k
     next-val
     seek-key
     v
     val-iterator})

(def ^:private allowed-inter-fn-interpret-api
  '#{inter-fn})

(def ^:private runtime-inter-fn-interpret-api
  (conj allowed-inter-fn-interpret-api 'compile-inter-fn-source))

(def ^:private allowed-inter-fn-datalevin-api
  {"datalevin.bits" allowed-inter-fn-bits-api
   "datalevin.core" allowed-inter-fn-core-api
   "datalevin.interpret" allowed-inter-fn-interpret-api
   "datalevin.lmdb" allowed-inter-fn-lmdb-api})

(def ^:private allowed-inter-fn-datalevin-namespaces
  #{"datalevin.analyzer"
    "datalevin.constants"})

(def ^:private allowed-inter-fn-host-symbols
  '#{Thread/sleep
     java.lang.Thread/sleep})

(def ^:private allowed-inter-fn-interop-symbols
  '#{.indexOf
     .put})

(def ^:private max-inter-fn-sleep-ms 1000)

(declare quote-form?)

(defn- class-like-namespace?
  [ns-part]
  (boolean
   (when ns-part
     (or (s/starts-with? ns-part "java.")
         (s/starts-with? ns-part "javax.")
         (s/starts-with? ns-part "jdk.")
         (s/starts-with? ns-part "sun.")
         (s/starts-with? ns-part "com.sun.")
         (some #(and (seq %)
                     (Character/isUpperCase ^char (first %)))
               (s/split ns-part #"\."))))))

(defn- interop-symbol?
  [sym]
  (let [n  (name sym)
        ns (namespace sym)]
    (or (= "." n)
        (= "new" n)
        (s/starts-with? n ".")
        (s/ends-with? n ".")
        (class-like-namespace? ns))))

(defn- disallowed-datalevin-symbol?
  [sym]
  (when-let [ns (namespace sym)]
    (when (s/starts-with? ns "datalevin.")
      (if-let [allowed-api (allowed-inter-fn-datalevin-api ns)]
        (not (contains? allowed-api (symbol (name sym))))
        (not (contains? allowed-inter-fn-datalevin-namespaces ns))))))

(defn- quoted-query-dot-syntax?
  [quoted? sym]
  (and quoted?
       (nil? (namespace sym))
       (#{"." "..."} (name sym))))

(defn- validate-inter-fn-symbol!
  [quoted? sym]
  (when (or (contains? disallowed-inter-fn-core-symbol-set sym)
            (disallowed-datalevin-symbol? sym)
            (and (not (contains? allowed-inter-fn-host-symbols sym))
                 (not (contains? allowed-inter-fn-interop-symbols sym))
                 (not (quoted-query-dot-syntax? quoted? sym))
                 (interop-symbol? sym)))
    (u/raise "Disallowed inter-fn symbol " sym
             {:type   :datalevin/disallowed-inter-fn-symbol
              :symbol sym})))

(defn- validate-inter-fn-code!
  ([form]
   (validate-inter-fn-code! false form))
  ([quoted? form]
   (cond
     (symbol? form)
     (validate-inter-fn-symbol! quoted? form)

     (seq? form)
     (if (quote-form? form)
       (validate-inter-fn-code! true (second form))
       (doseq [x form]
         (validate-inter-fn-code! quoted? x)))

     (map? form)
     (doseq [[k v] form]
       (validate-inter-fn-code! quoted? k)
       (validate-inter-fn-code! quoted? v))

     (coll? form)
     (doseq [x form]
       (validate-inter-fn-code! quoted? x))

     :else nil)))

(defn- qualify-code
  [form]
  (if (quote-form? form)
    form
    (w/walk qualify-code qualify-fn form)))

(defn- filter-used
  "Only keep referred locals in the form"
  [locals [args & body]]
  (let [args (set args)
        used (reduce (fn [coll s]
                       (if-not (or (qualified-symbol? s) (args s))
                         (conj coll s)
                         coll))
                     #{}
                     (filter symbol? (flatten body)))]
    (set/intersection (set locals) used)))

(defn- save-env
  "Borrowed some pieces from https://github.com/technomancy/serializable-fn"
  [locals form]
  (let [form        (cons 'fn (qualify-code (rest form)))
        quoted-form `(quote ~form)]
    (if locals
      `(list `let [~@(for [local   (filter-used locals (rest form)),
                           let-arg [`(quote ~local)
                                    `(list `quote ~local)]]
                       let-arg)]
             ~quoted-form)
      quoted-form)))

(defn- quote-form?
  [x]
  (and (seq? x)
       (= 'quote (first x))
       (= 2 (count x))))

(defn- fn-source-form?
  [x]
  (and (seq? x)
       (#{'fn 'clojure.core/fn} (first x))
       (or (vector? (second x))
           (and (symbol? (second x))
                (vector? (nth x 2 nil))))))

(defn- literal-let-source-form?
  [x]
  (and (seq? x)
       (#{'let 'clojure.core/let} (first x))
       (= 3 (count x))
       (let [bindings (second x)]
         (and (vector? bindings)
              (even? (count bindings))
              (every?
               (fn [[sym value]]
                 (and (symbol? sym)
                      (quote-form? value)))
               (partition 2 bindings))))
       (fn-source-form? (nth x 2 nil))))

(defn- inter-fn-source-form?
  [x]
  (or (fn-source-form? x)
      (literal-let-source-form? x)))

(defn- host-var-namespace?
  [ns-part]
  (and ns-part
       (not (s/starts-with? ns-part "clojure."))
       (not (s/starts-with? ns-part "datalevin."))
       (not (class-like-namespace? ns-part))))

(defn- host-var-symbol?
  [sym]
  (let [ns-part (namespace sym)]
    (and (qualified-symbol? sym)
         (host-var-namespace? ns-part)
         (not (contains? allowed-inter-fn-host-symbols sym))
         (not (contains? allowed-inter-fn-interop-symbols sym))
         (not (interop-symbol? sym)))))

(defn- host-var-symbols
  [form]
  (letfn [(collect [form acc]
            (cond
              (symbol? form)
              (if (host-var-symbol? form)
                (conj acc form)
                acc)

              (seq? form)
              (if (quote-form? form)
                acc
                (reduce (fn [acc x] (collect x acc)) acc form))

              (map? form)
              (reduce-kv (fn [acc k v]
                           (collect v (collect k acc)))
                         acc
                         form)

              (coll? form)
              (reduce (fn [acc x] (collect x acc)) acc form)

              :else acc))]
    (collect form #{})))

(defn- resolve-public-host-var
  [sym]
  (when-let [ns (some-> sym namespace symbol find-ns)]
    (get (ns-publics ns) (symbol (name sym)))))

(defn- host-var-namespace-map
  [[ns-sym syms]]
  (let [sci-ns (sci/create-ns ns-sym)]
    [ns-sym
     (reduce
      (fn [m sym]
        (let [var-sym (symbol (name sym))
              v       (or (resolve-public-host-var sym)
                          (u/raise
                           "Cannot resolve host var for inter-fn " sym
                           {:type   :datalevin/unresolved-inter-fn-host-var
                            :symbol sym}))]
          (assoc m var-sym
                 (sci/new-var (symbol v) v
                              (assoc (meta v)
                                     :sci.impl/built-in true
                                     :ns sci-ns)))))
      {}
      syms)]))

(defn- host-var-namespaces
  [src]
  (into {}
        (map host-var-namespace-map)
        (group-by (comp symbol namespace) (host-var-symbols src))))

(defn- inter-fn-context
  [src]
  (let [host-namespaces (host-var-namespaces src)]
    (if (seq host-namespaces)
      (sci/init {:namespaces (merge-with merge
                                         (inter-fn-namespaces)
                                         host-namespaces)
                 :classes    inter-fn-classes})
      inter-fn-ctx)))

(defn ^:no-doc validate-inter-fn-source!
  [src]
  (when-not (inter-fn-source-form? src)
    (u/raise "Invalid inter-fn source form"
             {:type :datalevin/invalid-inter-fn-source
              :source src}))
  (validate-inter-fn-code! src)
  src)

(defn ^:no-doc compile-inter-fn-source
  "Compile validated inter-fn source with the restricted inter-fn interpreter."
  [src]
  (let [src (validate-inter-fn-source! src)]
    (with-meta
      (sci/eval-form (inter-fn-context src) src)
      {:type   :datalevin/inter-fn
       :source src})))

(defn- source->inter-fn
  "Convert a source form to get an inter-fn"
  [src]
  (compile-inter-fn-source src))

(defmacro inter-fn
  "Same signature as `fn`. Create a function that can be serialized in
  source code form.

  Such a function can be used as an input in Datalevin queries or
  transactions, e.g. as a filtering predicate or as a transaction
  function, and be stored in the database. This function can also be
  sent over the wire if the database is on a remote server or as a
  babashka pod. It runs in an interpreter.

  Symbols referred in inter-fn need to be fully-qualified. Public vars from
  loaded application namespaces can be called by qualified symbol."
  [_args & _body]
  `(compile-inter-fn-source ~(save-env (keys &env) &form)))

(defn inter-fn?
  "Return true if `x` is an `inter-fn`"
  [x]
  (= (:type (meta x)) :datalevin/inter-fn))

(defmacro definterfn
  "Create a named `inter-fn`"
  [fn-name args & body]
  `(def ~fn-name (inter-fn ~args ~@body)))

(hext/register-user-tag!
 9                                      ; subtag 9 = inter-fn (wire id 0x10000009)
 AFn
 (fn write-inter-fn [^com.s_exp.hako.Writer w ^AFn x]
   (if (inter-fn? x)
     (.writeAny w (:source (meta x)))
     (u/raise "Can only freeze an inter-fn" {:x x})))
 (fn read-inter-fn [^com.s_exp.hako.Reader r]
   (source->inter-fn (.readAny r))))

(defmethod print-method :datalevin/inter-fn [f, ^Writer w]
  (.write w "#datalevin/inter-fn ")
  (binding [*out* w] (p/pprint (:source (meta f)))))

(defn inter-fn-from-reader
  "Read a printed `inter-fn` back in."
  [x]
  (source->inter-fn x))

(def ^:no-doc sci-opts
  {:namespaces (merge (user-facing-vars) (additional-vars))
   :classes    {:allow                     :all
                'Thread                    java.lang.Thread
                'java.lang.Thread          java.lang.Thread
                'java.text.Normalizer      java.text.Normalizer
                'java.text.Normalizer$Form java.text.Normalizer$Form
                'org.tartarus.snowball.SnowballStemmer
                org.tartarus.snowball.SnowballStemmer}})

(defn- selected-vars-map
  [ns syms]
  (available-map ns (select-keys (ns-publics ns) syms) (constantly true)))

(defn- inter-fn-sleep
  [ms]
  (let [ms (long ms)]
    (when-not (<= 0 ms max-inter-fn-sleep-ms)
      (u/raise "inter-fn Thread/sleep exceeds allowed bound"
               {:type   :datalevin/inter-fn-sleep-out-of-bounds
                :ms     ms
                :max-ms max-inter-fn-sleep-ms}))
    (Thread/sleep ms)))

(defn- host-vars-map
  [ns]
  (let [sci-ns (sci/create-ns ns)]
    {'sleep (sci/new-var (symbol (name ns) "sleep")
                         inter-fn-sleep
                         {:sci.impl/built-in true
                          :ns sci-ns})}))

(defn- inter-fn-namespaces
  []
  {'clojure.core
   (zipmap disallowed-inter-fn-core-symbols
           (repeat nil))
   'datalevin.analyzer
   (additional-map 'datalevin.analyzer
                   (ns-publics 'datalevin.analyzer))
   'datalevin.constants
   (user-facing-map 'datalevin.constants
                    (ns-publics 'datalevin.constants))
   'datalevin.core
   (selected-vars-map 'datalevin.core allowed-inter-fn-core-api)
   'datalevin.bits
   (selected-vars-map 'datalevin.bits allowed-inter-fn-bits-api)
   'datalevin.lmdb
   (selected-vars-map 'datalevin.lmdb allowed-inter-fn-lmdb-api)
   'datalevin.interpret
   (selected-vars-map 'datalevin.interpret runtime-inter-fn-interpret-api)
   'Thread
   (host-vars-map 'Thread)
   'java.lang.Thread
   (host-vars-map 'java.lang.Thread)})

(def ^:private inter-fn-classes
  {'java.util.HashMap java.util.HashMap})

(def ^:no-doc inter-fn-sci-opts
  {:namespaces (inter-fn-namespaces)
   :classes    inter-fn-classes})

(def ^:no-doc ctx (sci/init sci-opts))

(def ^:no-doc inter-fn-ctx (sci/init inter-fn-sci-opts))
