;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.optimizer.range
  "Range and predicate pushdown helpers."
  (:require
   [clojure.set :as set]
   [clojure.walk :as w]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.query.predicate :as qpred]
   [datalevin.query-util :as qu]
   [datalevin.util :as u])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util Arrays]
   [datalevin.utl LikeFSM QueryKey]
   [datalevin.parser Predicate SrcVar]))

(defn- source-form?
  [form]
  (cond
    (instance? SrcVar form) true
    (qu/quoted-form? form) false
    (qu/source? form)      true
    (map? form)            (some (fn [[k v]]
                                   (or (source-form? k)
                                       (source-form? v)))
                                 form)
    (coll? form)           (some source-form? form)
    :else                  false))

(defn- pushdownable
  "Predicates that can be pushed down involve only one free variable."
  [where gseq]
  (when (instance? Predicate where)
    (let [{:keys [args]} where
          syms           (qu/collect-vars args)]
      (when (and (= (count syms) 1)
                 (not (source-form? args)))
        (let [s (first syms)]
          (some #(when (= s (:var %)) s) gseq))))))

(defn- range-compare
  ([r1 r2]
   (range-compare r1 r2 true))
  ([[p i] [q j] from?]
   (case i
     :db.value/sysMin -1
     :db.value/sysMax 1
     (case j
       :db.value/sysMax -1
       :db.value/sysMin 1
       (let [res (compare i j)]
         (if (zero? res)
           (if from?
             (cond
               (identical? p q)       0
               (identical? p :closed) -1
               :else                  1)
             (cond
               (identical? p q)     0
               (identical? p :open) -1
               :else                1))
           res))))))

(def ^:private range-compare-to #(range-compare %1 %2 false))

(defn- combine-ranges*
  [ranges]
  (let [orig-from (sort range-compare (map first ranges))]
    (loop [intervals (transient [])
           from      (rest orig-from)
           to        (sort range-compare-to (map peek ranges))
           thread    (transient [(first orig-from)])]
      (if (seq to)
        (let [fc (first from)
              tc (first to)]
          (if (= (count from) (count to))
            (recur (conj! intervals (persistent! thread)) (rest from) to
                   (transient [fc]))
            (if fc
              (if (< ^long (range-compare fc tc) 0)
                (recur intervals (rest from) to (conj! thread fc))
                (recur intervals from (rest to) (conj! thread tc)))
              (recur intervals from (rest to) (conj! thread tc)))))
        (mapv (fn [t] [(first t) (peek t)])
              (persistent! (conj! intervals (persistent! thread))))))))

(defn combine-ranges
  [ranges]
  (reduce
   (fn [vs [[cl l] [cr r] :as n]]
     (let [[[pcl pl] [pcr pr]] (peek vs)]
       (if (and (= pr l) (not (= pcr cl :open)))
         (conj (pop vs) [[pcl pl] [cr r]])
         (conj vs n))))
   [] (combine-ranges* ranges)))

(defn- flip [c]
  (if (identical? c :open) :closed :open))

(defn flip-ranges
  ([ranges]
   (flip-ranges ranges c/v0 c/vmax))
  ([ranges v0 vmax]
   (let [vs (reduce
             (fn [vs [[cl l] [cr r]]]
               (-> vs
                   (assoc-in [(dec (count vs)) (count (peek vs))]
                             [(if (= l v0) cl (flip cl)) l])
                   (conj [[(if (= r vmax) cr (flip cr)) r]])))
             [[[:closed v0]]] ranges)]
     (assoc-in vs [(dec (count vs)) (count (peek vs))]
               [:closed vmax]))))

(defn intersect-ranges
  [& ranges]
  (let [n         (count ranges)
        ranges    (apply u/concatv ranges)
        orig-from (sort range-compare (map first ranges))
        res
        (loop [res  []
               from (rest orig-from)
               fp   (first orig-from)
               to   (sort range-compare-to (map peek ranges))
               i    1
               j    0]
          (let [tc (first to)]
            (if (seq from)
              (let [fc (first from)]
                (if (<= 0 ^long (range-compare fc tc))
                  (if (= i (+ j n))
                    (recur (conj res [fp tc]) (rest from) fc
                           (drop n to) (inc i) i)
                    (recur res (rest from) fc to (inc i) j))
                  (recur res (rest from) fc to (inc i) j)))
              (if (and (<= ^long (range-compare fp tc) 0) (= i (+ j n)))
                (conj res [fp tc])
                res))))]
    (when (seq res) res)))

(defn- add-range
  [m & rs]
  (let [old-range (:range m)]
    (assoc m :range (cond
                      (= old-range :empty-range) :empty-range
                      old-range (or (intersect-ranges old-range rs) :empty-range)
                      :else (combine-ranges rs)))))

(defn- prefix-successor
  "An exclusive upper bound for a string prefix in both UTF-8 index order and
  UTF-16 predicate order. Carry past code points with no successor in both."
  [^String prefix]
  (loop [end (.length prefix)]
    (when (pos? end)
      (let [cp (.codePointBefore prefix end)
            start (- end (Character/charCount cp))]
        ;; U+FFFF -> U+10000 reverses UTF-16 order. U+10FFFF has no
        ;; successor. A shorter prefix gives a safe, possibly wider bound.
        (if (or (= cp 0xFFFF) (= cp 0x10FFFF))
          (recur start)
          (str (.substring prefix 0 start)
               (String. (Character/toChars
                          (if (= cp 0xD7FF) 0xE000 (inc cp))))))))))

(defn- inline-string-bound?
  [^String s]
  ;; Giant keys truncate their value and append an allocated ID, so they
  ;; cannot serve as exact logical range endpoints.
  (< (alength (.getBytes s StandardCharsets/UTF_8)) c/+val-bytes-wo-hdr+))

(defn disjoint-scan-ranges?
  "Prove that native scans cannot visit a value twice. Compare encoded index
  keys, including sentinels and endpoint inclusion. Unknown encodings and giant
  bounds retain deduplication; a single scan needs no endpoint comparison."
  [value-type ranges]
  (if (<= (count ranges) 1)
    true
    (let [native-type (case value-type
                        (:string :db.type/string) :db.type/string
                        (:long :db.type/long) :db.type/long
                        (:ref :db.type/ref) :db.type/ref
                        nil)
          valid? (fn [value]
                   (or (#{c/v0 c/vmax} value)
                       (if (= native-type :db.type/string)
                         (and (string? value)
                              (.canEncode (.newEncoder StandardCharsets/UTF_8)
                                          ^String value)
                              (inline-string-bound? value))
                         (int? value))))
          cmp (fn [^bytes a ^bytes b] (Arrays/compareUnsigned a b))]
      (boolean
        (when (and native-type
                   (every? (fn [[[cl lo] [ch hi]]]
                             (and (#{:open :closed} cl) (#{:open :closed} ch)
                                  (valid? lo) (valid? hi))) ranges))
          (let [encoded (mapv (fn [interval]
                               (mapv (fn [[edge value]]
                                       [edge (b/indexable-bytes
                                               (b/indexable nil 0 value
                                                            native-type c/gmax))])
                                     interval)) ranges)]
            (and (every? (fn [[[cl lo] [ch hi]]]
                           (let [order (long (cmp lo hi))]
                             (or (neg? order)
                                 (and (zero? order) (= cl ch :closed)))))
                         encoded)
                 (every? (fn [[[_ [ch hi]] [[cl lo] _]]]
                           (let [order (long (cmp hi lo))]
                             (or (neg? order)
                                 (and (zero? order)
                                      (not= [ch cl] [:closed :closed])))))
                         (partition 2 1 (sort #(cmp (second (first %1))
                                                   (second (first %2)))
                                              encoded))))))))))

(defn- literal-prefix-range
  [^String prefix]
  (when (and (pos? (.length prefix))
             (.canEncode (.newEncoder StandardCharsets/UTF_8) prefix)
             (inline-string-bound? prefix))
    (let [upper (prefix-successor prefix)]
      (when (or (nil? upper) (inline-string-bound? upper))
        [[:closed prefix] (if upper [:open upper] [:closed c/vmax])]))))

(defn- like-convert-range
  "Restrict scans only where the literal pattern proves a safe bound.
  Escapes terminate the known prefix; the original matcher interprets them."
  [m ^String pattern escape]
  (let [escape (or escape \!)]
    (if (and (char? escape) (< (int escape) 128)
             (.canEncode (.newEncoder StandardCharsets/UTF_8) pattern))
      (let [end (long (reduce min (.length pattern)
                              (filter #(<= 0 ^long %)
                                      [(.indexOf pattern (int \%))
                                       (.indexOf pattern (int \_))
                                       (.indexOf pattern (int escape))])))
            exact? (= end (.length pattern))
            prefix (.substring pattern 0 end)]
        (cond
          (not (inline-string-bound? prefix)) m

          exact?
          (add-range m [[:closed pattern] [:closed pattern]])

          (zero? end) m

          :else
          (if-let [range (literal-prefix-range prefix)]
            (add-range m range)
            m)))
      m)))

(defn activate-var-pred
  [{:keys [make-call resolve-pred prepare-call]} var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            idxs       (u/idxs-of #(= var %) args)
            ni         (count idxs)
            idxs-arr   (int-array idxs)
            args       (object-array args)
            resolved   (resolve-pred f nil)
            call       (if prepare-call
                         (prepare-call resolved args (set idxs))
                         (make-call resolved))
            factory
            (fn []
              (let [args-arr (aclone ^objects args)]
                (fn var-pred [x]
                  (dotimes [i ni]
                    (aset args-arr (aget idxs-arr i) x))
                  (call args-arr))))]
        (qpred/forkable-predicate factory)))))

(defn add-pred
  ([old-pred new-pred]
   (add-pred old-pred new-pred false))
  ([old-pred new-pred or?]
   (qpred/combine-predicates old-pred new-pred or?)))

(defn exact-inequality-range?
  "Whether AVE ordering exactly implements Datalog inequality ordering for a
  value type. BigDecimal values retain a residual predicate because their
  index encoding starts with an inexact double approximation. Generic data
  uses serialization order, which need not match logical value comparison."
  [value-type]
  (not (contains? #{nil :data :db.type/data :db.type/bigdec} value-type)))

(defn- inexact-inequality-range-attr?
  [helpers source attr]
  (when-let [attr-value-type (:attr-value-type helpers)]
    (not (exact-inequality-range? (attr-value-type source attr)))))

(defn- optimize-like
  [helpers m pred [input pattern {:keys [escape]}] v not?]
  ;; Validate constant patterns before choosing a range. An empty scan may
  ;; never invoke the residual matcher that would otherwise reject them.
  (when (and (= input v) (string? pattern))
    (LikeFSM/isValid ^String pattern (char (or escape \!))))
  (let [m' (update m :pred add-pred (activate-var-pred helpers v pred))]
    ;; A literal prefix is only a superset of LIKE matches. Complementing
    ;; it for NOT LIKE would discard valid rows before the predicate runs.
    ;; Keep negation as a residual filter, including alongside other ranges.
    (if (and (not not?) (= input v) (string? pattern))
      (like-convert-range m' pattern escape)
      m')))

(defn- inequality->range
  [m f args v]
  (let [args (vec args)
        ac-1 (dec (count args))
        i    ^long (u/index-of #(= % v) args)
        fa   (first args)
        pa   (peek args)]
    (case f
      <  (cond
           (== 0 i)   (add-range m [[:closed c/v0] [:open pa]])
           (= i ac-1) (add-range m [[:open fa] [:closed c/vmax]])
           :else      (add-range m [[:open fa] [:open pa]]))
      <= (cond
           (== 0 i)   (add-range m [[:closed c/v0] [:closed pa]])
           (= i ac-1) (add-range m [[:closed fa] [:closed c/vmax]])
           :else      (add-range m [[:closed fa] [:closed pa]]))
      >  (cond
           (== 0 i)   (add-range m [[:open pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:open fa]])
           :else      (add-range m [[:open pa] [:open fa]]))
      >= (cond
           (== 0 i)   (add-range m [[:closed pa] [:closed c/vmax]])
           (= i ac-1) (add-range m [[:closed c/v0] [:closed fa]])
           :else      (add-range m [[:closed pa] [:closed fa]])))))

(defn range->inequalities
  "Return predicates whose conjunction preserves both range endpoints."
  [v [[so sc :as s] [eo ec :as e]]]
  (let [lower-op (if (identical? so :open) '< '<=)
        upper-op (if (identical? eo :open) '< '<=)]
    (cond
      (= s [:closed c/v0]) [(list upper-op v ec)]
      (= e [:closed c/vmax]) [(list lower-op sc v)]
      (= so eo) [(list lower-op sc v ec)]
      ;; A chained comparison can only represent matching endpoint kinds.
      :else [(list lower-op sc v) (list upper-op v ec)])))

(defn- equality->range
  [m args]
  (let [c (some #(when-not (qu/free-var? %) %) args)]
    (add-range m [[:closed c] [:closed c]])))

(defn ^:no-doc constant-predicate-ranges
  "Lower one admitted variable/constant predicate without metadata, reads or
  function resolution. The caller proves index eligibility and retains any
  logical residual required by its encoding."
  [operator variable constant variable-first?]
  (let [args (if variable-first? [variable constant] [constant variable])]
    (if (= '= operator)
      [[[:closed constant] [:closed constant]]]
      (:range (inequality->range {} operator args variable)))))

(defn- in-convert-range
  [m [_ coll] not?]
  (assert (and (coll? coll) (not (map? coll)))
          "function `in` expects a collection")
  (apply add-range m
         (let [ranges (map (fn [v] [[:closed v] [:closed v]]) (sort coll))]
           (if not? (flip-ranges ranges) ranges))))

(defn- nested-pred
  [helpers f args v]
  (let [len      (count args)
        fn-preds (object-array len)
        args     (object-array args)
        call     ((:make-call helpers) ((:resolve-pred helpers) f nil))]
    (dotimes [i len]
      (let [arg (aget args i)]
        (when (list? arg)
          (aset fn-preds i (if (some list? arg)
                             (nested-pred helpers (first arg) (rest arg) v)
                             (activate-var-pred helpers v arg))))))
    (let [factory
          (fn []
            (let [args-arr (aclone ^objects args)
                  fn-arr   (qpred/fork-predicates fn-preds)]
              (fn [x]
                (dotimes [i len]
                  (when-some [f (aget fn-arr i)]
                    (aset args-arr i (f x))))
                (call args-arr))))]
      (if (every? qpred/forkable-predicate? fn-preds)
        (qpred/forkable-predicate factory)
        (factory)))))

(defn- split-and-clauses
  "If pred is an (and ...) form where every arg is a predicate list,
  return a flat seq of those predicate lists; otherwise nil."
  [pred]
  (when (and (list? pred) (= 'and (first pred)))
    (let [args (rest pred)]
      (when (every? list? args)
        (mapcat (fn [arg]
                  (or (split-and-clauses arg) [arg]))
                args)))))

(defn- equality-union-values
  [form v]
  (when (seq? form)
    (let [[op & args] form]
      (case op
        = (when (= 2 (count args))
            (let [[left right] args
                  value (cond (= left v) right (= right v) left)]
              (when (and (some? value) (not (qu/free-var? value))
                         (not (coll? value)) (not (source-form? value)))
                [value])))
        or (when (seq args)
             (let [values (mapv #(equality-union-values % v) args)]
               (when (every? some? values) (into [] cat values))))
        nil))))

(defn- exact-equality-value?
  [value-type value]
  (case value-type
    (:db.type/long :db.type/ref) (int? value)
    :db.type/string (and (string? value) (inline-string-bound? value))
    :db.type/keyword (and (keyword? value) (inline-string-bound? (str value)))
    :db.type/boolean (boolean? value)
    :db.type/uuid (uuid? value)
    :db.type/instant (= java.util.Date (class value))
    false))

(defn- equality-union-range
  "Use point ranges only for a complete equality union in an exact native
  domain. Unknown branches, decimal scale, signed zero and serialized values
  keep the original predicate; no inferred bound may discard their matches."
  [helpers source m pred v]
  (when-let [attr-value-type (:attr-value-type helpers)]
    (when-let [values (equality-union-values pred v)]
      (let [value-type (attr-value-type source (:attr m))]
        (when (every? #(exact-equality-value? value-type %) values)
          (in-convert-range m [v values] false))))))

(defn- add-pred-clause-to-source
  [helpers source nodes clause v]
  (let [pred        (first clause)
        and-clauses (split-and-clauses pred)
        preds       (or and-clauses [pred])
        apply-pred  (fn [m pred]
                      (let [[f & args] pred]
                        (if (some list? pred)
                          (or (when (= f 'or)
                                (equality-union-range helpers source m pred v))
                              (update m :pred add-pred
                                      (nested-pred helpers f args v)))
                          (case f
                            (< <= > >=)
                            (if (inexact-inequality-range-attr?
                                  helpers source (:attr m))
                              (update m :pred add-pred
                                      (activate-var-pred helpers v pred))
                              (inequality->range m f args v))
                            =
                            (if (some #(QueryKey/containsBytes %) args)
                              ;; Serialized data bounds are not logical equality
                              ;; bounds. Keep content equality in the scan filter.
                              (update m :pred add-pred
                                      (activate-var-pred helpers v pred))
                              (equality->range m args))
                            like        (optimize-like helpers m pred args v false)
                            not-like    (optimize-like helpers m pred args v true)
                            in          (in-convert-range m args false)
                            not-in      (in-convert-range m args true)
                            (update m :pred add-pred
                                    (activate-var-pred helpers v pred))))))]
    (w/postwalk
     (fn [m]
       (if (= (:var m) v)
         (reduce apply-pred m preds)
         m))
     nodes)))

(defn- add-pred-clause
  [helpers graph clause v]
  (reduce-kv
   (fn [graph source nodes]
     (assoc graph source (add-pred-clause-to-source helpers source nodes clause v)))
   {} graph))

(defn- implied-equalities
  "Project Boolean equality filters onto individual variables. Conjunctions
  retain every known restriction; disjunctions retain only variables restricted
  in every branch. Unknown expressions impose no restriction."
  [form]
  (when (list? form)
    (let [[op & args] form]
      (case op
        = (when (= 2 (count args))
            (let [[left right] args
                  [v value] (if (qu/binding-var? left)
                              [left right] [right left])]
              (when (and (qu/binding-var? v)
                         (not (qu/free-var? value))
                         (not (coll? value))
                         (not (source-form? value)))
                {v (list '= v value)})))
        and (reduce #(merge-with (fn [a b] (list 'and a b))
                                 %1 (implied-equalities %2))
                    {} args)
        or (when (seq args)
             (reduce
               (fn [known branch]
                 (let [other (implied-equalities branch)]
                   (reduce-kv
                     (fn [m v pred]
                       (if-let [other-pred (get other v)]
                         (assoc m v (list 'or pred other-pred))
                         m))
                     {} known)))
               (or (implied-equalities (first args)) {}) (next args)))
        nil))))

(defn- pushdown-implied-equalities
  [context helpers clause gseq]
  (reduce-kv
    (fn [context v pred]
      (if (some #(= v (:var %)) gseq)
        ;; Keep equality as a residual predicate: serialized index ordering
        ;; need not implement logical equality (e.g. bytes or numeric types).
        (let [inferred [(list 'or pred)]]
          (-> context
              (update :opt-clauses conj inferred)
              (update :graph #(add-pred-clause helpers % inferred v))))
        context))
    context (implied-equalities (first clause))))

(defn- total-prefix-expression
  "Recognize a prefix that is total on strings. Plain subs can throw on short
  inputs and must not acquire a range that hides that exception."
  [form]
  (when (and (vector? form) (= 2 (count form)))
    (let [[call output] form
          [op input start end] (when (list? call) call)]
      (when (and (= op 'subs) (= 4 (count call)) (= start 0)
                 (qu/binding-var? input) (qu/binding-var? output)
                 (list? end) (= 'min (first end)) (= 3 (count end)))
        (let [[a b] (rest end)
              [n length] (if (int? a) [a b] [b a])]
          (when (and (int? n) (<= 0 (long n)) (= length (list 'count input)))
            {:input input :output output :length n}))))))

(def ^:private ^:const max-prefix-alternatives 64)

(defn- string-domain
  "A necessary finite string domain. Every disjunct must constrain the output;
  a conjunct can supply a restriction while other conjuncts remain residual."
  ([form var] (string-domain form var false))
  ([form var complete?]
   (let [bounded #(when (and (some? %) (<= (count %) max-prefix-alternatives)) %)]
     (when (list? form)
       (let [[op & args] form]
         (case op
           = (when (= 2 (count args))
               (let [[a b] args]
                 (cond
                   (and (= a var) (string? b)) #{b}
                   (and (= b var) (string? a)) #{a})))
           in (when (and (= 2 (count args)) (= var (first args))
                         (coll? (second args)) (not (map? (second args)))
                         (or (not complete?)
                             (vector? (second args)) (set? (second args)))
                         (<= (count (second args)) max-prefix-alternatives)
                         (every? string? (second args)))
                (set (second args)))
           and (let [domains (mapv #(string-domain % var complete?) args)]
                 (when (and (seq domains)
                            (or (not complete?) (every? some? domains)))
                   (when-let [known (seq (keep identity domains))]
                     (reduce set/intersection known))))
           or (let [domains (mapv #(string-domain % var complete?) args)]
                (when (and (seq domains) (every? some? domains))
                  (bounded (reduce set/union domains))))
           nil))))))

(defn- prefix-output-ranges
  [^long length values]
  (loop [values (seq values) ranges []]
    (if-let [^String value (first values)]
      (let [n (.length value)]
        (cond
          ;; A truncated prefix cannot equal a longer constant.
          (< length n) (recur (next values) ranges)
          (not (and (.canEncode (.newEncoder StandardCharsets/UTF_8) value)
                    (inline-string-bound? value))) nil
          ;; A shorter output means the complete input is that string.
          (< n length) (recur (next values)
                             (conj ranges [[:closed value] [:closed value]]))
          (zero? n) nil ; length zero produces "" for every string
          :else (when-let [range (literal-prefix-range value)]
                  (recur (next values) (conj ranges range)))))
      (if (seq ranges) (combine-ranges ranges) :empty-range))))

(defn- ascii-prefix-domain?
  [length values]
  (and (pos? length)
         (every? (fn [^String value]
                   (and (<= (.length value) length)
                        (inline-string-bound? value)
                        (every? #(<= 32 (int %) 126) value)))
                 values)))

(defn- exact-prefix-predicate?
  [form output length]
  (when-let [values (string-domain form output true)]
    (ascii-prefix-domain? length values)))

(defn- guarded-prefix-ranges
  [^long length values]
  (if (seq values)
    (combine-ranges
      (mapv (fn [^String value]
              (if (< (.length value) length)
                [[:closed value] [:closed value]]
                ;; Native string keys have trailing flags. NUL suffixes can
                ;; sort before the bare prefix, including the upper prefix.
                ;; Widen the lower bound; a native predicate or retained
                ;; residual handles both edges. Bounds remain necessary when a
                ;; lookup reconstructs logical inequality predicates.
                (let [last-index (dec (.length value))
                      lower (str (.substring value 0 last-index)
                                 (char (dec (int (.charAt value last-index)))))]
                  [[:open lower] [:open (prefix-successor value)]])))
            values))
    :empty-range))

(defn- prefix-domain-predicate
  [^long length values]
  (let [prefixes (vec (sort values))
        n (count prefixes)]
    (qpred/shareable-predicate
      (fn [^String value]
        (loop [i 0]
          (if (< i n)
            (let [^String prefix (nth prefixes i)]
              (if (if (= (.length prefix) length)
                    (.startsWith value prefix)
                    (.equals value prefix))
                true
                (recur (inc i))))
            false))))))

(defn- remove-proven-prefix-work
  [{:keys [parsed-q late-clauses] :as context} producer predicates]
  (let [{:keys [output length]} (total-prefix-expression producer)
        proven (into #{}
                     (comp (filter #(exact-prefix-predicate? % output length))
                           (map vector)) predicates)
        remaining (into [] (remove proven) late-clauses)
        ;; Ignore the predicates just proved, while retaining all other uses,
        ;; including scoped clauses, find/with, inputs and function bindings.
        used? (or (contains? (qu/collect-vars
                              [(dissoc parsed-q :qwhere :qorig-where)
                               (:rels context)]) output)
                  (some #(contains? (qu/collect-vars %) output)
                        (remove (conj proven producer)
                                (:qorig-where parsed-q))))]
    (if (empty? proven)
      context
      (cond-> (assoc context :late-clauses remaining)
        (not used?) (update :late-clauses #(into [] (remove #{producer}) %))
        used? (update :range-proven-prefixes (fnil conj #{}) producer)))))

(defn defer-prefix-projections
  "Compute a proven total string projection after adjacent absence filters
  that do not mention its output. Keep scoped negation dependencies intact,
  and never move a binding that also filters an already-bound output."
  [clauses proven initial-bound]
  (loop [todo (seq clauses) bound initial-bound result []]
    (if-let [clause (first todo)]
      (let [{:keys [input output]} (when (contains? proven clause)
                                    (total-prefix-expression clause))
            [filters tail]
            (when (and input (contains? bound input)
                       (not (contains? bound output)))
              (split-with #(and (seq? %) (#{'not 'not-join} (first %))
                                (not (contains? (qu/collect-vars %) output)))
                          (next todo)))]
        (if (seq filters)
          (recur tail (conj bound output)
                 (conj (into result filters) clause))
          (recur (next todo) (into bound (qu/collect-vars clause))
                 (conj result clause))))
      result)))

(defn- pushdown-prefix-expressions
  [{:keys [parsed-q] :as context} helpers]
  (let [forms (:qorig-where parsed-q)
        producers (keep #(when-let [expression (total-prefix-expression %)]
                           (assoc expression :form %)) forms)]
    (if (seq producers)
      (let [predicates (into []
                             (keep (fn [[parsed form]]
                                     (when (instance? Predicate parsed)
                                       (first form))))
                             (map vector (:qwhere parsed-q) forms))]
        (reduce
          (fn [context {:keys [input output length form]}]
            (if-let [domains (seq (keep #(string-domain % output) predicates))]
              (let [values (reduce set/intersection domains)
                    guarded? (some #(exact-prefix-predicate? % output length)
                                   predicates)
                    guard (when guarded? (prefix-domain-predicate length values))]
               (if-let [ranges (if (ascii-prefix-domain? length values)
                                 (guarded-prefix-ranges length values)
                                 (prefix-output-ranges length values))]
                (let [restricted? (volatile! false)
                      context
                      (update context :graph
                        (fn [graph]
                          (into {}
                                (map (fn [[source nodes]]
                                       [source
                                        (w/postwalk
                                          (fn [m]
                                            (if (and (map? m) (= input (:var m))
                                                     (#{:string :db.type/string}
                                                       (when-let [value-type
                                                                  (:attr-value-type helpers)]
                                                         (value-type source (:attr m)))))
                                              (do
                                                (vreset! restricted? true)
                                                (if (= ranges :empty-range)
                                                  (assoc m :range :empty-range)
                                                  (cond-> (apply add-range m ranges)
                                                    guard (update :pred add-pred guard))))
                                              m))
                                          nodes)]))
                                graph)))]
                  (if @restricted?
                    (remove-proven-prefix-work context form predicates)
                    context))
                context))
              context))
          context producers))
      context)))

(defn pushdown-predicates
  "Push unary predicates and implied equality restrictions into value scans.
  Multi-variable predicates remain late to enforce correlations between vars."
  [{:keys [parsed-q graph] :as context} helpers]
  (let [context (pushdown-prefix-expressions context helpers)
        gseq (tree-seq coll? seq graph)]
    (u/reduce-indexed
     (fn [c where i]
       (if-let [v (pushdownable where gseq)]
         (let [clause (nth (:qorig-where parsed-q) i)]
           (-> c
               (update :late-clauses #(remove #{clause} %))
               (update :opt-clauses conj clause)
               ;; Keep :var even for exact LIKE so projection and later joins
               ;; retain the binding. A singleton range still uses the index.
               (update :graph #(add-pred-clause helpers % clause v))))
         (if (and (instance? Predicate where)
                  (< 1 (count (qu/collect-vars (:args where))))
                  (not (source-form? (:args where))))
           (pushdown-implied-equalities
             c helpers (nth (:qorig-where parsed-q) i) gseq)
           c)))
     context (:qwhere parsed-q))))
