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
   [clojure.walk :as w]
   [datalevin.constants :as c]
   [datalevin.query.predicate :as qpred]
   [datalevin.query-util :as qu]
   [datalevin.util :as u])
  (:import
   [java.nio.charset StandardCharsets]
   [datalevin.utl LikeFSM]
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
    (assoc m :range (if old-range
                      (if-let [new-range (intersect-ranges old-range rs)]
                        new-range
                        :empty-range)
                      (combine-ranges rs)))))

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
          (let [upper (prefix-successor prefix)]
            (if (and upper (not (inline-string-bound? upper)))
              m
              (add-range m [[:closed prefix]
                            (if upper [:open upper] [:closed c/vmax])])))))
      m)))

(defn activate-var-pred
  [{:keys [make-call resolve-pred]} var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            idxs       (u/idxs-of #(= var %) args)
            ni         (count idxs)
            idxs-arr   (int-array idxs)
            args       (object-array args)
            call       (make-call (resolve-pred f nil))
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
  index encoding starts with an inexact double approximation."
  [value-type]
  (not (identical? :db.type/bigdec value-type)))

(defn- inexact-inequality-range-attr?
  [helpers source attr]
  (when-let [attr-value-type (:attr-value-type helpers)]
    (not (exact-inequality-range? (attr-value-type source attr)))))

(defn- optimize-like
  [helpers m pred [input pattern {:keys [escape]}] v not?]
  ;; Validate constant patterns before choosing a range. An empty scan may
  ;; never invoke the residual matcher that would otherwise reject them.
  (when (and (= input v) (string? pattern))
    (LikeFSM/isValid (.getBytes ^String pattern StandardCharsets/UTF_8)
                     (or escape \!)))
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

(defn range->inequality
  [v [[so sc :as s] [eo ec :as e]]]
  (cond
    (= s [:closed c/v0])
    (if (identical? eo :open) (list '< v ec) (list '<= v ec))
    (= e [:closed c/vmax])
    (if (identical? so :open) (list '< sc v) (list '<= sc v))
    :else
    (if (identical? so :open) (list '< sc v ec) (list '<= sc v ec))))

(defn- equality->range
  [m args]
  (let [c (some #(when-not (qu/free-var? %) %) args)]
    (add-range m [[:closed c] [:closed c]])))

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

(defn- add-pred-clause-to-source
  [helpers source nodes clause v]
  (let [pred        (first clause)
        and-clauses (split-and-clauses pred)
        preds       (or and-clauses [pred])
        apply-pred  (fn [m pred]
                      (let [[f & args] pred]
                        (if (some list? pred)
                          (update m :pred add-pred
                                  (nested-pred helpers f args v))
                          (case f
                            (< <= > >=)
                            (if (inexact-inequality-range-attr?
                                  helpers source (:attr m))
                              (update m :pred add-pred
                                      (activate-var-pred helpers v pred))
                              (inequality->range m f args v))
                            =           (equality->range m args)
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

(defn pushdown-predicates
  "Optimization that pushes predicates down to value scans."
  [{:keys [parsed-q graph] :as context} helpers]
  (let [gseq (tree-seq coll? seq graph)]
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
         c))
     context (:qwhere parsed-q))))
