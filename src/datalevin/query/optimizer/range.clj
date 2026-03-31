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
   [clojure.string :as str]
   [clojure.walk :as w]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.query-util :as qu]
   [datalevin.util :as u])
  (:import
   [java.util Arrays]
   [datalevin.parser Predicate]
   [datalevin.utl LikeFSM]))

(defn- pushdownable
  "Predicates that can be pushed down involve only one free variable."
  [where gseq]
  (when (instance? Predicate where)
    (let [{:keys [args]} where
          syms           (qu/collect-vars args)]
      (when (= (count syms) 1)
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

(defn- prefix-max-string
  [^String prefix]
  (let [n (alength (.getBytes prefix))]
    (if (< n c/+val-bytes-wo-hdr+)
      (let [l  (- c/+val-bytes-wo-hdr+ n)
            ba (byte-array l)]
        (Arrays/fill ba (unchecked-byte 0xFF))
        (str prefix (String. ba)))
      prefix)))

(def ^:const wildm (int \%))
(def ^:const wilds (int \_))
(def ^:const max-string (b/text-ba->str c/max-bytes))

(defn- like-convert-range
  "Turn wildcard-free prefix into range."
  [m ^String pattern not?]
  (let [wm-s (.indexOf pattern wildm)
        ws-s (.indexOf pattern wilds)]
    (cond
      (or (zero? wm-s) (zero? ws-s)) m
      (== wm-s ws-s -1)
      (add-range m [[:closed ""] [:open pattern]]
                 [[:open pattern] [:closed max-string]])
      :else
      (let [min-s    (min wm-s ws-s)
            end      (if (== min-s -1) (max wm-s ws-s) min-s)
            prefix-s (subs pattern 0 end)
            prefix-e (prefix-max-string prefix-s)
            range    [[:closed prefix-s] [:closed prefix-e]]]
        (if not?
          (apply add-range m (flip-ranges [range] "" max-string))
          (add-range m range))))))

(defn- like-pattern-as-string
  "Used for plain text matching, e.g. as bounded val or range, not as FSM."
  [^String pattern escape]
  (let [esc (str (or escape \!))]
    (-> pattern
        (str/replace (str esc esc) esc)
        (str/replace (str esc "%") "%")
        (str/replace (str esc "_") "_"))))

(defn- wildcard-free-like-pattern
  [^String pattern {:keys [escape]}]
  (LikeFSM/isValid (.getBytes pattern) (or escape \!))
  (let [pstring (like-pattern-as-string pattern escape)]
    (when (and (not (str/includes? pstring "%"))
               (not (str/includes? pstring "_")))
      pstring)))

(defn activate-var-pred
  [{:keys [make-call resolve-pred]} var clause]
  (when clause
    (if (fn? clause)
      clause
      (let [[f & args] clause
            idxs       (u/idxs-of #(= var %) args)
            ni         (count idxs)
            idxs-arr   (int-array idxs)
            args-arr   (object-array args)
            call       (make-call (resolve-pred f nil))]
        (fn var-pred [x]
          (dotimes [i ni]
            (aset args-arr (aget idxs-arr i) x))
          (call args-arr))))))

(defn add-pred
  ([old-pred new-pred]
   (add-pred old-pred new-pred false))
  ([old-pred new-pred or?]
   (if new-pred
     (if old-pred
       (if or?
         (fn [x] (or (old-pred x) (new-pred x)))
         (fn [x] (and (old-pred x) (new-pred x))))
       new-pred)
     old-pred)))

(defn- optimize-like
  [helpers m pred [_ ^String pattern {:keys [escape]}] v not?]
  (let [pstring (like-pattern-as-string pattern escape)
        m'      (update m :pred add-pred (activate-var-pred helpers v pred))]
    (like-convert-range m' pstring not?)))

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
        fn-arr   (object-array len)
        args-arr (object-array args)
        call     ((:make-call helpers) ((:resolve-pred helpers) f nil))]
    (dotimes [i len]
      (let [arg (aget args-arr i)]
        (when (list? arg)
          (aset fn-arr i (if (some list? arg)
                           (nested-pred helpers (first arg) (rest arg) v)
                           (activate-var-pred helpers v arg))))))
    (fn [x]
      (dotimes [i len]
        (when-some [f (aget fn-arr i)]
          (aset args-arr i (f x))))
      (call args-arr))))

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

(defn- add-pred-clause
  [helpers graph clause v]
  (let [pred        (first clause)
        and-clauses (split-and-clauses pred)
        preds       (or and-clauses [pred])
        apply-pred  (fn [m pred]
                      (let [[f & args] pred]
                        (if (some list? pred)
                          (update m :pred add-pred
                                  (nested-pred helpers f args v))
                          (case f
                            (< <= > >=) (inequality->range m f args v)
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
     graph)))

(defn- free->bound
  "Cases where free var can be rewritten as bound:
    * like pattern is free of wildcards."
  [graph clause v]
  (w/postwalk
   (fn [m]
     (if-let [free (:free m)]
       (if-let [[new k]
                (u/some-indexed
                 (fn [{:keys [var] :as old}]
                   (when (= v var)
                     (let [[f & args] (first clause)]
                       (when (= f 'like)
                         (let [[_ pattern opts] args]
                           (when-let [ps (wildcard-free-like-pattern
                                          pattern opts)]
                             (-> old
                                 (dissoc :var)
                                 (assoc :val ps))))))))
                 free)]
         (-> m
             (update :bound u/conjv new)
             (update :free u/vec-remove k))
         m)
       m))
   graph))

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
               (update :graph #(free->bound % clause v))
               (update :graph #(add-pred-clause helpers % clause v))))
         c))
     context (:qwhere parsed-q))))
