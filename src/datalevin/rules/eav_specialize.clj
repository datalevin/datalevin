;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.eav-specialize
  "Recognition and execution of linear, transitive, and synchronized EAV rules."
  (:require
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules.clause
    :refer [rule-args rule-call? rule-head source values-for-var]]
   [datalevin.rules.eav
    :refer [add-adjacency-value! build-eav-adjacency distinct-vars?
            eav-link-plan simple-eav-clause?]]
   [datalevin.rules.relation :refer [map-rule-result]]
   [datalevin.util :refer [concatv]])
  (:import
   [datalevin.db DB]
   [org.eclipse.collections.impl.list.mutable FastList]
   [org.eclipse.collections.impl.list.mutable.primitive LongArrayList]
   [org.eclipse.collections.impl.map.mutable.primitive LongObjectHashMap]
   [org.eclipse.collections.impl.set.mutable.primitive LongHashSet]
   [java.util List HashMap HashSet ArrayDeque BitSet]))

(defn- transitive-eav-base-plan
  "Recognize a binary rule base branch backed by one EAV relation. The
   returned entity position describes which logical rule argument occupies
   the physical E position."
  [branch]
  (let [[head & clauses] branch
        head-vars        (vec (rest head))]
    (when (and (= 2 (count head-vars))
               (every? qu/binding-var? head-vars)
               (distinct-vars? head-vars)
               (= 1 (count clauses))
               (simple-eav-clause? (first clauses)))
      (let [[e attr v] (first clauses)]
        (cond
          (= [e v] head-vars) {:attr attr, :entity-pos 0}
          (= [v e] head-vars) {:attr attr, :entity-pos 1})))))

(defn- logical-eav-ends
  [clause entity-pos]
  (let [[e _ v] clause]
    (if (zero? ^long entity-pos) [e v] [v e])))

(defn- transitive-eav-recursive-branch?
  [context rule-name branch attr entity-pos]
  (let [[head & clauses] branch
        head-vars        (vec (rest head))
        eavs             (filterv simple-eav-clause? clauses)
        calls            (filterv
                           #(and (sequential? %)
                                 (rule-call? context %)
                                 (= rule-name (rule-head %)))
                           clauses)]
    (when (and (= 2 (count head-vars))
               (every? qu/binding-var? head-vars)
               (distinct-vars? head-vars)
               (= 2 (count clauses))
               (= 1 (count eavs))
               (= 1 (count calls))
               (nil? (source (first calls))))
      (let [eav       (first eavs)
            call-args (vec (rule-args (first calls)))
            [left right] head-vars
            [edge-left edge-right] (logical-eav-ends eav entity-pos)]
        (and (= attr (second eav))
             (= 2 (count call-args))
             (let [[call-left call-right] call-args]
               (or
                 ;; edge(left, mid), tc(mid, right)
                 (and (= edge-left left)
                      (= edge-right call-left)
                      (= call-right right)
                      (qu/binding-var? edge-right)
                      (not= edge-right left)
                      (not= edge-right right))
                 ;; tc(left, mid), edge(mid, right)
                 (and (= call-left left)
                      (= call-right edge-left)
                      (= edge-right right)
                      (qu/binding-var? edge-left)
                      (not= edge-left left)
                      (not= edge-left right)))))))))

(defn transitive-eav-rule-plan
  [context rule-name]
  (let [branches (get-in context [:rules rule-name])]
    (when (= 2 (count branches))
      (let [base-plans (keep (fn [branch]
                               (when-not (some #(and (sequential? %)
                                                     (rule-call? context %))
                                               (rest branch))
                                 (when-let [plan
                                            (transitive-eav-base-plan branch)]
                                   [branch plan])))
                             branches)]
        (when (= 1 (count base-plans))
          (let [[base-branch {:keys [attr entity-pos]}] (first base-plans)
                rec-branches (remove #(identical? % base-branch) branches)]
            (when (and (= 1 (count rec-branches))
                       (transitive-eav-recursive-branch?
                         context rule-name (first rec-branches)
                         attr entity-pos))
              {:attr       attr
               :entity-pos entity-pos
               :head-vars  (vec (rest (ffirst branches)))})))))))

(defn singleton-bound-argument
  [context arg]
  (if (qu/free-var? arg)
    (let [values (values-for-var context arg)]
      (when (= 1 (count values))
        {:value (first values)}))
    {:value arg}))

(defn- reverse-linear-eav-links
  [links]
  (mapv (fn [link]
          (update link :bound-side #(case % :e :v :v :e)))
        (rseq (vec links))))

(defn linear-eav-rule-path
  "Return the ref-EAV path from the first head argument to the second for a
   single-branch, nonrecursive binary rule. Nested eligible rules are flattened
   without crossing unions, predicates, or other semantic boundaries."
  ([context rule-name]
   (linear-eav-rule-path context rule-name #{}))
  ([context rule-name seen]
   (when-not (contains? seen rule-name)
     (let [branches (get-in context [:rules rule-name])]
       (when (= 1 (count branches))
         (let [branch    (first branches)
               head      (first branch)
               head-vars (vec (rule-args head))
               seen      (conj seen rule-name)]
           (when (and (= rule-name (rule-head head))
                      (nil? (source head))
                      (= 2 (count head-vars))
                      (every? qu/binding-var? head-vars)
                      (distinct-vars? head-vars))
             (letfn [(clause-segment [clause]
                       (cond
                         (simple-eav-clause? clause)
                         (let [[from attr to] clause]
                           (when (not= from to)
                             {:from from
                              :to to
                              :links [{:attr attr :bound-side :e}]}))

                         (and (sequential? clause)
                              (rule-call? context clause)
                              (nil? (source clause)))
                         (let [nested-name (rule-head clause)
                               nested-args (vec (rule-args clause))]
                           (when (and (= 2 (count nested-args))
                                      (every? qu/binding-var? nested-args)
                                      (distinct-vars? nested-args))
                             (when-let [links
                                        (linear-eav-rule-path
                                          context nested-name seen)]
                               {:from (nth nested-args 0)
                                :to (nth nested-args 1)
                                :links links})))))]
               (let [segments (mapv clause-segment (rest branch))]
                 (when (and (seq segments) (every? some? segments))
                   (loop [current   (nth head-vars 0)
                          remaining segments
                          links     []]
                     (if (empty? remaining)
                       (when (= current (nth head-vars 1)) links)
                       (when-not (= current (nth head-vars 1))
                         (let [matches
                               (into []
                                     (keep-indexed
                                       (fn [idx {:keys [from to]}]
                                         (when (or (= current from)
                                                   (= current to))
                                           idx)))
                                     remaining)]
                           (when (= 1 (count matches))
                             (let [idx     (long (first matches))
                                   segment (nth remaining idx)
                                   forward? (= current (:from segment))
                                   next-node (if forward?
                                               (:to segment)
                                               (:from segment))
                                   next-links (if forward?
                                                (:links segment)
                                                (reverse-linear-eav-links
                                                  (:links segment)))]
                               (recur next-node
                                      (concatv (take idx remaining)
                                               (drop (inc idx) remaining))
                                      (into links next-links))))))))))))))))))

(def ^:const ^long bound-transitive-full-scan-min-observations 8)

(def ^:const ^long bound-transitive-full-scan-min-pending 128)

(def ^:const ^double bound-transitive-full-scan-work-ratio 0.5)

(defn build-long-eav-adjacency
  [^DB database attr bound-side]
  (db/ref-attr-adjacency database attr bound-side))

(defn add-bound-transitive-tuple!
  [^FastList tuples ^long neighbor]
  (.add tuples (object-array [(Long/valueOf neighbor)])))

(defn singleton-bound-binary-result
  [^FastList tuples args ^long bound-idx]
  (let [free-var (nth args (bit-xor (int bound-idx) 1))]
    ;; The specialized plan is admitted only when the other argument is free
    ;; and the bound argument has exactly one value in the surrounding
    ;; context. Keep that singleton in its existing input relation and expose
    ;; only the genuinely new column, avoiding a redundant one-key join.
    (r/with-unique-key
      (r/relation! {free-var 0} tuples)
      [free-var])))

(defn transitive-output-saturated?
  [^FastList tuples output-domain-size]
  (and output-domain-size
       (= (.size tuples) (long output-domain-size))))

(def ^:private ^:const bound-linear-full-scan-threshold 128)

(defn- cached-long-eav-adjacency
  [^HashMap cache ^DB db attr bound-side]
  (let [k [attr bound-side]]
    (or (.get cache k)
        (let [adjacency (build-long-eav-adjacency db attr bound-side)]
          (.put cache k adjacency)
          adjacency))))

(defn- advance-bound-linear-frontier
  [^DB db ^HashMap adjacency-cache ^longs frontier
   {:keys [attr bound-side]} pending-tx?]
  (let [next       (LongHashSet.)
        full-scan? (and (not pending-tx?)
                        (db/local-ref-attr-adjacency? db)
                        (>= (alength frontier)
                            bound-linear-full-scan-threshold))]
    (if full-scan?
      (let [adjacency ^LongObjectHashMap
            (cached-long-eav-adjacency adjacency-cache db attr bound-side)]
        (dotimes [i (alength frontier)]
          (when-let [values ^LongArrayList
                     (.get adjacency (aget frontier i))]
            (dotimes [j (.size values)]
              (.add next (.get values j))))))
      (dotimes [i (alength frontier)]
        (let [node      (aget frontier i)
              pattern   (if (= bound-side :e)
                          [node attr nil]
                          [nil attr node])
              neighbors ^List (db/-search-tuples db pattern)]
          (when neighbors
            (dotimes [j (.size neighbors)]
              (.add next
                    (long (aget ^objects (.get neighbors j) 0))))))))
    (.toArray next)))

(defn eval-bound-linear-eav
  [{:keys [^DB db head-vars links ^long bound-idx bound-value
           traversal-value pending-tx?]}
   args]
  (let [links       (if (zero? bound-idx)
                      links
                      (reverse-linear-eav-links links))
        cache       (HashMap.)
        ^longs final-frontier
        (loop [^longs frontier (long-array [(long traversal-value)])
               remaining       (seq links)]
          (if (or (nil? remaining) (zero? (alength frontier)))
            frontier
            (recur (advance-bound-linear-frontier
                     db cache frontier (first remaining) pending-tx?)
                   (next remaining))))
        tuples      (FastList. (alength final-frontier))]
    (dotimes [i (alength final-frontier)]
      (let [output (Long/valueOf (aget final-frontier i))]
        (.add tuples
              (if (zero? bound-idx)
                (object-array [bound-value output])
                (object-array [output bound-value])))))
    (map-rule-result
      (r/relation! (zipmap head-vars (range)) tuples)
      head-vars args)))

(defn- synchronized-eav-recursive-plan
  "Recognize p(X,Y) :- left(X,Z), p(Z,Z1), right(Y,Z1), including
   physically reversed EAV links. Each head position must connect to the same
   recursive-call position so a singleton-bound side remains bound throughout
   demand evaluation."
  [context rule-name branch]
  (let [[head & clauses] branch
        head-vars       (vec (rest head))
        eavs            (filterv simple-eav-clause? clauses)
        calls           (filterv
                          #(and (sequential? %)
                                (rule-call? context %)
                                (= rule-name (rule-head %)))
                          clauses)]
    (when (and (= 2 (count head-vars))
               (every? qu/binding-var? head-vars)
               (distinct-vars? head-vars)
               (= 3 (count clauses))
               (= 2 (count eavs))
               (= 1 (count calls))
               (nil? (source (first calls))))
      (let [call-args (vec (rule-args (first calls)))
            head-pos  (zipmap head-vars (range))
            call-pos  (zipmap call-args (range))
            links     (mapv #(eav-link-plan head-pos call-pos %) eavs)]
        (when (and (= 2 (count call-args))
                   (every? qu/binding-var? call-args)
                   (distinct-vars? call-args)
                   (every? some? links)
                   (= #{0 1} (set (map :head-pos links)))
                   (every? #(= (:head-pos %) (:call-pos %)) links))
          {:links (vec (sort-by :head-pos links))})))))

(defn synchronized-eav-rule-plan
  [context rule-name]
  (let [branches (get-in context [:rules rule-name])]
    (when (= 2 (count branches))
      (let [base-plans
            (keep (fn [branch]
                    (when-not (some #(and (sequential? %)
                                          (rule-call? context %))
                                    (rest branch))
                      (when-let [plan (transitive-eav-base-plan branch)]
                        [branch plan])))
                  branches)]
        (when (= 1 (count base-plans))
          (let [[base-branch base] (first base-plans)
                recursive-branches
                (remove #(identical? % base-branch) branches)]
            (when (= 1 (count recursive-branches))
              (when-let [recursive
                         (synchronized-eav-recursive-plan
                           context rule-name (first recursive-branches))]
                {:base      base
                 :head-vars (vec (rest (ffirst branches)))
                 :links     (:links recursive)}))))))))

(def ^:private ^:const full-synchronized-max-bitmap-bytes
  (* 64 1024 1024))

(def ^:private ^:const full-transitive-max-bitmap-bytes
  (* 64 1024 1024))

(defn- add-ordinal!
  ^long [^HashMap ordinals ^FastList values value]
  (if-let [^Number ordinal (.get ordinals value)]
    (.longValue ordinal)
    (let [ordinal (.size values)]
      (.put ordinals value (Integer/valueOf ordinal))
      (.add values value)
      ordinal)))

(defn- collect-eav-domain!
  [^HashMap ordinals ^FastList values ^List tuples]
  (when tuples
    (dotimes [i (.size tuples)]
      (let [^objects tuple (.get tuples i)]
        (add-ordinal! ordinals values (aget tuple 0))
        (add-ordinal! ordinals values (aget tuple 1))))))

(defn dense-synchronized-domain?
  [^long domain-size]
  (let [words (quot (+ domain-size 63) 64)
        ;; Result, current delta, next delta, and the two recursive-link
        ;; adjacencies are the maximum simultaneously live dense matrices.
        bytes (* words 8 domain-size 5)]
    (and (pos? domain-size)
         (<= bytes full-synchronized-max-bitmap-bytes))))

(defn- dense-transitive-domain?
  [^long domain-size]
  (let [words         (quot (+ domain-size 63) 64)
        bytes-per-row (* words 8)]
    (and (pos? domain-size)
         (pos? bytes-per-row)
         (<= domain-size
             (quot full-transitive-max-bitmap-bytes bytes-per-row)))))

(defn- bitset-row!
  ^BitSet [^objects rows ^long row-idx ^long domain-size]
  (or (aget rows row-idx)
      (let [row (BitSet. (int domain-size))]
        (aset rows row-idx row)
        row)))

(defn- build-base-bitset-rows
  [^List tuples ^HashMap ordinals ^long entity-pos ^long domain-size]
  (let [rows (object-array domain-size)]
    (when tuples
      (dotimes [i (.size tuples)]
        (let [^objects tuple (.get tuples i)
              x              (if (zero? entity-pos)
                               (aget tuple 0) (aget tuple 1))
              y              (if (zero? entity-pos)
                               (aget tuple 1) (aget tuple 0))
              ^Number x-idx  (.get ordinals x)
              ^Number y-idx  (.get ordinals y)]
          (.set (bitset-row! rows (.longValue x-idx) domain-size)
                (.intValue y-idx)))))
    rows))

(defn- transitive-bitset-closure!
  [^objects rows ^long domain-size]
  ;; BitSet Warshall retains set semantics while turning each dense row update
  ;; into a word-wise union instead of producing one tuple per path proof.
  (dotimes [k domain-size]
    (when-let [^BitSet via (aget rows k)]
      (dotimes [i domain-size]
        (when-let [^BitSet row (aget rows i)]
          (when (.get row k)
            (.or row via))))))
  rows)

(defn- build-link-bitset-rows
  [^List tuples ^HashMap ordinals bound-side ^long domain-size]
  (let [rows      (object-array domain-size)
        bound-idx (if (= bound-side :e) 0 1)
        out-idx   (bit-xor bound-idx 1)]
    (when tuples
      (dotimes [i (.size tuples)]
        (let [^objects tuple   (.get tuples i)
              ^Number row-idx (.get ordinals (aget tuple bound-idx))
              ^Number out     (.get ordinals (aget tuple out-idx))]
          (.set (bitset-row! rows (.longValue row-idx) domain-size)
                (.intValue out)))))
    rows))

(defn- synchronized-bitset-fixed-point!
  [^objects results ^objects left-rows ^objects right-rows
   ^long domain-size]
  ;; The current delta may share its rows with `results`: all propagation for
  ;; a round finishes before results are mutated with the next delta.
  (loop [^objects delta (aclone results)]
    (let [next-rows (object-array domain-size)]
      (dotimes [z domain-size]
        (when-let [^BitSet delta-row (aget delta z)]
          (when-not (.isEmpty delta-row)
            (let [expanded (BitSet. (int domain-size))]
              ;; Lift all new right-hand recursive values through the second
              ;; EAV link once for this row.
              (loop [z1 (.nextSetBit delta-row 0)]
                (when-not (neg? z1)
                  (when-let [^BitSet outputs (aget right-rows z1)]
                    (.or expanded outputs))
                  (recur (.nextSetBit delta-row
                                      (unchecked-inc-int z1)))))
              ;; Every caller of z receives that complete lifted row. This is
              ;; a word-wise union instead of one hash insertion per proof.
              (when-not (.isEmpty expanded)
                (when-let [^BitSet callers (aget left-rows z)]
                  (loop [x (.nextSetBit callers 0)]
                    (when-not (neg? x)
                      (.or (bitset-row! next-rows x domain-size) expanded)
                      (recur (.nextSetBit callers
                                          (unchecked-inc-int x)))))))))))
      (let [changed? (boolean-array 1)]
        (dotimes [x domain-size]
          (when-let [^BitSet candidates (aget next-rows x)]
            (when-let [^BitSet known (aget results x)]
              (.andNot candidates known))
            (if (.isEmpty candidates)
              (aset next-rows x nil)
              (do
                (aset changed? 0 true)
                (if-let [^BitSet known (aget results x)]
                  (.or known candidates)
                  (aset results x candidates))))))
        (when (aget changed? 0)
          (recur next-rows)))))
  results)

(defn- emit-synchronized-bitset-relation
  [^objects rows ^FastList values head-vars]
  (let [domain-size (.size values)
        ^long output-size
        (loop [x 0, total 0]
          (if (< x domain-size)
            (let [^BitSet row (aget rows x)]
              (recur (unchecked-inc-int x)
                     (+ total (long (if row (.cardinality row) 0)))))
            total))
        tuples (FastList. (int (min output-size Integer/MAX_VALUE)))]
    (dotimes [x domain-size]
      (when-let [^BitSet row (aget rows x)]
        (loop [y (.nextSetBit row 0)]
          (when-not (neg? y)
            (.add tuples
                  (object-array [(.get values x) (.get values y)]))
            (recur (.nextSetBit row (unchecked-inc-int y)))))))
    (r/relation! (zipmap head-vars (range)) tuples)))

(def ^:private ^:const full-linear-max-bitmap-bytes
  (* 64 1024 1024))

(defn- add-long-ordinal!
  ^long [^HashMap ordinals ^LongArrayList values ^long value]
  (let [boxed (Long/valueOf value)]
    (if-let [^Number ordinal (.get ordinals boxed)]
      (.longValue ordinal)
      (let [ordinal (.size values)]
        (.put ordinals boxed (Integer/valueOf ordinal))
        (.add values value)
        ordinal))))

(defn- collect-long-adjacency-values!
  [^LongObjectHashMap adjacency ^HashMap ordinals
   ^LongArrayList values]
  (let [^longs keys (.toArray (.keySet adjacency))]
    (dotimes [i (alength keys)]
      (let [neighbors ^LongArrayList (.get adjacency (aget keys i))]
        (dotimes [j (.size neighbors)]
          (add-long-ordinal! ordinals values (.get neighbors j)))))))

(defn- dense-full-linear-domain?
  [^long domain-size adjacencies]
  (let [words      (long (quot (+ domain-size 63) 64))
        row-bytes  (long (* words 8))
        row-counts (mapv #(.size ^LongObjectHashMap %) adjacencies)
        peak-rows
        (loop [i 0, peak (long 0)]
          (if (< i (dec (count row-counts)))
            (let [rows (+ (long (nth row-counts i))
                          (long (nth row-counts (inc i))))]
              (recur (unchecked-inc-int i) (max peak rows)))
            peak))]
    (and (pos? domain-size)
         (pos? row-bytes)
         (<= (long peak-rows)
             (quot full-linear-max-bitmap-bytes row-bytes)))))

(defn- terminal-linear-bitset-rows
  [^LongObjectHashMap adjacency ^HashMap ordinals ^long domain-size]
  (let [rows        (LongObjectHashMap. (.size adjacency))
        ^longs keys (.toArray (.keySet adjacency))]
    (dotimes [i (alength keys)]
      (let [key       (aget keys i)
            neighbors ^LongArrayList (.get adjacency key)
            row       (BitSet. (int domain-size))]
        (dotimes [j (.size neighbors)]
          (let [^Number ordinal
                (.get ordinals (Long/valueOf (.get neighbors j)))]
            (.set row (.intValue ordinal))))
        (when-not (.isEmpty row)
          (.put rows key row))))
    rows))

(defn- prepend-linear-bitset-rows
  [^LongObjectHashMap adjacency ^LongObjectHashMap suffix
   ^long domain-size]
  (let [rows        (LongObjectHashMap. (.size adjacency))
        ^longs keys (.toArray (.keySet adjacency))]
    (dotimes [i (alength keys)]
      (let [key       (aget keys i)
            neighbors ^LongArrayList (.get adjacency key)
            row       (BitSet. (int domain-size))]
        (dotimes [j (.size neighbors)]
          (when-let [reachable ^BitSet (.get suffix (.get neighbors j))]
            (.or row reachable)))
        (when-not (.isEmpty row)
          (.put rows key row))))
    rows))

(defn- emit-full-linear-relation
  [^LongObjectHashMap rows ^LongArrayList endpoint-values head-vars]
  (let [^longs starts (.toArray (.keySet rows))
        output-size
        (loop [i 0, total (long 0)]
          (if (< i (alength starts))
            (recur (unchecked-inc-int i)
                   (+ total
                      (long (.cardinality
                              ^BitSet (.get rows (aget starts i))))))
            total))
        tuples (FastList. (int (min (long output-size)
                                    (long Integer/MAX_VALUE))))]
    (dotimes [i (alength starts)]
      (let [start (Long/valueOf (aget starts i))
            row   ^BitSet (.get rows (aget starts i))]
        (loop [ordinal (.nextSetBit row 0)]
          (when-not (neg? ordinal)
            (.add tuples
                  (object-array
                    [start (Long/valueOf (.get endpoint-values ordinal))]))
            (recur (.nextSetBit row (unchecked-inc-int ordinal)))))))
    (r/relation! (zipmap head-vars (range)) tuples)))

(defn eval-full-linear-eav
  [{:keys [^DB db head-vars links]} args]
  (let [adjacencies
        (mapv #(build-long-eav-adjacency db (:attr %) (:bound-side %)) links)
        terminal   ^LongObjectHashMap (peek adjacencies)
        ordinals   (HashMap.)
        endpoints  (LongArrayList.)]
    (collect-long-adjacency-values! terminal ordinals endpoints)
    (let [domain-size (.size endpoints)]
      (cond
        (zero? domain-size)
        (map-rule-result
          (r/relation! (zipmap head-vars (range)) (FastList.))
          head-vars args)

        (dense-full-linear-domain? domain-size adjacencies)
        (let [terminal-rows
              (terminal-linear-bitset-rows terminal ordinals domain-size)
              rows
              (loop [idx    (- (count adjacencies) 2)
                     suffix terminal-rows]
                (if (neg? idx)
                  suffix
                  (recur (dec idx)
                         (prepend-linear-bitset-rows
                           (nth adjacencies idx) suffix domain-size))))]
          (map-rule-result
            (emit-full-linear-relation rows endpoints head-vars)
            head-vars args))

        :else nil))))

(defn eval-full-transitive-eav
  [{:keys [^DB db attr ^long entity-pos head-vars]} args]
  (let [tuples   ^List (or (db/-search-tuples db [nil attr nil])
                           (FastList.))
        ordinals (HashMap.)
        values   (FastList.)]
    (collect-eav-domain! ordinals values tuples)
    (let [domain-size (.size values)]
      (when (dense-transitive-domain? domain-size)
        (let [rows (build-base-bitset-rows
                     tuples ordinals entity-pos domain-size)]
          (transitive-bitset-closure! rows domain-size)
          (map-rule-result
            (emit-synchronized-bitset-relation rows values head-vars)
            head-vars args))))))

(defn eval-full-synchronized-eav
  [{:keys [^DB db base links head-vars]} args]
  (let [tuple-cache (HashMap.)
        tuples-for  (fn [attr]
                      (or (.get tuple-cache attr)
                          (let [tuples (or (db/-search-tuples
                                            db [nil attr nil])
                                           (FastList.))]
                            (.put tuple-cache attr tuples)
                            tuples)))
        ^List base-tuples (tuples-for (:attr base))
        ^List left-tuples (tuples-for (:attr (nth links 0)))
        ^List right-tuples (tuples-for (:attr (nth links 1)))
        ordinals (HashMap.)
        values   (FastList.)]
    (collect-eav-domain! ordinals values base-tuples)
    (collect-eav-domain! ordinals values left-tuples)
    (collect-eav-domain! ordinals values right-tuples)
    (let [domain-size (.size values)]
      (when (dense-synchronized-domain? domain-size)
        (let [results (build-base-bitset-rows
                        base-tuples ordinals (:entity-pos base) domain-size)
              left    (build-link-bitset-rows
                        left-tuples ordinals (:bound-side (nth links 0))
                        domain-size)
              right   (build-link-bitset-rows
                        right-tuples ordinals (:bound-side (nth links 1))
                        domain-size)]
          (synchronized-bitset-fixed-point! results left right domain-size)
          (map-rule-result
            (emit-synchronized-bitset-relation results values head-vars)
            head-vars args))))))

(defn- opposite-eav-side
  [side]
  (case side :e :v :v :e))

(defn- local-eav-adjacency
  [^HashMap cache ^DB db attr side]
  (let [k [attr side]]
    (or (.get cache k)
        (let [adjacency (build-eav-adjacency db attr side)]
          (.put cache k adjacency)
          adjacency))))

(defn- add-pending-values!
  [^HashMap pending ^HashSet queued ^ArrayDeque queue node ^List values
   ^HashSet seen]
  (let [existing ^FastList (.get pending node)
        delta    (or existing (FastList.))
        before   (.size delta)]
    (dotimes [i (.size values)]
      (let [value (.get values i)]
        (when (.add seen value)
          (.add delta value))))
    (when (< before (.size delta))
      (when-not existing
        (.put pending node delta))
      (when (.add queued node)
        (.addLast queue node)))))

(defn eval-bound-synchronized-eav
  [{:keys [^DB db base links head-vars ^long bound-idx bound-value
           traversal-value]}
   args]
  (let [free-idx        (bit-xor (int bound-idx) 1)
        bound-link      (nth links bound-idx)
        free-link       (nth links free-idx)
        cache           (HashMap.)
        base-side       (if (= bound-idx (:entity-pos base)) :e :v)
        ^HashMap base-index
        (local-eav-adjacency cache db (:attr base) base-side)
        ^HashMap demand-index
        (local-eav-adjacency
          cache db (:attr bound-link)
          (opposite-eav-side (:bound-side bound-link)))
        ^HashMap output-index
        (local-eav-adjacency
          cache db (:attr free-link) (:bound-side free-link))
        demanded        (HashSet.)
        discover        (ArrayDeque.)
        dependents      (HashMap.)]
    ;; Discover only recursive bound values reachable from the singleton
    ;; demand, and record which callers depend on each recursive call.
    (.add demanded traversal-value)
    (.add discover traversal-value)
    (while (not (.isEmpty discover))
      (let [node      (.removeFirst discover)
            successors ^List (.get demand-index node)]
        (when successors
          (dotimes [i (.size successors)]
            (let [successor (.get successors i)]
              (add-adjacency-value! dependents successor node)
              (when (.add demanded successor)
                (.addLast discover successor)))))))

    ;; Seed every demanded subproblem with its base facts. Thereafter each new
    ;; result is propagated exactly once to the callers that depend on it.
    (let [results (HashMap.)
          pending (HashMap.)
          queued  (HashSet.)
          queue   (ArrayDeque.)]
      (doseq [node demanded]
        (when-let [values ^List (.get base-index node)]
          (let [seen (HashSet.)]
            (.put results node seen)
            (add-pending-values! pending queued queue node values seen))))
      (while (not (.isEmpty queue))
        (let [node       (.removeFirst queue)
              _          (.remove queued node)
              delta      ^List (.remove pending node)
              callers    ^List (.get dependents node)]
          (when (and delta callers)
            (dotimes [i (.size callers)]
              (let [caller       (.get callers i)
                    seen         (or (.get results caller)
                                     (let [seen (HashSet.)]
                                       (.put results caller seen)
                                       seen))
                    existing     ^FastList (.get pending caller)
                    next-delta   (or existing (FastList.))
                    before       (.size next-delta)]
                (dotimes [j (.size delta)]
                  (let [outputs ^List (.get output-index (.get delta j))]
                    (when outputs
                      (dotimes [k (.size outputs)]
                        (let [output (.get outputs k)]
                          (when (.add ^HashSet seen output)
                            (.add next-delta output)))))))
                (when (< before (.size next-delta))
                  (when-not existing
                    (.put pending caller next-delta))
                  (when (.add queued caller)
                    (.addLast queue caller))))))))
      (let [answers ^HashSet (.get results traversal-value)
            tuples  (FastList. (int (if answers (.size answers) 0)))]
        (when answers
          (doseq [answer answers]
            (.add tuples
                  (if (zero? bound-idx)
                    (object-array [bound-value answer])
                    (object-array [answer bound-value])))))
        (map-rule-result
          (r/relation! (zipmap head-vars (range)) tuples)
          head-vars args)))))
