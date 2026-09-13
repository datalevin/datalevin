;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.rules.eav
  "Compiled EAV branch evaluation, cached adjacency, and projected deduplication."
  (:require
   [datalevin.db :as db]
   [datalevin.query-util :as qu]
   [datalevin.relation :as r]
   [datalevin.rules.clause :refer [rule-args rule-call? rule-head]])
  (:import
   [datalevin.utl ArrayUtil]
   [datalevin.db DB]
   [datalevin.relation Relation]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.util List HashMap HashSet]))

(defn distinct-vars?
  [vars]
  (= (count vars) (count (distinct vars))))

(defn simple-eav-clause?
  [clause]
  (and (vector? clause)
       (= 3 (count clause))
       (keyword? (second clause))
       (qu/binding-var? (first clause))
       (qu/binding-var? (nth clause 2))))

(defn eav-link-plan
  [head-pos call-pos clause]
  (let [[e attr v] clause
        e-head     (head-pos e)
        v-head     (head-pos v)
        e-call     (call-pos e)
        v-call     (call-pos v)]
    (cond
      (and (some? e-head) (nil? e-call) (some? v-call) (nil? v-head))
      {:attr attr, :head-pos e-head, :call-pos v-call, :bound-side :v}

      (and (some? v-head) (nil? v-call) (some? e-call) (nil? e-head))
      {:attr attr, :head-pos v-head, :call-pos e-call, :bound-side :e})))

(def ^:private output-source-call 0)

(def ^:private output-source-eav 1)

(defn- compile-head-sources
  [head-vars call-pos eav-plans]
  (let [n        (count head-vars)
        types    (long-array n)
        idxs     (int-array n)
        eav-mask (boolean-array n)]
    (doseq [[i {:keys [head-pos]}] (map-indexed vector eav-plans)]
      (aset-boolean eav-mask (int head-pos) true)
      (aset-long types (int head-pos) (long output-source-eav))
      (aset-int idxs (int head-pos) (int i)))
    (loop [i 0]
      (if (< i n)
        (if (aget eav-mask i)
          (recur (unchecked-inc i))
          (let [head-var (nth head-vars i)]
            (when-let [call-idx (call-pos head-var)]
              (aset-long types i (long output-source-call))
              (aset-int idxs i (int call-idx))
              (recur (unchecked-inc i)))))
        {:types types, :idxs idxs}))))

(defn- linear-eav-branch-plan
  [context branch]
  (let [head       (first branch)
        head-vars  (vec (rest head))
        clauses    (vec (rest branch))
        rule-calls (filterv #(and (sequential? %) (rule-call? context %))
                            clauses)
        eavs       (filterv simple-eav-clause? clauses)]
    (when (and (seq head-vars)
               (<= 1 (count eavs) 2)
               (= 1 (count rule-calls))
               (= (count clauses) (inc (count eavs)))
               (every? qu/binding-var? head-vars)
               (distinct-vars? head-vars))
      (let [call      (first rule-calls)
            rname     (rule-head call)
            rel       (get (:rule-rels context) rname)
            branches  (get (:rules context) rname)
            rhead     (when branches (vec (rest (ffirst branches))))
            call-args (vec (rule-args call))
            source    (get-in context [:sources '$])]
        (when (and rel
                   source
                   (db/-searchable? source)
                   (= (count rhead) (count call-args))
                   (every? qu/binding-var? call-args)
                   (distinct-vars? call-args)
                   (every? #(contains? (:attrs rel) %) rhead))
          (let [rel-attrs     (:attrs rel)
                call-rel-idxs (int-array
                                (map (fn [v] (int (rel-attrs v))) rhead))
                head-pos      (zipmap head-vars (range))
                call-pos      (zipmap call-args (range))
                eav-plans     (mapv #(eav-link-plan head-pos call-pos %) eavs)]
            (when (and (= (count eavs) (count eav-plans))
                       (every? some? eav-plans)
                       (= (count eav-plans)
                          (count (distinct (map :head-pos eav-plans)))))
              (let [head-source (compile-head-sources head-vars call-pos
                                                      eav-plans)]
                (when head-source
                  {:db            source
                   :call-rule     rname
                   :delta-rel     rel
                   :call-rel-idxs call-rel-idxs
                   :head-vars     head-vars
                   :head-types    (:types head-source)
                   :head-idxs     (:idxs head-source)
                   :eav-plans     eav-plans})))))))))

(defn add-adjacency-value!
  [^HashMap adjacency bound-value out-value]
  (if-let [^FastList values (.get adjacency bound-value)]
    (.add values out-value)
    (let [values (FastList.)]
      (.add values out-value)
      (.put adjacency bound-value values))))

(defn build-eav-adjacency
  [^DB db attr bound-side]
  (let [adjacency (HashMap.)
        tuples    ^List (db/-search-tuples db [nil attr nil])]
    (when tuples
      (let [bound-idx (case bound-side :e 0 :v 1)
            out-idx   (case bound-side :e 1 :v 0)]
        (dotimes [i (.size tuples)]
          (let [^objects tuple (.get tuples i)]
            (add-adjacency-value! adjacency
                                  (aget tuple bound-idx)
                                  (aget tuple out-idx))))))
    adjacency))

(defn- cached-eav-adjacency
  [context ^DB db {:keys [attr bound-side]}]
  (if-let [cache (:linear-eav-cache context)]
    (let [k [db attr bound-side]]
      (if (contains? @cache k)
        (get @cache k)
        (locking cache
          (if (contains? @cache k)
            (get @cache k)
            (let [adjacency (build-eav-adjacency db attr bound-side)]
              (swap! cache assoc k adjacency)
              adjacency)))))
    (build-eav-adjacency db attr bound-side)))

(defn- cached-eav-adjacencies
  [context ^DB db eav-plans]
  (let [n (count eav-plans)
        res (object-array n)]
    (dotimes [i n]
      (aset res i (cached-eav-adjacency context db (eav-plans i))))
    res))

(defn- eav-call-positions
  [eav-plans]
  (let [n   (count eav-plans)
        res (int-array n)]
    (dotimes [i n]
      (aset res i (int (:call-pos (eav-plans i)))))
    res))

(defn- call-value
  [^objects delta-tuple ^ints call-rel-idxs call-pos]
  (aget delta-tuple (aget call-rel-idxs (int call-pos))))

(def ^:private empty-group-key (Object.))

(defn- group-values!
  [^HashMap groups ^objects tuple ^ints tuple-idxs ^objects scratch lookup]
  (case (alength tuple-idxs)
    0
    (or (.get groups empty-group-key)
        (let [values (HashSet.)]
          (.put groups empty-group-key values)
          values))

    1
    (let [key (aget tuple (aget tuple-idxs 0))]
      (or (.get groups key)
          (let [values (HashSet.)]
            (.put groups key values)
            values)))

    (do
      (dotimes [i (alength tuple-idxs)]
        (aset scratch i (aget tuple (aget tuple-idxs i))))
      (or (.get groups (r/reset-array-lookup! lookup scratch))
          (let [key    (aclone scratch)
                values (HashSet.)]
            (.put groups (r/wrap-array key) values)
            values)))))

(defn- build-keyed-eav-seen
  [{:keys [^Relation delta-rel ^ints call-rel-idxs ^longs head-types
           ^ints head-idxs eav-plans]}
   ^HashMap eav-index]
  (let [head-count       (alength head-types)
        eav-head-pos     (int (:head-pos (first eav-plans)))
        key-count        (dec head-count)
        candidate-idxs   (int-array key-count)
        initial-idxs     (int-array key-count)
        valid?           (loop [head-pos 0, key-pos 0]
                           (if (< head-pos head-count)
                             (if (= head-pos eav-head-pos)
                               (recur (unchecked-inc-int head-pos) key-pos)
                               (if (= output-source-call
                                      (aget head-types head-pos))
                                 (let [call-pos (aget head-idxs head-pos)]
                                   (aset candidate-idxs key-pos
                                         (aget call-rel-idxs call-pos))
                                   (aset initial-idxs key-pos
                                         (aget call-rel-idxs head-pos))
                                   (recur (unchecked-inc-int head-pos)
                                          (unchecked-inc-int key-pos)))
                                 false))
                             true))]
    (when valid?
      (let [groups          (HashMap.)
            domain         (HashSet.)
            ^List tuples    (:tuples delta-rel)
            initial-out-idx (aget call-rel-idxs eav-head-pos)
            scratch         (object-array key-count)
            lookup          (r/array-lookup)]
        (when tuples
          (dotimes [i (.size tuples)]
            (let [^objects tuple  (.get tuples i)
                  ^HashSet values (group-values! groups tuple initial-idxs
                                                  scratch lookup)]
              (.add values (aget tuple initial-out-idx))
              (.add domain (aget tuple initial-out-idx)))))
        (doseq [^List values (.values eav-index)]
          (dotimes [i (.size values)]
            (.add domain (.get values i))))
        {:groups         groups
         :candidate-idxs candidate-idxs
         :domain-size    (.size domain)}))))

(defn- keyed-eav-seen
  [context branch plan ^HashMap eav-index]
  (when (and (:single-recursive-branch? context)
             (= 1 (count (:eav-plans plan)))
             (= (:current-rule context) (:call-rule plan)))
    (let [cache (:linear-eav-keyed-seen-cache context)
          k     [(:current-rule context) branch]]
      (if (contains? @cache k)
        (get @cache k)
        (locking cache
          (if (contains? @cache k)
            (get @cache k)
            (let [state (build-keyed-eav-seen plan eav-index)]
              (swap! cache assoc k state)
              state)))))))

(defn- add-fast-output-with-probe!
  [^FastList acc ^HashSet seen-set seen-lookup ^objects scratch
   ^longs head-types ^ints head-idxs ^objects delta-tuple ^ints call-rel-idxs
   eav0 eav1]
  (let [h (ArrayUtil/fillRuleOutputAndHash
            scratch head-types head-idxs delta-tuple call-rel-idxs eav0 eav1)]
    (when-not (.contains seen-set
                         (r/reset-array-lookup-with-hash!
                           seen-lookup scratch h))
      (let [tuple (aclone scratch)]
        (.add seen-set (r/wrap-array-with-hash tuple h))
        (.add acc tuple)))))

(defn- add-fast-output-known-new!
  [^FastList acc ^HashSet seen-set ^objects scratch ^longs head-types
   ^ints head-idxs ^objects delta-tuple ^ints call-rel-idxs eav0 eav1]
  (let [h     (ArrayUtil/fillRuleOutputAndHash
                scratch head-types head-idxs delta-tuple call-rel-idxs
                eav0 eav1)
        tuple (aclone scratch)]
    (when (.add seen-set (r/wrap-array-with-hash tuple h))
      (.add acc tuple))))

(defn eval-linear-eav-branch-with-dedup
  [context branch ^HashSet seen-set]
  (when-let [plan (linear-eav-branch-plan context branch)]
    (let [{:keys [^DB db ^Relation delta-rel ^ints call-rel-idxs head-vars
                  ^longs head-types ^ints head-idxs eav-plans]} plan
          ^List delta-tuples (:tuples delta-rel)
          acc                (FastList.)
          eav-count          (count eav-plans)
          ^objects eav-indexes
          (cached-eav-adjacencies context db eav-plans)
          ^ints eav-call-idxs
          (eav-call-positions eav-plans)
          output-scratch     (object-array (alength head-types))
          seen-lookup        (r/array-lookup)
          keyed-seen         (when (= 1 eav-count)
                               (keyed-eav-seen
                                 context branch plan
                                 (aget eav-indexes 0)))
          keyed-scratch      (when keyed-seen
                               (object-array
                                 (alength ^ints (:candidate-idxs keyed-seen))))
          keyed-lookup       (when keyed-seen (r/array-lookup))]
      (when (and delta-tuples (pos? (.size delta-tuples)))
        (dotimes [i (.size delta-tuples)]
          (let [^objects delta-tuple (.get delta-tuples i)]
            (case eav-count
              1
              (let [values0  ^List (.get ^HashMap (aget eav-indexes 0)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 0)))]
                (when values0
                  (if keyed-seen
                    (let [^HashSet values-seen
                          (group-values!
                            (:groups keyed-seen) delta-tuple
                            (:candidate-idxs keyed-seen)
                            keyed-scratch keyed-lookup)
                          domain-size (long (:domain-size keyed-seen))]
                      (loop [j 0]
                        (when (and (< j (.size values0))
                                   (< (.size values-seen) domain-size))
                          (let [value (.get values0 j)]
                            (when (.add values-seen value)
                              (add-fast-output-known-new!
                                acc seen-set output-scratch head-types head-idxs
                                delta-tuple call-rel-idxs value nil))
                            (recur (unchecked-inc-int j))))))
                    (dotimes [j (.size values0)]
                      (add-fast-output-with-probe!
                        acc seen-set seen-lookup output-scratch
                        head-types head-idxs delta-tuple call-rel-idxs
                        (.get values0 j) nil)))))

              2
              (let [values0  ^List (.get ^HashMap (aget eav-indexes 0)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 0)))
                    values1  ^List (.get ^HashMap (aget eav-indexes 1)
                                          (call-value
                                            delta-tuple call-rel-idxs
                                            (aget eav-call-idxs 1)))]
                (when (and values0 values1)
                  (dotimes [j (.size values0)]
                    (let [v0 (.get values0 j)]
                      (dotimes [k (.size values1)]
                        (add-fast-output-with-probe!
                          acc seen-set seen-lookup output-scratch
                          head-types head-idxs delta-tuple call-rel-idxs
                          v0 (.get values1 k)))))))))))
      (r/relation! (zipmap head-vars (range)) acc))))
