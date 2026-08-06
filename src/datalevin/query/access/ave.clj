;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.access.ave
  "Ordered AVE access paths."
  (:require
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.query.access :as access])
  (:import
   [datalevin.datom Datom]
   [datalevin.parser Constant DefaultSrc FindRel Pattern Predicate Variable]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private ^:const ^long default-batch-size 1024)

(declare discover-plans)

(deftype AVECursor
    [db attr reverse? start-value ^long batch-size max-candidates state closed?]

  access/IAccessCursor
  (-next-batch [_]
    (if @closed?
      (access/->AccessBatch (FastList.) nil true)
      (let [{:keys [frontier emitted]} @state
            cursor       frontier
            continuation (:continuation cursor)
            cursor-v     (:key continuation)
            cursor-n     (long (or (:ties continuation) 0))
            remaining    (when (some? max-candidates)
                           (max 0 (- (long max-candidates)
                                     (long emitted))))
            page-size    (long
                           (if (some? remaining)
                             (min batch-size (long remaining))
                             batch-size))
            requested    (+ page-size cursor-n 1)
            ^java.util.List found
            (if (zero? page-size)
              (FastList.)
              (if reverse?
                (db/-rseek-datoms db :ave attr (or cursor-v start-value)
                                  nil requested)
                (db/-seek-datoms db :ave attr (or cursor-v start-value)
                                 nil requested)))
            size         (.size found)
            start        (long
                           (loop [i 0]
                             (if (and cursor-v (< i size) (< i cursor-n)
                                      (= cursor-v
                                         (.-v ^Datom (.get found i))))
                               (recur (unchecked-inc-int i))
                               i)))
            end          (long (min size (+ start page-size)))
            boundary     (when (< start end)
                           (.-v ^Datom (.get found (dec end))))
            boundary-count
            (long
              (if boundary
                (loop [i (dec end)
                       n 0]
                  (if (and (<= start i)
                           (= boundary (.-v ^Datom (.get found i))))
                    (recur (dec i) (unchecked-inc n))
                    n))
                0))
            consumed-ties
            (if (= boundary cursor-v)
              (+ cursor-n boundary-count)
              boundary-count)
            more?        (< end size)
            exhausted?   (and (pos? page-size)
                              (not more?)
                              (< size requested))
            boundary-complete?
            (and boundary
                 (or exhausted?
                     (and more?
                          (not= boundary
                                (.-v ^Datom (.get found end))))))
            tuples       (FastList. (int (- end start)))
            frontier     (when boundary
                           (access/->AccessFrontier
                             {:key boundary :ties consumed-ties}
                             (when boundary-complete? boundary)))]
        (loop [i start]
          (when (< i end)
            (let [^Datom datom (.get found i)]
              (.add tuples
                    (object-array [(.-e datom) (.-v datom)]))
              (recur (unchecked-inc-int i)))))
        (vreset! state {:frontier frontier
                        :emitted  (+ (long emitted) (.size tuples))})
        (when exhausted? (vreset! closed? true))
        (access/->AccessBatch tuples frontier exhausted?))))

  (-close-cursor [_]
    (vreset! closed? true)))

(defrecord AVEAccessMethod []
  access/IAccessMethod
  (-access-plans [_ planning-context]
    (discover-plans planning-context))

  (-open-access [_ path _demand _bounds work source]
    (let [{:keys [db attr direction start-value batch-size]} (:options path)
          db             (or source db)
          requested-size (long (or (:sample-size work)
                                   (:batch-size work)
                                   batch-size
                                   default-batch-size))]
      (AVECursor. db attr (identical? direction :desc) start-value
                  (max 1 requested-size)
                  (:max-candidates work)
                  (volatile! {:frontier (:resume work)
                              :emitted  (long (or (:emitted work) 0))})
                  (volatile! false))))

  (-frontier-satisfies? [_ path _demand frontier cutoff]
    (when-let [certificate (:certificate frontier)]
      (let [direction (get-in path [:options :direction])
            c         (compare certificate (:primary-value cutoff))]
        (if (identical? direction :desc)
          (not (pos? c))
          (not (neg? c)))))))

(def access-method (->AVEAccessMethod))

(defn ordered-path
  ([db attr direction start-value]
   (ordered-path db attr direction start-value attr))
  ([db attr direction start-value order-term]
   (assoc
     (access/->AccessPath
       :ave
       access-method
       :ordered-scan
       [[order-term direction]]
       #{:complete :ordered :resumable :monotone :tie-complete :exact-frontier}
       {:db          db
        :attr        attr
        :direction   direction
        :start-value start-value
        :batch-size  default-batch-size}
       (access/access-policy :resumable :planning))
     :quality :exact)))

(defn- finite-limit?
  [limit]
  (and (some? limit) (not= -1 limit)))

(defn- first-order-key
  [order]
  (when-let [order-var (first order)]
    (when (symbol? order-var)
      [order-var (if (keyword? (second order)) (second order) :asc)])))

(defn- ranked-pattern
  [parsed-q order-var]
  (first
    (keep-indexed
      (fn [i clause]
        (when (and (instance? Pattern clause)
                   (instance? DefaultSrc (:source ^Pattern clause)))
          (let [pattern (:pattern ^Pattern clause)]
            (when (and (= 3 (count pattern))
                       (instance? Variable (nth pattern 0))
                       (instance? Constant (nth pattern 1))
                       (keyword? (:value ^Constant (nth pattern 1)))
                       (instance? Variable (nth pattern 2))
                       (= order-var (:symbol ^Variable (nth pattern 2))))
              {:clause-idx i
               :clause     clause
               :orig-clause (nth (:qorig-where parsed-q) i)
               :entity-var (:symbol ^Variable (nth pattern 0))
               :order-var  order-var
               :attr       (:value ^Constant (nth pattern 1))}))))
      (:qwhere parsed-q))))

(defn- term-value
  [values term]
  (cond
    (instance? Constant term) (:value ^Constant term)
    (instance? Variable term) (get values (:symbol ^Variable term) ::none)
    :else                     ::none))

(defn- ordered-range-start
  [parsed-q values order-var direction]
  (some
    (fn [clause]
      (when (instance? Predicate clause)
        (let [op   (get-in clause [:fn :symbol])
              args (:args ^Predicate clause)
              lhs  (first args)
              rhs  (second args)
              lvar (when (instance? Variable lhs) (:symbol ^Variable lhs))
              rvar (when (instance? Variable rhs) (:symbol ^Variable rhs))]
          (cond
            (and (identical? direction :desc)
                 (= lvar order-var) (#{'< '<=} op))
            (let [v (term-value values rhs)] (when-not (= ::none v) v))

            (and (identical? direction :desc)
                 (= rvar order-var) (#{'> '>=} op))
            (let [v (term-value values lhs)] (when-not (= ::none v) v))

            (and (identical? direction :asc)
                 (= lvar order-var) (#{'> '>=} op))
            (let [v (term-value values rhs)] (when-not (= ::none v) v))

            (and (identical? direction :asc)
                 (= rvar order-var) (#{'< '<=} op))
            (let [v (term-value values lhs)] (when-not (= ::none v) v))

            :else nil))))
    (:qwhere parsed-q)))

(defn discover-plans
  [{:keys [parsed-q inputs input-values demand]}]
  (let [find          (:qfind parsed-q)
        find-elements (dp/find-elements find)
        limit         (:qlimit parsed-q)
        dbs           (filterv db/db? inputs)]
    (if (and (instance? FindRel find)
             (finite-limit? limit)
             (pos? (long limit))
             (seq (:qorder parsed-q))
             (nil? (:qwith parsed-q))
             (empty? (:qhaving parsed-q))
             (nil? (:qreturn-map parsed-q))
             (not-any? #(or (dp/aggregate? %) (dp/find-expr? %)
                            (dp/pull? %))
                       find-elements)
             (= 1 (count dbs))
             (not (db/pending-tx-cache? (first dbs))))
      (if-let [[order-var direction] (first-order-key (:qorder parsed-q))]
        (if-let [ranked (ranked-pattern parsed-q order-var)]
          (if-some [start-value
                    (ordered-range-start
                      parsed-q
                      (access/planning-input-values
                        {:parsed-q parsed-q
                         :inputs inputs
                         :input-values input-values})
                      order-var direction)]
            (let [expr
                  (access/map->AccessExpr
                    {:method     :ave
                     :covers     #{(:clause ranked)}
                     :covered-originals #{(:orig-clause ranked)}
                     :requires   #{}
                     :produces   #{(:entity-var ranked) order-var}
                     :join-vars  #{(:entity-var ranked)}
                     :cols       [(:entity-var ranked) order-var]
                     :source     '$})
                  path
                  (ordered-path (first dbs) (:attr ranked) direction
                                start-value order-var)
                  demand
                  (or demand
                      (access/top-k-demand (:qorder parsed-q)
                                           (:qoffset parsed-q) limit))
                  range-rows
                  (long
                    (if (identical? direction :desc)
                      (db/-index-range-size (first dbs) (:attr ranked)
                                            nil start-value)
                      (db/-index-range-size (first dbs) (:attr ranked)
                                            start-value nil)))
                  required-count (long (:required-count demand))
                  rows
                  (long (min range-rows
                             (max required-count default-batch-size)))
                  estimate
                  (assoc
                    (access/->AccessEstimate 1.0 1.0 rows
                                             (+ 1.0 (double rows)) :low)
                    :range-rows range-rows)]
              [(access/->AccessPlan
                 expr path (access/source-bounds)
                 (access/->AccessWork
                   nil default-batch-size nil nil 0)
                 estimate)])
            [])
          [])
        [])
      [])))
