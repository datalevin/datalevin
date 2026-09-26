;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query.execute.ordered-range
  "Reusable ordered range access and projection for prepared queries."
  (:require
   [datalevin.datom :as datom]
   [datalevin.db :as db]
   [datalevin.parser :as dp]
   [datalevin.pull-api :as pull]
   [datalevin.query.access :as access]
   [datalevin.query.access.ave :as ave]
   [datalevin.query.execute.point-lookup :refer [unsupported]]
   [datalevin.query.execute.result :as result]
   [datalevin.storage :as storage]
   [datalevin.timeout :as timeout])
  (:import
   [datalevin.parser BindScalar Constant DefaultSrc FindRel Pattern Predicate
    SrcVar Variable]
   [java.util List]
   [java.util.concurrent.atomic AtomicReference]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- input-reader [indexes term]
  (if (instance? Constant term)
    (constantly (:value term))
    (when-some [idx (get indexes (if (instance? DefaultSrc term)
                                  '$ (:symbol term)))]
      #(nth % idx))))

(defn- range-shape [parsed-q]
  (let [{:keys [qin qfind qwhere qorder qlimit qwith qhaving qreturn-map]} parsed-q
        indexes (into {} (map-indexed #(vector (:symbol (:variable %2)) %1)) qin)
        patterns (filterv #(instance? Pattern %) qwhere)
        pattern (first (filter #(= (first qorder)
                                   (get-in % [:pattern 2 :symbol])) patterns))
        fields (filterv #(not= pattern %) patterns)
        pred    (first (filter #(instance? Predicate %) qwhere))
        [entity attr value] (:pattern pattern)
        [lhs rhs] (:args pred)
        op      (get-in pred [:fn :symbol])
        direction (second qorder)
        value-left? (= lhs value)
        bound   (if value-left? rhs lhs)
        start   (input-reader indexes bound)
        forward? (if value-left? (#{'> '>=} op) (#{'< '<=} op))
        reverse? (if value-left? (#{'< '<=} op) (#{'> '>=} op))
        elements (dp/find-elements qfind)
        field-vars (mapv #(get-in % [:pattern 2 :symbol]) fields)
        columns (zipmap (into [(:symbol entity) (:symbol value)] field-vars)
                        (range))
        projection (mapv #(get columns (:symbol (if (dp/pull? %) (:variable %) %)))
                         elements)
        pulls (keep-indexed
                (fn [idx element]
                  (when (dp/pull? element)
                    {:index idx
                     :source (input-reader indexes (:source element))
                     :pattern (input-reader indexes (:pattern element))}))
                elements)]
    (when (and (instance? FindRel qfind)
               (every? #(instance? BindScalar %) qin)
               (instance? SrcVar (:variable (first qin)))
               (= '$ (:symbol (:variable (first qin))))
               (not-any? #(instance? SrcVar (:variable %)) (rest qin))
               (= (inc (count patterns)) (count qwhere))
               (= (count columns) (+ 2 (count fields)))
               (every? (fn [field]
                         (let [[e a v] (:pattern field)]
                           (and (instance? DefaultSrc (:source field))
                                (= 3 (count (:pattern field)))
                                (= e entity)
                                (instance? Constant a) (keyword? (:value a))
                                (instance? Variable v)
                                (not (contains? indexes (:symbol v))))))
                       fields)
               (apply distinct? (map #(get-in % [:pattern 1 :value]) patterns))
               (instance? DefaultSrc (:source pattern))
               (= 3 (count (:pattern pattern)))
               (instance? Variable entity) (instance? Variable value)
               (not= entity value)
               (not (contains? indexes (:symbol entity)))
               (not (contains? indexes (:symbol value)))
               (instance? Constant attr) (keyword? (:value attr))
               (= 2 (count (:args pred)))
               (or (= lhs value) (= rhs value))
               start
               (= (:symbol value) (first qorder))
               (or (and (= :asc direction) forward?)
                   (and (= :desc direction) reverse?))
               (or (symbol? qlimit) (and (integer? qlimit) (pos? qlimit)))
               (nil? qwith) (empty? qhaving) (nil? qreturn-map)
               (every? #(or (instance? Variable %) (dp/pull? %)) elements)
               (every? some? projection)
               (every? #(and (:source %) (:pattern %)) pulls)
               ;; Ordering must refer to projected values, never pulled maps.
               (every? (fn [sym]
                         (some #(and (instance? Variable %) (= sym (:symbol %)))
                               elements))
                       (take-nth 2 qorder)))
      {:attr (:value attr) :direction direction :start start
       :strict? (boolean (#{'< '>} op))
       :fields (mapv #(get-in % [:pattern 1 :value]) fields)
       :projection projection :pulls (vec pulls)})))

(defn- materialize [pulls inputs rows]
  (if (seq rows)
    (reduce
      (fn [rows {:keys [index source pattern]}]
        (let [values (pull/pull-many (source inputs) (pattern inputs)
                                     (mapv #(nth % index) rows))]
          (mapv #(assoc %1 index %2) rows values)))
      rows pulls)
    []))

(deftype ^:private FieldLayout [schema attrs-v projection scan])

(defn- field-layout [schema fields projection]
  (when (every? #(and (some? (get-in schema [% :db/aid]))
                     (not= :db.cardinality/many
                           (get-in schema [% :db/cardinality])))
                fields)
    (let [ordered (vec (sort-by #(get-in schema [% :db/aid]) fields))
          ;; Field scans retain an ordinal beside [eid key] so the existing
          ;; EAV scan may sort by eid without changing the selected key order.
          indexes (zipmap ordered (range 3 (+ 3 (count ordered))))
          attrs-v (mapv #(vector % {:skip? false}) ordered)]
      (FieldLayout. schema
                    attrs-v
                    (mapv #(if (< % 2) % (indexes (nth fields (- % 2))))
                          projection)
                    (storage/prepare-eav-scan-v-list schema attrs-v)))))

(defn- select-key-page
  [database path demand accepts? start-value strict?]
  (let [limit (long (:limit demand))
        ;; A unique key contributes at most one excluded boundary for > or <.
        work (assoc (access/access-work (min 1024 limit))
                    :max-candidates (+ limit (if strict? 1 0)))
        cursor (access/open-access path demand work database)
        selected (FastList.)]
    (try
      (loop []
        (timeout/assert-time-left)
        (let [{:keys [tuples exhausted?]} (access/next-batch cursor)]
          (doseq [^objects tuple tuples
                  :while (< (.size selected) limit)]
            (when (accepts? (datom/compare-with-type (aget tuple 1) start-value))
              (.add selected (object-array [(aget tuple 0) (aget tuple 1)
                                            (.size selected)]))))
          (if (or exhausted? (>= (.size selected) limit))
            selected
            (recur))))
      (finally (access/close-cursor cursor)))))

(defn- project-key-page [database ^FieldLayout layout ^List selected]
  (if (.isEmpty selected)
    #{}
    (let [^List tuples (db/-eav-scan-v-list database selected 0 (.-attrs-v layout)
                                         (.-scan layout))
          n (.size selected)]
      ;; Missing fields make these clauses filters. Let general execution
      ;; handle that case; this access path never refills its limited key page.
      (if (= n (count tuples))
        (let [rows (object-array n)]
          (dotimes [i n]
            (let [^objects tuple (.get tuples i)]
              (aset rows (int (aget tuple 2))
                    (mapv #(aget tuple (int %)) (.-projection layout)))))
          (vec rows))
        unsupported))))

(defn prepared-executor
  "Prepare an ordered AVE access path and its result projection. For unique
  keys, apply the limit in AVE before projecting a page with the EAV scan.
  No source, data sample, cursor or result survives an execution. Unsupported
  inputs use the general planner, including transaction overlays and additional
  DBs."
  [parsed-q]
  (when-let [{:keys [attr direction start strict? projection pulls fields]}
             (range-shape parsed-q)]
    (let [layout-cache (AtomicReference.)
          path-template (ave/ordered-path nil attr direction nil)
          find-vars (vec (dp/find-vars (:qfind parsed-q)))
          order (result/prepare-order find-vars (:qorder parsed-q))
          primary-index (.indexOf ^List find-vars (first (:qorder parsed-q)))
          accepts? (if (= :asc direction)
                     (if strict? pos? #(not (neg? %)))
                     (if strict? neg? #(not (pos? %))))]
      (fn [resolved-q inputs]
        (let [database (first inputs)
              start-value (start inputs)
              schema (when (db/db? database) (db/-schema database))
              ^FieldLayout cached (.get layout-cache)
              ^FieldLayout layout
              (when schema
                (if (and cached (identical? schema (.-schema cached)))
                  cached
                  (let [layout (field-layout schema fields projection)]
                    (.set layout-cache layout)
                    layout)))]
          (if (and layout
                   (not (get-in schema [attr :db/noindex]))
                   (or (empty? fields)
                       (and (get-in schema [attr :db/unique])
                            (zero? (long (or (:qoffset resolved-q) 0)))))
                   (db/db? database)
                   (not (db/pending-tx-cache? database))
                   (not-any? db/-searchable? (rest inputs))
                   ;; Reference resolution and custom ordering need the
                   ;; general planner's normalization and residual predicate.
                   (#{:db.type/long :db.type/string :db.type/keyword
                      :db.type/symbol :db.type/float :db.type/double
                      :db.type/instant :db.type/uuid}
                    (get-in (db/-schema database) [attr :db/valueType]))
                   (some? start-value))
            (let [demand (access/top-k-demand (:qorder resolved-q)
                                             (:qoffset resolved-q)
                                             (:qlimit resolved-q))
                  window-end (long (:required-count demand))
                  path (assoc-in path-template [:options :start-value] start-value)]
              (if (seq fields)
                (let [selected (select-key-page database path demand accepts?
                                                start-value strict?)
                      rows (project-key-page database layout selected)]
                  (if (or (identical? unsupported rows) (empty? pulls))
                    rows
                    (materialize pulls inputs rows)))
                (let [cursor (access/open-access
                               path demand
                               (access/access-work (min 1024 window-end)) database)]
                  (try
                    (loop [rows #{}]
                      (timeout/assert-time-left)
                      (let [{:keys [tuples frontier exhausted?]}
                            (access/next-batch cursor)
                            rows (reduce
                                   (fn [rows ^objects tuple]
                                     (if (accepts? (datom/compare-with-type
                                                     (aget tuple 1) start-value))
                                       (conj rows (mapv #(aget tuple (int %)) projection))
                                       rows))
                                   rows tuples)
                            selected (when (or exhausted? (<= window-end (count rows)))
                                       (order rows (:qlimit resolved-q)
                                              (:qoffset resolved-q)))]
                        (if (or exhausted?
                                (and selected (<= window-end (count rows))
                                     (access/frontier-satisfies?
                                       path demand frontier
                                       {:primary-value (nth (peek selected) primary-index)})))
                          (if (seq pulls)
                            (materialize pulls inputs (vec selected))
                            selected)
                          (recur rows))))
                    (finally (access/close-cursor cursor))))))
            unsupported))))))
