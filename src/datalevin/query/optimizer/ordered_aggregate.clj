;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.query.optimizer.ordered-aggregate
  "Choose checked ordered reduction when group keys appear clustered by entity."
  (:require [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.index :as idx]
            [datalevin.interface :as i]
            [datalevin.lmdb :as lmdb]
            [datalevin.parser :as parser]
            [datalevin.query.optimizer.parallel :as parallel]
            [datalevin.storage.scan :as scan])
  (:import [datalevin.db DB]
           [datalevin.storage Store]
           [datalevin.parser Aggregate Constant Pattern Variable]
           [java.nio ByteBuffer]))

(def ^:dynamic *enabled?* true)
(def ^:dynamic *sample-size* 256)

(defn- total-having? [parsed]
  ;; A speculative prefix must not raise an error in HAVING that a later
  ;; ordering failure would have avoided by completing that group normally.
  ;; Numeric comparisons are total; arbitrary calls retain ordinary execution.
  (every? (fn [predicate]
            (and (#{'= '== 'not= '< '<= '> '>=} (get-in predicate [:fn :symbol]))
                 (seq (:args predicate))
                 (every? #(or (and (instance? Constant %) (number? (:value %)))
                               (and (instance? Aggregate %)
                                    (#{'sum 'avg 'count} (get-in % [:fn :symbol]))))
                         (:args predicate)))) (:qhaving parsed)))

(defn- ranks [population n]
  (let [last-index (dec (long population)) denominator (max 1 (dec (long n)))
        step (quot last-index denominator) remainder (rem last-index denominator)]
    (long-array (map #(+ (* (long %) step)
                         (quot (* (long %) remainder) denominator)) (range n)))))

(defn plan
  "Sampling is an admission hint, never an ordering proof. The executor must
  validate ascending keys and partition bounds on every qualifying row. Keep
  the existing path for small domains, serial scans and expression programs."
  [parsed fields ^DB database schema estimated-input]
  (when (and *enabled?* (seq (:qhaving parsed))
             (total-having? parsed)
             (= 1 (count (:qin parsed)))
             (every? #(instance? Pattern %) (:qwhere parsed))
             (< 1 (parallel/participants estimated-input parallel/*scan-rows-per-worker*)))
    (let [groups (filter #(instance? Variable %)
                        (parser/find-elements (:qfind parsed)))
          attr (when (= 1 (count groups))
                 (some #(when (= (:variable %) (:symbol (first groups))) (:attr %)) fields))
          props (schema attr) vt (:db/valueType props)]
      (when (#{:db.type/long :db.type/ref} vt)
        (let [store (.-store database) lmdb (.-lmdb ^Store store)
              population (long (i/a-size store attr))
              n (min population (max 2 (long *sample-size*)))
              entities (volatile! [])]
          (when (pos? population)
            (scan/ave-tuples-scan* lmdb (:db/aid props) vt
              [[[:closed c/v0] [:closed c/vmax]]] (ranks population n)
              (fn [entry]
                (vswap! entities conj (.getLong ^ByteBuffer (lmdb/v entry) 0))))
          (when (and (< 1 (count @entities)) (apply < @entities))
            {:attribute attr :aid (:db/aid props) :value-type vt
             :sampled-entities (count @entities)})))))))

(defn partitions
  "Choose entity boundaries at the first entity of sampled AVE groups, inside
  the caller's pinned snapshot. Each partition also carries its required group
  interval; checking both order and interval prevents splitting a group even
  when sampling misses disorder or the data changes after planning."
  [lmdb {:keys [aid value-type]} participants]
  (let [key-range (scan/ave-key-range aid value-type
                                   [[:closed c/v0] [:closed c/vmax]])
        population (long (i/list-range-count lmdb c/ave key-range :avg))
        values (volatile! [])]
    (when (pos? population)
      (scan/ave-tuples-scan* lmdb aid value-type
        [[[:closed c/v0] [:closed c/vmax]]]
        (long-array (map #(+ (* (quot population (long participants)) (long %))
                              (quot (* (rem population (long participants)) (long %))
                                    (long participants)))
                         (range 1 participants)))
        (fn [entry] (vswap! values conj (idx/avg-buffer->v lmdb (lmdb/k entry))))))
    (let [boundaries
          (mapv (fn [value]
                  (let [key (b/indexable nil aid value value-type c/g0)]
                    [(second (i/list-range-first lmdb c/ave [:closed key key] :avg [:all] :id))
                     value])) (distinct @values))
          entities (into [0] (map first) boundaries)]
      (when (and (every? some? entities) (or (= 1 (count entities)) (apply < entities)))
        (mapv (fn [[[start lower] [end upper]]]
                {:entity-range [start end] :lower lower :upper upper})
              (partition 2 1 (concat [[0 nil]] boundaries [[nil nil]])))))))
