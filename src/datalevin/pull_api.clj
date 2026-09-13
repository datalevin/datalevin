;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.pull-api
  "API for pull syntax"
  (:require
   [datalevin.pull-parser :as dpp]
   [datalevin.db :as db]
   [datalevin.constants :as c]
   [datalevin.datom :as dd]
   [datalevin.interface :as i]
   [datalevin.timeout :as timeout]
   [datalevin.util :as u :refer [cond+]])
  (:import
   [datalevin.db DB]
   [datalevin.utl LRUCache]
   [datalevin.datom Datom]
   [datalevin.pull_parser PullAttr PullPattern]))

(declare pull-impl attrs-frame ref-frame ->ReverseAttrsFrame)

(defn- first-seq [xs] (if (nil? xs) nil (first xs)))

(defn- next-seq [xs] (if (nil? xs) nil (next xs)))

(defn- conj-seq [xs x] (if (nil? xs) (list x) (cons x xs)))

(defn- assoc-some! [m k v] (if (nil? v) m (assoc! m k v)))

(defn- conj-some! [xs v] (if (nil? v) xs (conj! xs v)))

(defrecord Context [db visitor])

(defn visit [^Context context pattern e a v]
  (when-some [visitor (.-visitor context)]
    (visitor pattern e a v)))

(defprotocol IFrame
  (-merge [this result])
  (-run [this context]))

(defrecord ResultFrame [value datoms])

(defrecord MultivalAttrFrame [acc ^PullAttr attr datoms]
  IFrame
  (-run [_ _]
    (loop [acc    acc
           datoms datoms]
      (cond+
        :let [^Datom datom (first-seq datoms)]

        (or (nil? datom) (not= (.-a datom) (.-name attr)))
        [(ResultFrame. (not-empty (persistent! acc)) (or datoms ()))]

        ;; got limit, skip rest of the datoms
        (and (.-limit attr) (>= (count acc) ^long (.-limit attr)))
        (loop [datoms datoms]
          (let [^Datom datom (first-seq datoms)]
            (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
              [(ResultFrame. (persistent! acc) (or datoms ()))]
              (recur (next-seq datoms)))))

        :else
        (recur (conj! acc (.-v datom)) (next-seq datoms))))))

(defrecord MultivalRefAttrFrame [seen recursion-limits acc pattern
                                 ^PullAttr attr datoms]
  IFrame
  (-merge [_ result]
    (MultivalRefAttrFrame.
      seen
      recursion-limits
      (conj-some! acc (.-value ^ResultFrame result))
      pattern
      attr
      (next-seq datoms)))
  (-run [this context]
    (cond+
      :let [^Datom datom (first-seq datoms)]

      (or (nil? datom) (not= (.-a datom) (.-name attr)))
      [(ResultFrame. (not-empty (persistent! acc)) (or datoms ()))]

      ;; got limit, skip rest of the datoms
      (and (.-limit attr) (>= (count acc) ^long (.-limit attr)))
      (loop [datoms datoms]
        (let [^Datom datom (first-seq datoms)]
          (if (or (nil? datom) (not= (.-a datom) (.-name attr)))
            [(ResultFrame. (persistent! acc) (or datoms ()))]
            (recur (next-seq datoms)))))

      :let [id (if (.-reverse? attr) (.-e datom) (.-v datom))]

      :else
      [this (ref-frame context seen recursion-limits pattern attr id)])))

(defrecord AttrsFrame [seen recursion-limits acc ^PullPattern pattern
                       ^PullAttr attr attrs datoms id]
  IFrame
  (-merge [_ result]
    (AttrsFrame.
      seen
      recursion-limits
      (assoc-some!
        acc (.-as attr) ((.-xform attr) (.-value ^ResultFrame result)))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      (not-empty (or (.-datoms ^ResultFrame result) (next-seq datoms)))
      id))
  (-run [_ context]
    (loop [acc    acc
           attr   attr
           attrs  attrs
           datoms datoms]
      (cond+
        ;; exit
        (and (nil? datoms) (nil? attr))
        [(->ReverseAttrsFrame seen recursion-limits acc pattern
                              (first-seq (.-reverse-attrs pattern))
                              (next-seq (.-reverse-attrs pattern)) id)]

        ;; :db/id
        (and (some? attr) (= :db/id (.-name attr)))
        (recur (assoc! acc (.-as attr) ((.-xform attr) id))
               (first-seq attrs) (next-seq attrs) datoms)

        :let [^Datom datom (first-seq datoms)
              aid #(-> (.-db ^Context context) db/-schema % :db/aid)
              cmp          (when (and datom attr)
                             (compare (aid (.-name attr)) (aid (.-a datom))))
              attr-ahead?  (or (nil? attr) (and cmp (pos? ^long cmp)))
              datom-ahead? (or (nil? datom) (and cmp (neg? ^long cmp)))]

        ;; wildcard
        (and (.-wildcard? pattern) (some? datom) attr-ahead?)
        (let [datom-attr (dpp/parse-attr-name (.-db ^Context context)
                                              (.-a datom))]
          (recur acc datom-attr (when attr (conj-seq attrs attr)) datoms))

        ;; advance datom
        attr-ahead?
        (recur acc attr attrs (next-seq datoms))

        :do (visit context :db.pull/attr id (.-name attr) nil)

        ;; advance attr
        (and datom-ahead? (nil? attr))
        (recur acc (first-seq attrs) (next-seq attrs) datoms)

        ;; default
        (and datom-ahead? (some? (.-default attr)))
        (recur (assoc! acc (.-as attr) (.-default attr))
               (first-seq attrs) (next-seq attrs) datoms)

        ;; xform
        datom-ahead?
        (if-some [value ((.-xform attr) nil)]
          (recur (assoc! acc (.-as attr) value) (first-seq attrs)
                 (next-seq attrs) datoms)
          (recur acc (first-seq attrs) (next-seq attrs) datoms))

        ;; matching attr
        (and (.-multival? attr) (.-ref? attr))
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalRefAttrFrame. seen recursion-limits (transient [])
                                pattern attr datoms)]

        (.-multival? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalAttrFrame. (transient []) attr datoms)]

        (.-ref? attr)
        [(AttrsFrame. seen recursion-limits acc pattern attr attrs datoms id)
         (MultivalRefAttrFrame.
           seen recursion-limits (transient {}) pattern attr datoms)]

        :else
        (recur
          (assoc! acc (.-as attr) ((.-xform attr) (.-v datom)))
          (first-seq attrs) (next-seq attrs) (next-seq datoms))))))

(defrecord ReverseAttrsFrame [seen recursion-limits acc pattern
                              ^PullAttr attr attrs id]
  IFrame
  (-merge [this result]
    (ReverseAttrsFrame.
      seen
      recursion-limits
      (assoc-some!
        acc (.-as attr) ((.-xform attr) (.-value ^ResultFrame result)))
      pattern
      (first-seq attrs)
      (next-seq attrs)
      id))
  (-run [this context]
    (loop [acc   acc
           attr  attr
           attrs attrs]
      (cond+
        (nil? attr)
        [(ResultFrame. (not-empty (persistent! acc)) nil)]

        :let [name   (.-name attr)
              datoms (db/-av-datoms (.-db ^Context context) name id)]

        :do (visit context :db.pull/reverse nil name id)

        (and (empty? datoms) (some? (.-default attr)))
        (recur (assoc! acc (.-as attr) (.-default attr))
               (first-seq attrs) (next-seq attrs))

        (empty? datoms)
        (recur acc (first-seq attrs) (next-seq attrs))

        (.-component? attr)
        [(ReverseAttrsFrame. seen recursion-limits acc pattern attr attrs id)
         (ref-frame context seen recursion-limits pattern attr
                    (.-e ^Datom (first-seq datoms)))]

        :else
        [(ReverseAttrsFrame. seen recursion-limits acc pattern attr attrs id)
         (MultivalRefAttrFrame. seen recursion-limits (transient []) pattern attr
                                datoms)]))))

(defn- auto-expanding? [^PullAttr attr]
  (or
    (.-recursive? attr)
    (and
      (.-component? attr)
      (.-wildcard? ^PullPattern (.-pattern attr)))))

(defn ref-frame [context seen recursion-limits pattern ^PullAttr attr id]
  (cond+
    (not (auto-expanding? attr))
    (attrs-frame context seen recursion-limits (.-pattern attr) id)

    (seen id)
    (ResultFrame. {:db/id id} nil)

    :let [lim (recursion-limits attr)]

    (and lim (<= ^long lim 0))
    (ResultFrame. nil nil)

    :let [seen' (conj seen id)
          recursion-limits'
          (cond
            lim (update recursion-limits attr dec)

            (.-recursion-limit attr)
            (assoc recursion-limits attr (dec ^long (.-recursion-limit attr)))
            :else recursion-limits)]

    :else
    (attrs-frame context seen' recursion-limits'
                 (if (.-recursive? attr) pattern (.-pattern attr)) id)))

(defn attrs-frame
  [^Context context seen recursion-limits ^PullPattern pattern id]
  (let [datoms
        (cond+
          (.-wildcard? pattern)
          (db/-e-datoms (.-db context) id)

          (nil? (.-first-attr pattern))
          nil

          :else
          (db/-range-datoms
            (.-db context) :eav
            (dd/datom id (.-name ^PullAttr (.-first-attr pattern)) nil c/tx0)
            (dd/datom id (.-name ^PullAttr (.-last-attr pattern)) nil
                      c/txmax)))]
    (when (.-wildcard? pattern)
      (visit context :db.pull/wildcard id nil nil))
    (AttrsFrame.
      seen
      recursion-limits
      (transient {})
      pattern
      (first-seq (.-attrs pattern))
      (next-seq (.-attrs pattern))
      datoms
      id)))

(defn pull-impl [parsed-opts id]
  (let [{^Context context     :context
         ^PullPattern pattern :pattern} parsed-opts]
    (when-some [eid (db/entid (.-db context) id)]
      (loop [stack (list (attrs-frame context #{} {} pattern eid))]
        (timeout/assert-time-left)
        (cond+
          :let [last   (first-seq stack)
                stack' (next-seq stack)]

          (not (instance? ResultFrame last))
          (recur (reduce conj-seq stack' (-run last context)))

          (nil? stack')
          (.-value ^ResultFrame last)

          :let [penultimate (first-seq stack')
                stack''     (next-seq stack')]

          :else
          (recur (conj-seq stack'' (-merge penultimate last))))))))

(defn parse-opts
  ([^DB db pattern] (parse-opts db pattern nil))
  ([^DB db pattern {:keys [visitor]}]
   {:pattern (let [^LRUCache c (.-pull-patterns db)]
               (or (.get c pattern)
                   (let [res (dpp/parse-pattern db pattern)]
                     (.put c pattern res)
                     res)))
    :context (Context. db visitor)}))

(defn pull*
  "Supported opts:

   :visitor a fn of 4 arguments, will be called for every entity/attribute pull touches

   (:db.pull/attr     e   a   nil) - when pulling a normal attribute, no matter if it has value or not
   (:db.pull/wildcard e   nil nil) - when pulling every attribute on an entity
   (:db.pull/reverse  nil a   v  ) - when pulling reverse attribute"
  ([^DB db pattern id] (pull* db pattern id {}))
  ([^DB db pattern id {:keys [timeout] :as opts}]
   {:pre [(db/db? db)]}
   (binding [timeout/*deadline* (timeout/to-deadline timeout)]
     (let [parsed-opts (parse-opts db pattern opts)]
       (pull-impl parsed-opts id)))))

(defn pull
  ([db pattern id opts]
   (let [store (.-store ^DB db)]
     (if (satisfies? i/IRemoteDB store)
       (i/pull store pattern id opts)
       (pull* db pattern id opts))))
  ([db pattern id]
   (pull db pattern id {})))

(defn pull-many*
  ([^DB db pattern ids] (pull-many* db pattern ids {}))
  ([^DB db pattern ids {:keys [timeout] :as opts}]
   {:pre [(db/db? db)]}
   (binding [timeout/*deadline* (timeout/to-deadline timeout)]
     (let [parsed-opts (parse-opts db pattern opts)]
       (mapv #(pull-impl parsed-opts %) ids)))))

(defn pull-many
  ([db pattern id opts]
   (let [store (.-store ^DB db)]
     (if (satisfies? i/IRemoteDB store)
       (i/pull-many store pattern id opts)
       (pull-many* db pattern id opts))))
  ([db pattern id]
   (pull-many db pattern id {})))
