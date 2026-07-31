;;
;; Copyright (c) Nikita Prokopov, Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.entity
  "Entity object"
  (:refer-clojure :exclude [keys get])
  (:require
   [clojure.core :as c]
   [datalevin.db :as db]
   [datalevin.remote :as r]
   [datalevin.util :as u]
   [datalevin.interface :refer [db-name]]
   [s-exp.hako.ext :as hext]
   [clojure.set :as set]
   [datalevin.query-util :as qu])
  (:import
   [com.s_exp.hako Reader Writer]
   [datalevin.db DB]
   [datalevin.interface IStore]
   [datalevin.remote DatalogStore]))

(declare entity ->Entity equiv-entity lookup-entity touch entity->txs
         lookup-stage-then-entity)

(defn- entid [db eid]
  (when (or (number? eid)
            (sequential? eid)
            (keyword? eid))
    (db/entid db eid)))

(defn entity
  [db eid]
  (when-let [e (entid db eid)]
    (->Entity db e (volatile! false) (volatile! {}) {} {})))

(defn- entity-attr [db a datoms]
  (if (db/multival? db a)
    (if (db/ref? db a)
      (reduce #(conj %1 (entity db (:v %2))) #{} datoms)
      (reduce #(conj %1 (:v %2)) #{} datoms))
    (if (db/ref? db a)
      (entity db (:v (first datoms)))
      (:v (first datoms)))))

(defn- -lookup-backwards [db eid attr not-found]
  (if-let [datoms (not-empty (db/-search db [nil attr eid]))]
    (if (db/component? db attr)
      (entity db (:e (first datoms)))
      (reduce #(conj %1 (entity db (:e %2))) #{} datoms))
    not-found))

(defprotocol Transactable
  (add [this attr v])
  (retract
    [this attr]
    [this attr v])
  (->txs [this]))

(deftype Entity [db eid touched cache tbd meta-map]
  clojure.lang.IPersistentMap
  (equiv [e o] (equiv-entity e o))
  (containsKey [e k] (not= ::nf (lookup-stage-then-entity e k ::nf)))
  (entryAt [e k] (some->> (lookup-stage-then-entity e k)
                          (clojure.lang.MapEntry. k)))

  (empty [_] (throw (UnsupportedOperationException.)))
  (assoc [_ k v]
    (assert (keyword? k) "attribute must be keyword")
    (Entity. db eid touched cache (assoc tbd k [{:op :assoc} v]) meta-map))
  (without [_ k]
    (assert (keyword? k) "attribute must be keyword")
    (Entity. db eid touched cache (assoc tbd k [{:op :dissoc}]) meta-map))
  (cons [this [k v]]
    (assoc this k v))
  (count [this] (touch this) (count @(.-cache this)))
  (seq [this] (touch this) (seq @cache))

  Iterable
  (iterator [this] (clojure.lang.SeqIterator. (seq this)))

  clojure.lang.ILookup
  (valAt [e k] (lookup-stage-then-entity e k))
  (valAt [e k not-found] (lookup-stage-then-entity e k not-found))

  clojure.lang.IFn
  (invoke [e k] (lookup-stage-then-entity e k))
  (invoke [e k not-found] (lookup-stage-then-entity e k not-found))

  Transactable
  (add [_ attr v]
    (assert (keyword? attr) "attribute must be keyword")
    (Entity. db eid touched cache (assoc tbd attr [{:op :add} v]) meta-map))
  (retract [this attr]
    (assert (keyword? attr) "attribute must be keyword")
    (Entity. db eid touched cache (assoc tbd attr [{:op :retract}]) meta-map))
  (retract [_ attr v]
    (assert (keyword? attr) "attribute must be keyword")
    (Entity. db eid touched cache (assoc tbd attr [{:op :retract} v]) meta-map))
  (->txs [this]
    (entity->txs this))

  Object
  (toString [_] (pr-str (assoc @cache :db/id eid)))
  (hashCode [_] (hash eid))
  (equals [this o] (equiv-entity this o)))

(defmethod print-method Entity [^Entity e, ^java.io.Writer w]
  (let [staged (not-empty (.-tbd e))
        ent    (assoc @(.cache e) :db/id (.eid e))]
    (.write w (pr-str
               (cond-> ent
                 staged (assoc :<STAGED> staged))))))

(defn- rschema->attr-types
  [{ref-attrs  :db.type/ref
    many-attrs :db.cardinality/many
    components :db/isComponent}]
  {:ref-attrs       (set/difference ref-attrs many-attrs)
   :ref-rattrs      (into #{} (map db/reverse-ref) components)
   :ref-many-rattrs (into #{} (map db/reverse-ref)
                          (set/difference ref-attrs components))
   :ref-many-attrs  (set many-attrs)})

(def ^:private mem-rschema->attr-types
  (u/memoize-1 rschema->attr-types))

(defn- db->attr-types [db]
  (mem-rschema->attr-types (db/-rschema db)))

(defn lookup-stage-then-entity
  ([e k] (lookup-stage-then-entity e k nil))
  ([^Entity e k default-value]
   (if-let [[_ v] (c/get (.-tbd e) k)]
     v
     (if-some [v (lookup-entity e k)]
       v
       default-value))))

(defn- entity->txs [^Entity e]
  (let [eid (.-eid e)
        db  (.-db e)
        {:keys [ref-many-rattrs ref-many-attrs]}
        (db->attr-types db)]
    (into
     []
     (mapcat
      (fn [[k [meta v]]]
        (let [{:keys [op]} meta]
          (if (or (ref-many-attrs k) (ref-many-rattrs k))
            (let [v (if (qu/lookup-ref? v)
                      [v]
                      (u/ensure-vec v))]
              (case op
                :assoc   [[:db.fn/retractAttribute eid k]
                          {:db/id eid
                           k      (mapv (fn [e] (or (:db/id e) e)) v)}]
                :add     [{:db/id eid
                           k      (mapv (fn [e] (or (:db/id e) e)) v)}]
                :dissoc  [[:db.fn/retractAttribute eid k]]
                :retract (into []
                               (map (fn [e]
                                      [:db/retract eid k (:db/id e)]))
                               v)))
            (case op
              (:dissoc :retract) [[:db.fn/retractAttribute eid k]]
              (:assoc :add)      [[:db/add eid k v]])))))
     (.-tbd e))))

(defn entity? [x] (instance? Entity x))

(defn- equiv-entity [^Entity this that]
  (and
   (instance? Entity that)
   (identical? (.-db this) (.-db ^Entity that)) ; `=` and `hash` on db is expensive
   (= (.-eid this) (.-eid ^Entity that))))

(defn- lookup-entity
  ([this attr] (lookup-entity this attr nil))
  ([^Entity this attr not-found]
   (if (= attr :db/id)
     (.-eid this)
     (if (db/reverse-ref? attr)
       (-lookup-backwards (.-db this) (.-eid this) (db/reverse-ref attr) not-found)
       (if-some [v (@(.-cache this) attr)]
         v
         (if @(.-touched this)
           not-found
           (if-some [datoms (not-empty (db/-search (.-db this) [(.-eid this) attr]))]
             (let [value (entity-attr (.-db this) attr datoms)]
               (vreset! (.-cache this) (assoc @(.-cache this) attr value))
               value)
             not-found)))))))

(defn touch-components [db a->v]
  (reduce-kv (fn [acc a v]
               (assoc acc a
                      (if (db/component? db a)
                        (if (db/multival? db a)
                          (set (map touch v))
                          (touch v))
                        v)))
             {} a->v))

(defn- datoms->cache [db datoms]
  (reduce (fn [acc part]
            (let [a (:a (first part))]
              (assoc acc a (entity-attr db a part))))
          {} (partition-by :a datoms)))

(defn touch [^Entity e]
  {:pre [(entity? e)]}
  (when-not @(.-touched e)
    (when-let [datoms (not-empty (db/-search (.-db e) [(.-eid e)]))]
      (vreset! (.-cache e) (->> datoms
                                (datoms->cache (.-db e))
                                (touch-components (.-db e))))
      (vreset! (.-touched e) true)))
  e)

(defn entity-db
  "Returns the Datalog DB that this entity was created from."
  [^Entity entity]
  {:pre [(entity? entity)]}
  (.-db entity))

(defn- load-cache [^Entity e cache]
  (vreset! (.-cache e) cache)
  (vreset! (.-touched e) true)
  e)

(defn- ent->map
  [^Entity x]
  (let [^DB db  (.-db x)
        db-name (db-name ^IStore (.-store db))
        m       {:db/id   (.-eid x)
                 :db-name db-name}]
    (if @(.-touched x)
      (assoc m :touched true :cache @(.-cache x))
      m)))

(defn- map->ent
  [{:keys [db-name touched cache db/id]}]
  (let [db (let [^DB db (@db/dbs db-name)]
             (when-not (instance? DatalogStore (.-store db)) db))
        e  (entity db id)]
    (if touched
      (load-cache e cache)
      e)))

(hext/register-user-tag!
 8                                      ; subtag 8 = entity (wire id 0x10000008)
 Entity
 (fn write-entity [^Writer w ^Entity x]
   (.writeAny w (ent->map x)))
 (fn read-entity [^Reader r]
   (map->ent (.readAny r))))
