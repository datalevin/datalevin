(ns datalevin.db.transfer-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.datom :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i])
  (:import [java.util Comparator]
           [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(defn- database [store]
  (let [[eavt avet] (db/batch-tx-indexes store #(i/schema store) (constantly nil))]
    (db/with-runtime-opts
      (db/map->DB {:store store :max-eid 10 :max-tx 20 :eavt eavt :avet avet
                  :pull-patterns (Object.) :pull-readers (Object.)})
      {:test/marker :preserved})))

(deftest same-store-transfers-share-comparators-and-isolate-mutations
  (let [store (reify i/IStore (schema [_] {}))
        source (database store)
        original (d/datom 1 :value 10)
        pending (d/datom 2 :value 20)]
    (doseq [index [:eavt :avet]]
      (.add ^TreeSortedSet (index source) original))
    (let [view (db/transfer source store)]
      (doseq [index [:eavt :avet]]
        (let [^TreeSortedSet old (index source)
              ^TreeSortedSet fresh (index view)]
          (is (not (identical? old fresh)))
          (is (identical? (.comparator old) (.comparator fresh)))
          (is (.isEmpty fresh))
          (.add fresh pending)
          (is (= [original] (vec old)))
          (.clear old)
          (is (= [pending] (vec fresh)))))
      (is (= (select-keys source [:max-eid :max-tx])
             (select-keys view [:max-eid :max-tx])))
      (is (= (db/runtime-opts source) (db/runtime-opts view)))
      (is (identical? (:pull-patterns source) (:pull-patterns view)))
      (is (identical? (:pull-readers source) (:pull-readers view))))))

(deftest different-store-transfers-bind-comparisons-to-the-target-store
  (let [reads (atom [])
        source-store (reify i/IStore (schema [_] (swap! reads conj :source) {}))
        target-store (reify i/IStore (schema [_] (swap! reads conj :target) {}))
        source (database source-store)
        view (db/transfer source target-store)
        a (d/datom 1 :value 10)
        b (d/datom 1 :value 20)]
    (doseq [index [:eavt :avet]]
      (let [^TreeSortedSet old (index source)
            ^TreeSortedSet fresh (index view)]
        (is (not (identical? (.comparator old) (.comparator fresh))))
        (is (neg? (.compare ^Comparator (.comparator fresh) a b)))))
    (is (= [:target :target] @reads))
    (is (identical? target-store (:store view)))))

(deftest shared-comparators-continue-reading-the-current-schema
  (let [current (atom {})
        reads (atom [])
        store (reify i/IStore
                (schema [_] (swap! reads conj @current) @current))
        source (database store)
        a (d/datom 1 :value 10)
        b (d/datom 1 :value 20)]
    (doseq [index [:eavt :avet]]
      (.compare ^Comparator (.comparator ^TreeSortedSet (index source)) a b))
    (reset! current {:value {:db/valueType :db.type/long}})
    (reset! reads [])
    (let [view (db/transfer source store)]
      (doseq [index [:eavt :avet]]
        (is (neg? (.compare ^Comparator (.comparator ^TreeSortedSet (index view)) a b))))
      (is (= [@current @current] @reads)))))
