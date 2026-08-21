;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.storage-group-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.interface :as i]
   [datalevin.storage :as storage]
   [datalevin.util :as u])
  (:import
   [java.util Collection UUID]
   [org.eclipse.collections.impl.list.mutable FastList]))

(defn- tuples
  [rows]
  (FastList. ^Collection (mapv object-array rows)))

(defn- scan-list
  [store rows eid-idx attrs-v]
  (mapv vec (i/eav-scan-v-list store (tuples rows) eid-idx attrs-v)))

(deftest sorted-eav-scan-groups-equal-entities
  (let [dir   (u/tmp-dir (str "storage-group-test-" (UUID/randomUUID)))
        store (storage/open
                dir
                {:a {:db/valueType :db.type/long}
                 :b {:db/valueType   :db.type/string
                     :db/cardinality :db.cardinality/many}}
                {:kv-opts {:flags (conj c/default-env-flags :nosync)}})]
    (try
      (i/load-datoms store [(d/datom 5 :a 1)
                            (d/datom 5 :b "x")
                            (d/datom 5 :b "y")
                            (d/datom 8 :a 7)
                            (d/datom 8 :b "z")])

      (testing "single-valued scans reuse hits and misses within an entity group"
        (let [predicate-calls (atom 0)
              pred            (fn [_]
                                (swap! predicate-calls inc)
                                true)]
          (is (= [[:five 5 1]
                  [:five-again 5 1]
                  [:eight 8 7]
                  [:eight-again 8 7]]
                 (scan-list store
                            [[:eight 8]
                             [:missing 99]
                             [:five 5]
                             [:eight-again 8]
                             [:five-again 5]
                             [:missing-again 99]]
                            1
                            [[:a {:pred pred :skip? false}]])))
          (is (= 2 @predicate-calls))))

      (testing "a provenance-certified scan can bypass the entity cache"
        (let [predicate-calls (atom 0)
              pred            (fn [_]
                                (swap! predicate-calls inc)
                                true)]
          (is (= [[:five 5 1]
                  [:five-again 5 1]
                  [:eight 8 7]
                  [:eight-again 8 7]]
                 (scan-list store
                            [[:eight 8]
                             [:five 5]
                             [:eight-again 8]
                             [:five-again 5]]
                            1
                            [[:a {:pred pred
                                  :skip? false
                                  :cache-eids? false}]])))
          (is (= 4 @predicate-calls))))

      (testing "multi-valued scans reuse the product for every tuple in a group"
        (is (= #{[:five 5 "x" "x"]
                 [:five 5 "x" "y"]
                 [:five 5 "y" "x"]
                 [:five 5 "y" "y"]
                 [:five-again 5 "x" "x"]
                 [:five-again 5 "x" "y"]
                 [:five-again 5 "y" "x"]
                 [:five-again 5 "y" "y"]
                 [:eight 8 "z" "z"]}
               (set (scan-list store
                               [[:eight 8] [:five 5] [:five-again 5]]
                               1
                               [[:b {:skip? false}]
                                [:b {:skip? false}]])))))

      (testing "tuple-dependent filters retain their per-tuple semantics"
        (is (= [[5 1 :hit] [8 7 :hit]]
               (scan-list store
                          [[5 1 :hit] [5 999 :miss] [8 7 :hit]]
                          0
                          [[:a {:fidx 1 :skip? true}]])))
        (is (= [[5 "x" :hit] [8 "z" :hit]]
               (scan-list store
                          [[5 "x" :hit] [5 "no" :miss] [8 "z" :hit]]
                          0
                          [[:b {:fidx 1 :skip? true}]]))))
      (finally
        (i/close store)
        (u/delete-files dir)))))
