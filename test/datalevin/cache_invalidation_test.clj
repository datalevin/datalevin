(ns datalevin.cache-invalidation-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.datom :as d]
            [datalevin.db :as db])
  (:import [datalevin.utl LRUCache]
           [java.util.function Function]))

(defn- indexed-cache [capacity]
  (LRUCache. capacity 0
             (reify Function
               (apply [_ k] (#'db/cache-key-dependencies k)))))

(deftest candidates-preserve-full-scan-invalidation-semantics
  (let [keys (vec
               (concat
                 (for [tag [:search :search-tuples :first :count]
                       e [nil 1 2 :ident [:id "one"]]
                       a [nil :a :b]
                       v [nil 10 20 :ident [:id "one"]]]
                   [tag e a v])
                 (for [tag [:populated? :datoms :seek :rseek]
                       index [:eav :ave :unknown]
                       e [nil 1 2 :ident [:id "one"]]
                       a [nil :a :b]
                       v [nil 10 :ident [:id "one"]]]
                   (into [tag index] (if (= index :ave) [a v e] [e a v])))
                 (for [e [nil 1 2 :ident [:id "one"]]] [:e-datoms e])
                 (for [a [nil :a :b], v [nil 10 :ident [:id "one"]]]
                   [:av-datoms a v])
                 (for [tag [:init-tuples :sample-init-tuples :e-sample
                            :default-ratio :cardinality], a [nil :a :b]]
                   [tag a])
                 (for [tag [:val-eq-scan-e :val-eq-filter-e], a [nil :a :b]]
                   [tag :input 0 a])
                 (for [attrs [[] [:a] [:b] [:a :b] [[:a :value] :b :b]]]
                   [:eav-scan-v :input 0 attrs])
                 (for [deps [nil {} {:attrs #{}} {:attrs #{:a}} {:attrs #{:b}}
                             {:attrs #{:a :b}} {:all? true :attrs #{:a}}]]
                   [:query-result deps :query :inputs])
                 [[:range-datoms :eav (d/datom 1 :a 0) (d/datom 2 :a 20)]
                  [:index-range :a 0 20] [:index-range-size :b nil nil]
                  [:index-range :ref [:id "one"] [:id "two"]]
                  [:unknown 1] [{:old-query :key}] :unknown]))
        cache (indexed-cache (count keys))]
    (doseq [k keys] (.put cache k :cached))
    (doseq [tx [[(d/datom 1 :a 10)] [(d/datom 2 :b 20)]
                [(d/datom 3 :c 30)] [(d/datom 1 :b :ident)]
                [(d/datom 1 :a 10) (d/datom 2 :b 20)]]]
      (let [touches (#'db/tx-touch-summary tx)
            affected? #(#'db/tx-affects-cache-key? touches %)
            expected (set (filter affected? keys))
            candidates (.candidateKeys cache (#'db/cache-invalidation-dependencies touches false))]
        (is (= expected (set (filter affected? candidates))) (str tx))
        ;; Local native range checks can be broader than the remote predicate.
        ;; They must all remain candidates, including unresolved reference bounds.
        (is (every? (set candidates)
                    (filter #(and (vector? %)
                                  (#{:range-datoms :index-range :index-range-size}
                                    (first %))) keys)))))))

(deftest entity-lookups-avoid-unrelated-cache-entries
  (let [cache (indexed-cache 512)]
    (doseq [e (range 1 513)] (.put cache [:e-datoms e] e))
    (testing "a point write selects just that entity from a full cache"
      (let [touches (#'db/tx-touch-summary [(d/datom 42 :a 10)])]
        (is (= #{[:e-datoms 42]}
               (set (.candidateKeys cache (#'db/cache-invalidation-dependencies touches false)))))))
    (testing "eviction removes the old dependency before the key is reused"
      (.put cache [:e-datoms 513] 513)
      (let [touches (#'db/tx-touch-summary [(d/datom 1 :a 10)])]
        (is (empty? (.candidateKeys cache (#'db/cache-invalidation-dependencies touches false)))))
      (.put cache [:e-datoms 1] :new)
      (is (= #{[:e-datoms 1]}
             (set (.candidateKeys cache [[:datalevin.db/entity 1]])))))))

(deftest pull-ranges-use-entity-buckets-only-for-local-invalidation
  (let [cache (indexed-cache 512)
        key (fn [e] [:range-datoms :eav (d/datom e :a nil) (d/datom e :z nil)])
        touches (#'db/tx-touch-summary [(d/datom 42 :m 10)])]
    ;; Explicit pull projections cache a native range, even for one entity.
    (doseq [e (range 1 513)] (.put cache (key e) e))
    (is (= #{(key 42)}
           (set (.candidateKeys cache (#'db/cache-invalidation-dependencies touches true)))))
    ;; Remote invalidation keeps its existing schema-independent fallback.
    (is (= 512 (count (.candidateKeys cache
                                     (#'db/cache-invalidation-dependencies touches false))))))
  (let [cache (indexed-cache 4)
        broad [:range-datoms :eav (d/datom 1 :a nil) (d/datom 10 :z nil)]
        unknown [:range-datoms :unknown (d/datom 1 :a nil) (d/datom 1 :z nil)]
        touches (#'db/tx-touch-summary [(d/datom 20 :m 10)])]
    (.put cache broad :broad)
    (.put cache unknown :unknown)
    (is (= #{broad unknown}
           (set (.candidateKeys cache (#'db/cache-invalidation-dependencies touches true)))))))
