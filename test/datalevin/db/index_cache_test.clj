(ns datalevin.db.index-cache-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.datom :as datom]
            [datalevin.db :as db]
            [datalevin.interface :as i]))

(defn- with-cache [f]
  (let [store (reify i/IStore
                (dir [_] (str ::index-cache))
                (opts [_] {:cache-limit 64}))]
    (db/refresh-cache store 1)
    (try (f store) (finally (db/remove-cache store)))))

(deftest index-cache-preserves-storage-representations-and-invalidation
  (doseq [[stored missing] [[[1 2] '(1 2)]
                            [{:key [1 2]} {:key '(1 2)}]
                            [1.0M 1.00M]]
          pattern [(fn [v] [:search nil :value v])
                   (fn [v] [:search-tuples nil :value v])
                   (fn [v] [:first nil :value v])
                   (fn [v] [:count nil :value v nil])
                   (fn [v] [:populated? :ave :value v nil])
                   (fn [v] [:datoms :ave :value v nil])
                   (fn [v] [:datoms :eav 1 :value v])
                   (fn [v] [:datoms :ave :value v nil 1])
                   (fn [v] [:av-datoms :value v])
                   (fn [v] [:seek :ave :value v nil])
                   (fn [v] [:rseek :ave :value v nil 1])
                   (fn [v] [:index-range :value v v])
                   (fn [v] [:index-range-size :value v v])
                   (fn [v] [:range-datoms :ave
                            (datom/datom 0 :value v) (datom/datom 100 :value v)])]]
    (testing (str (first (pattern stored)) " " stored " / " missing)
      (with-cache
        (fn [store]
          (let [calls (atom 0)
                stored-pattern (pattern stored)
                missing-pattern (pattern missing)
                read (fn [input result]
                       (let [key (db/index-cache-key store input)]
                         (if-some [cached (when key (db/cache-get store key))]
                           cached
                           (do
                             (swap! calls inc)
                             (when key (db/cache-put store key result))
                             result))))]
            (is (= stored-pattern missing-pattern))
            (is (= [:stored] (read stored-pattern [:stored])))
            (is (= [] (read missing-pattern [])))
            (is (= [:stored] (read stored-pattern :unexpected)))
            (is (= [] (read missing-pattern :unexpected)))
            (is (= 2 @calls))
            (#'db/invalidate-cache store [(datom/datom 1 :value stored)] 1 2)
            (is (= [:updated] (read stored-pattern [:updated])))
            (is (= [] (read missing-pattern [])))
            (is (<= 3 @calls 4))
            (#'db/invalidate-cache store [(datom/datom 1 :value missing)] 1 3)
            (is (= [:created] (read missing-pattern [:created])))))))))

(defrecord OpaqueValue [rank])

(deftest ordinary-and-opaque-index-cache-inputs
  (with-cache
    (fn [store]
      (doseq [key [[:datoms :ave :value 1 nil]
                   [:search 1 :value "one"]
                   [:cardinality :value]
                   [:query-result {:attrs #{:value}} :parsed :inputs]]]
        (is (identical? key (db/index-cache-key store key))))
      (doseq [value [(->OpaqueValue 1) (Object.)]]
        (is (nil? (db/index-cache-key store [:search nil :value value])))))))
