(ns datalevin.cache-publication-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u])
  (:import [java.util UUID]))

(use-fixtures :each db-fixture)

(defn- tuple-ids [tuples]
  (into #{} (map #(aget ^objects % 0)) tuples))

(deftest disabled-reader-cannot-cache-a-pre-commit-snapshot
  (doseq [wal? [false true], abort? [false true], grouped? [false true]]
    (testing (str "wal=" wal? ", abort=" abort? ", grouped=" grouped?)
      (let [dir     (u/tmp-dir (str "cache-publication-" (UUID/randomUUID)))
            conn    (d/create-conn dir {:value {:db/valueType :db.type/long}}
                                   {:wal? wal?})
            store   (:store @conn)
            ranges  [[[:closed 0] [:closed 200]]]
            key     [:init-tuples :value ranges nil false]
            sampled (promise)
            release (promise)
            reader  (atom nil)]
        (try
          (d/transact! conn [{:db/id 1 :value 100}])
          (d/with-transaction [cn conn]
            (let [write (fn [tx]
                          (d/with-transaction [nested tx]
                            (d/transact! nested [{:db/id 2 :value 101}])
                            (d/transact! nested [{:db/id 2 :value 102}])))]
              (if grouped?
                (db/execute-write-group cn write)
                (write cn)))
            ;; Nested completion must leave caching disabled until the outer
            ;; native transaction commits or rolls back.
            (is (db/cache-disabled? store))
            (reset! reader
                    (future
                      (let [token (db/cache-token store)
                            rows  (i/ave-tuples-list store :value ranges nil false)]
                        ;; Hold the cache miss between its native read and
                        ;; conditional publication, as in wrap-cache/q-result.
                        (deliver sampled (tuple-ids rows))
                        (when (= ::timeout (deref release 5000 ::timeout))
                          (throw (ex-info "Writer did not release reader" {})))
                        {:ids (tuple-ids rows)
                         :published? (db/cache-put-if-current store token key rows)})))
            (is (= #{1} (deref sampled 5000 ::timeout)))
            (when abort? (d/abort-transact cn)))
          (is (not (db/cache-disabled? store)))
          (let [expected (if abort? #{1} #{1 2})]
            (is (= expected (tuple-ids (i/ave-tuples-list store :value ranges nil false))))
            (deliver release true)
            (is (= {:ids #{1} :published? false} (deref @reader 5000 ::timeout)))
            ;; The old reader can return its own snapshot, but later cached
            ;; reads must agree with native storage after transaction completion.
            (is (= expected (tuple-ids (db/-init-tuples-list @conn :value ranges nil false)))))
          (finally
            (deliver release true)
            (when-let [task @reader] (future-cancel task))
            (d/close conn)
            (u/delete-files dir)))))))
