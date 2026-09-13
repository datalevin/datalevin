(ns datalevin.server.transaction-cache-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.server.handlers :as handlers]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import
   [java.util UUID]
   [org.eclipse.collections.impl.set.sorted.mutable TreeSortedSet]))

(use-fixtures :once db-fixture)

(defn- transact [db txs simulated?]
  (#'handlers/transact* {} db txs nil simulated? nil "tx-cache" false))

(deftest server-transactions-preserve-published-overlays-test
  (doseq [wal? [false true], simulated? [false true]]
    (testing (str "WAL=" wal? ", simulated=" simulated?)
      (let [dir (u/tmp-dir (str "server-tx-cache-" (UUID/randomUUID)))
            conn (d/get-conn dir {:value {:db/valueType :db.type/string}}
                             {:wal? wal? :background-sampling? false})]
        (try
          (let [published (:db-after (transact @conn [[:db/add 1 :value "first"]
                                                     [:db/add 2 :value "second"]]
                                              false))
                eavt (:eavt published)
                avet (:avet published)
                eavt-before (vec eavt)
                avet-before (vec avet)
                ;; A read can retain an iterator while another worker commits.
                eavt-reader (.iterator ^TreeSortedSet eavt)
                avet-reader (.iterator ^TreeSortedSet avet)
                report (transact published [[:db/add 1 :value "updated"]] simulated?)]
            (is (seq eavt-before))
            (is (seq avet-before))
            (is (identical? published (:db-before report)))
            (is (not (identical? eavt (:eavt (:db-after report)))))
            (is (not (identical? avet (:avet (:db-after report)))))
            (is (= eavt-before (vec eavt)))
            (is (= avet-before (vec avet)))
            (is (= eavt-before (vec (iterator-seq eavt-reader))))
            (is (= avet-before (vec (iterator-seq avet-reader))))
            (is (= {:value "updated"} (d/pull (:db-after report) [:value] 1))))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

(deftest failed-server-transaction-preserves-published-overlays-test
  (let [dir (u/tmp-dir (str "failed-server-tx-cache-" (UUID/randomUUID)))
        conn (d/get-conn dir {:value {:db/valueType :db.type/string}})]
    (try
      (let [published (:db-after (transact @conn [[:db/add 1 :value "first"]] false))
            eavt-before (vec (:eavt published))
            avet-before (vec (:avet published))]
        (is (thrown? clojure.lang.ExceptionInfo
                     (transact published [[:db/add 1 :value "discarded"]
                                          [:db/invalid 1 :value "invalid"]]
                               false)))
        (is (= eavt-before (vec (:eavt published))))
        (is (= avet-before (vec (:avet published))))
        (is (= {:value "first"} (d/pull published [:value] 1))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))
