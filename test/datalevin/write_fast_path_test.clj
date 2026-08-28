;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice.
;;
(ns datalevin.write-fast-path-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [java.util UUID]))

(def ^:private schema
  {:item/id    {:db/valueType :db.type/long
                :db/unique    :db.unique/identity}
   :item/value {:db/valueType :db.type/string}
   :item/left  {:db/valueType :db.type/boolean}
   :item/right {:db/valueType :db.type/boolean}})

(defn- item-batch
  [start n]
  (let [start (long start)
        n     (long n)]
    (mapv (fn [id]
            {:item/id id :item/value (str "v" id)})
          (range start (+ start n)))))

(deftest large-unique-insert-preparation-is-conservatively-gated
  (let [dir  (u/tmp-dir (str "blind-unique-prepare-" (UUID/randomUUID)))
        conn (d/get-conn dir schema)]
    (try
      (testing "small unique transactions retain the full upsert path"
        (is (nil? (db/prepare-blind-local-tx @conn (item-batch 0 255)))))
      (testing "large, distinct scalar identity values use blind preparation"
        (let [prepared (db/prepare-blind-local-tx @conn (item-batch 0 256))]
          (is (some? prepared))
          (is (= 256 (count (:entities prepared))))
          (is (= 256 (count (:unique-avs prepared))))))
      (testing "duplicate identities and tempids fall back to full resolution"
        (let [txs (into [{:db/id "same" :item/id 1 :item/left true}
                         {:db/id "same" :item/id 1 :item/right true}]
                        (item-batch 2 254))]
          (is (nil? (db/prepare-blind-local-tx @conn txs)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest large-unique-inserts-preserve-upsert-and-durability-semantics
  (doseq [wal? [false true]]
    (testing (if wal? "WAL" "default LMDB")
      (let [dir   (u/tmp-dir (str "blind-unique-write-" wal? "-"
                                  (UUID/randomUUID)))
            conn* (atom nil)
            opts  {:wal? wal?}]
        (try
          (let [conn (d/get-conn dir schema opts)]
            (reset! conn* conn)
            @(d/transact-async conn (item-batch 0 256))
            (is (= 256 (d/count-datoms @conn nil :item/id nil)))
            (is (= "v42"
                   (:item/value (d/entity @conn [:item/id 42]))))

            (testing "an existing identity makes the whole batch fall back"
              (d/transact!
                conn
                (into [{:item/id 42 :item/value "updated"}]
                      (item-batch 256 255)))
              (is (= 511 (d/count-datoms @conn nil :item/id nil)))
              (is (= "updated"
                     (:item/value (d/entity @conn [:item/id 42])))))

            (testing "duplicates retain normal tempid merge semantics"
              (d/transact!
                conn
                (into [{:db/id "same" :item/id 1000 :item/left true}
                       {:db/id "same" :item/id 1000 :item/right true}]
                      (item-batch 1001 254)))
              (let [entity (d/entity @conn [:item/id 1000])]
                (is (true? (:item/left entity)))
                (is (true? (:item/right entity))))
              (is (= 766 (d/count-datoms @conn nil :item/id nil))))

            (d/close conn)
            (reset! conn* nil))

          (testing "committed data survives reopening"
            (let [conn (d/get-conn dir schema opts)]
              (reset! conn* conn)
              (is (= 766 (d/count-datoms @conn nil :item/id nil)))
              (is (= "updated"
                     (:item/value (d/entity @conn [:item/id 42]))))))
          (finally
            (when-let [conn @conn*]
              (d/close conn))
            (u/delete-files dir)))))))
