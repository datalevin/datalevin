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
   [datalevin.binding.cpp]
   [datalevin.conn :as conn]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [datalevin.lmdb DatomKVTxData]
   [java.util ArrayList UUID]
   [java.util.concurrent.atomic AtomicLong]))

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

(defn- repeated-value-batch
  [start n]
  (let [start (long start)
        n     (long n)]
    (mapv (fn [id]
            {:item/id    id
             :item/value "shared"
             :item/left  true
             :item/right false})
          (range start (+ start n)))))

(deftest relaxed-wal-idle-writes-retain-txlog-grouping-on-direct-path
  (let [conn* (d/create-conn nil
                             schema
                             {:wal? true
                              :wal-durability-profile :relaxed
                              :kv-opts {:inmemory? true}})
        paths (atom [])]
    (try
      (binding [conn/*txlog-sync-path-observer*
                #(swap! paths conj %)]
        (dotimes [i 16]
          (d/transact! conn* [{:item/id i :item/value (str "v" i)}])))
      (is (= 16 (count @paths)))
      (is (every? #{:direct-wal-idle-relaxed} @paths))
      (is (= 16 (d/count-datoms @conn* nil :item/id nil)))
      (testing "pending pressure retains the combining queue fallback"
        (let [^AtomicLong pending (:sync-queue-pending (meta conn*))]
          (.set pending 1)
          (try
            (binding [conn/*txlog-sync-path-observer*
                      #(swap! paths conj %)]
              (d/transact! conn* [{:item/id 16 :item/value "v16"}]))
            (finally
              (.set pending 0))))
        (is (= :queued-relaxed (peek @paths)))
        (is (= 17 (d/count-datoms @conn* nil :item/id nil))))
      (finally
        (d/close conn*)))))

(deftest add-only-datom-writes-always-use-ordered-index-passes
  (let [add-only? (ns-resolve 'datalevin.binding.cpp
                              'add-only-datom-batch?)
        txs       (ArrayList.)]
    (is (some? add-only?))
    (testing "metadata-only and empty transactions do not select the path"
      (is (false? (add-only? txs)))
      (.add txs (Object.))
      (is (false? (add-only? txs))))
    (testing "one added datom is sufficient, with no size threshold"
      (.add txs (DatomKVTxData. 1 (byte-array 0) true))
      (is (true? (add-only? txs))))
    (testing "any retraction retains the general transaction path"
      (.add txs (DatomKVTxData. 2 (byte-array 0) false))
      (is (false? (add-only? txs))))))

(deftest supported-unique-inserts-use-small-batch-blind-preparation
  (let [dir  (u/tmp-dir (str "blind-unique-prepare-" (UUID/randomUUID)))
        conn (d/get-conn dir schema)]
    (try
      (testing "single non-WAL unique inserts retain the ordinary resolver"
        (is (nil? (db/prepare-blind-local-tx @conn (item-batch 0 1)))))
      (testing "two unique inserts use blind preparation"
        (let [prepared (db/prepare-blind-local-tx @conn (item-batch 0 2))]
          (is (some? prepared))
          (is (= 2 (count (:entities prepared))))
          (is (= 2 (count (:unique-avs prepared))))))
      (testing "WAL can opt a single unique insert into blind preparation"
        (let [prepared (db/prepare-blind-local-tx
                         @conn (item-batch 2 1) true)]
          (is (some? prepared))
          (is (= 1 (count (:entities prepared))))
          (is (= 1 (count (:unique-avs prepared))))))
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

(deftest single-unique-wal-inserts-preserve-upsert-semantics
  (let [dir  (u/tmp-dir (str "blind-unique-single-wal-" (UUID/randomUUID)))
        conn (d/get-conn dir schema {:wal? true})]
    (try
      (d/transact! conn [{:item/id 1 :item/value "initial"}])
      (is (= 1 (d/count-datoms @conn nil :item/id nil)))
      (is (= "initial" (:item/value (d/entity @conn [:item/id 1]))))

      (testing "an existing identity falls back to normal upsert resolution"
        (d/transact! conn [{:item/id 1 :item/value "updated"}])
        (is (= 1 (d/count-datoms @conn nil :item/id nil)))
        (is (= "updated" (:item/value (d/entity @conn [:item/id 1])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest ordered-ave-duplicate-appends-preserve-data
  (let [dir   (u/tmp-dir (str "ordered-ave-duplicates-" (UUID/randomUUID)))
        conn* (atom nil)]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        ;; The second batch appends to duplicate trees created by the first.
        (d/transact! conn (repeated-value-batch 0 256))
        (d/transact! conn (repeated-value-batch 256 256))
        (doseq [attr [:item/id :item/value :item/left :item/right]]
          (is (= 512 (d/count-datoms @conn nil attr nil))))
        (is (= "shared" (:item/value (d/entity @conn [:item/id 511]))))
        (is (true? (:item/left (d/entity @conn [:item/id 511]))))
        (is (false? (:item/right (d/entity @conn [:item/id 511]))))
        (d/close conn)
        (reset! conn* nil))

      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        (doseq [attr [:item/id :item/value :item/left :item/right]]
          (is (= 512 (d/count-datoms @conn nil attr nil)))))
      (finally
        (when-let [conn @conn*]
          (d/close conn))
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

            (testing "large add-only updates retain ordinary EAV puts"
              (d/transact!
                conn
                (mapv (fn [id]
                        {:db/id     [:item/id id]
                         :item/left true
                         :item/right true})
                      (range 256)))
              (is (= 256 (d/count-datoms @conn nil :item/left nil)))
              (is (= 256 (d/count-datoms @conn nil :item/right nil))))

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
