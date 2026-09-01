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
   [datalevin.binding.cpp :as cpp]
   [datalevin.conn :as conn]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.db :as db]
   [datalevin.util :as u])
  (:import
   [datalevin.dtlvnative DTLV]
   [datalevin.lmdb DatomKVTxData]
   [java.util ArrayList UUID]
   [java.util.concurrent.atomic AtomicLong]
   [org.eclipse.collections.impl.list.mutable FastList]))

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

(defn- invoke-conn-private
  [sym & args]
  (apply (ns-resolve 'datalevin.conn sym) args))

(defn- report-datom-tuples
  [report]
  (mapv (fn [datom]
          [(:e datom) (:a datom) (:v datom) (:tx datom) (:added datom)])
        (:tx-data report)))

(deftest wal-default-profile-is-strict
  (is (= :strict c/*wal-durability-profile*))
  (is (= :strict c/*datalog-wal-durability-profile*))
  (testing "Datalog WAL uses the strict dispatch path when omitted"
    (let [conn* (d/create-conn nil
                               schema
                               {:wal? true
                                :kv-opts {:inmemory? true}})
          paths (atom [])]
      (try
        (binding [conn/*txlog-sync-path-observer*
                  #(swap! paths conj %)]
          (d/transact! conn* [{:item/id 1 :item/value "v1"}]))
        (is (= [:direct-wal-idle-strict] @paths))
        (finally
          (d/close conn*)))))
  (testing "direct KV WAL reports the strict default"
    (let [dir      (u/tmp-dir (str "strict-wal-default-"
                                   (UUID/randomUUID)))
          kv-store (d/open-kv dir {:wal? true})]
      (try
        (is (= :strict
               (:durability-profile (d/txlog-watermarks kv-store))))
        (finally
          (d/close-kv kv-store)
          (u/delete-files dir))))))

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
      (.add txs (DatomKVTxData. 1 (byte-array 0) true false))
      (is (true? (add-only? txs))))
    (testing "any retraction retains the general transaction path"
      (.add txs (DatomKVTxData. 2 (byte-array 0) false false))
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
          (is (= 2 (count (:unique-avs prepared))))
          (is (true? (:fuse-unique-inserts? prepared)))))
      (testing "WAL can opt a single unique insert into blind preparation"
        (let [prepared (db/prepare-blind-local-tx
                         @conn (item-batch 2 1) true)]
          (is (some? prepared))
          (is (= 1 (count (:entities prepared))))
          (is (= 1 (count (:unique-avs prepared))))
          (is (true? (:fuse-unique-inserts? prepared)))
          (is (= [:item/id 2] (:identity-upsert-av prepared)))))
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

(deftest combined-queued-wal-inserts-use-blind-preparation
  (let [dir  (u/tmp-dir (str "blind-queued-wal-" (UUID/randomUUID)))
        conn (d/get-conn dir schema {:wal? true
                                     :wal-durability-profile :relaxed})]
    (try
      (let [requests (doto (FastList. 2)
                       (.add (conn/->SyncQueuedReq
                               (item-batch 0 10) {:request 0} (promise)))
                       (.add (conn/->SyncQueuedReq
                               (item-batch 10 10) {:request 1} (promise))))
            ^objects prepared
            (invoke-conn-private
              'prepare-sync-queued-blind-batch conn requests)
            ^objects reports (object-array 2)]
        (is (some? prepared))
        (is (true?
              (invoke-conn-private
                'try-commit-sync-queued-blind-batch!
                conn requests prepared reports)))
        (let [report-0 (aget reports 0)
              report-1 (aget reports 1)]
          (is (= {:request 0} (:tx-meta report-0)))
          (is (= {:request 1} (:tx-meta report-1)))
          (is (not= (:db/current-tx (:tempids report-0))
                    (:db/current-tx (:tempids report-1))))
          (is (= 20 (d/count-datoms @conn nil :item/id nil)))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest combined-queued-blind-preparation-falls-back-safely
  (let [dir  (u/tmp-dir (str "blind-queued-fallback-" (UUID/randomUUID)))
        conn (d/get-conn dir schema {:wal? true
                                     :wal-durability-profile :relaxed})]
    (try
      (testing "identities duplicated across requests require full resolution"
        (let [requests (doto (FastList. 2)
                         (.add (conn/->SyncQueuedReq
                                 [{:item/id 1 :item/value "first"}]
                                 nil (promise)))
                         (.add (conn/->SyncQueuedReq
                                 [{:item/id 1 :item/value "second"}]
                                 nil (promise))))]
          (is (nil?
                (invoke-conn-private
                  'prepare-sync-queued-blind-batch conn requests)))))
      (testing "an existing identity aborts before any queued write is made"
        (d/transact! conn [{:item/id 1 :item/value "existing"}])
        (let [requests (doto (FastList. 2)
                         (.add (conn/->SyncQueuedReq
                                 [{:item/id 1 :item/value "updated"}]
                                 nil (promise)))
                         (.add (conn/->SyncQueuedReq
                                 [{:item/id 2 :item/value "new"}]
                                 nil (promise))))
              ^objects prepared
              (invoke-conn-private
                'prepare-sync-queued-blind-batch conn requests)
              ^objects reports (object-array 2)]
          (is (some? prepared))
          (is (false?
                (invoke-conn-private
                  'try-commit-sync-queued-blind-batch!
                  conn requests prepared reports)))
          (is (= "existing" (:item/value
                               (d/entity @conn [:item/id 1]))))
          (is (nil? (d/entity @conn [:item/id 2])))))
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

      (testing "an existing identity retains normal upsert semantics"
        (d/transact! conn [{:item/id 1 :item/value "updated"}])
        (is (= 1 (d/count-datoms @conn nil :item/id nil)))
        (is (= "updated" (:item/value (d/entity @conn [:item/id 1])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest simple-existing-identity-wal-upserts-use-specialized-stamping
  (let [specialized-dir (u/tmp-dir
                          (str "identity-upsert-specialized-"
                               (UUID/randomUUID)))
        fallback-dir    (u/tmp-dir
                          (str "identity-upsert-fallback-"
                               (UUID/randomUUID)))
        specialized     (d/get-conn specialized-dir schema {:wal? true})
        fallback        (d/get-conn fallback-dir schema {:wal? true})
        initial         {:item/id    1
                         :item/value "initial"
                         :item/left  true}
        update          {:db/id      "upsert-tempid"
                         :item/id    1
                         :item/value "updated"
                         :item/left  true
                         :item/right false}
        tx-meta         {:source :identity-upsert-test}
        paths           (atom [])]
    (try
      (d/transact! specialized [initial])
      (d/transact! fallback [initial])
      (let [specialized-report
            (binding [conn/*local-wal-tx-path-observer*
                      #(swap! paths conj %)]
              (d/transact! specialized [update] tx-meta))
            fallback-report
            (binding [conn/*local-wal-identity-upsert?* false]
              (d/transact! fallback [update] tx-meta))
            eid (get (:tempids specialized-report) "upsert-tempid")]
        (is (= [:identity-upsert] @paths))
        (is (= (:tempids fallback-report)
               (:tempids specialized-report)))
        (is (= tx-meta (:tx-meta specialized-report)))
        (is (= (report-datom-tuples fallback-report)
               (report-datom-tuples specialized-report)))
        (is (= #{[:item/value "initial" false]
                 [:item/value "updated" true]
                 [:item/right false true]}
               (into #{}
                     (map (fn [datom]
                            [(:a datom) (:v datom) (:added datom)]))
                     (:tx-data specialized-report))))
        (is (= eid (:db/id (d/touch (d/entity @specialized
                                               [:item/id 1])))))
        (is (= (into {} (d/touch (d/entity @fallback [:item/id 1])))
               (into {} (d/touch
                          (d/entity @specialized [:item/id 1])))))

        (testing "a redundant identity upsert still advances the transaction"
          (let [before (:max-tx @specialized)
                report (d/transact! specialized
                                    [{:item/id    1
                                      :item/value "updated"
                                      :item/left  true
                                      :item/right false}])]
            (is (empty? (:tx-data report)))
            (is (= (inc (long before)) (:max-tx @specialized)))
            (is (= (:max-tx @specialized)
                   (:db/current-tx (:tempids report)))))))
      (finally
        (d/close specialized)
        (d/close fallback)
        (u/delete-files specialized-dir)
        (u/delete-files fallback-dir)))))

(deftest complex-identity-wal-upserts-retain-general-fallback
  (let [dir   (u/tmp-dir (str "identity-upsert-general-fallback-"
                              (UUID/randomUUID)))
        schema' (assoc schema
                       :item/tags {:db/valueType   :db.type/string
                                   :db/cardinality :db.cardinality/many})
        conn  (d/get-conn dir schema' {:wal? true})
        paths (atom [])]
    (try
      (d/transact! conn [{:item/id 1 :item/tags ["one"]}])
      (binding [conn/*local-wal-tx-path-observer*
                #(swap! paths conj %)]
        (d/transact! conn [{:item/id 1 :item/tags ["two"]}]))
      (is (= [:general] @paths))
      (is (= #{"one" "two"}
             (:item/tags (d/touch (d/entity @conn [:item/id 1])))))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(deftest direct-transactions-do-not-mutate-published-db-overlays
  (doseq [wal? [false true]]
    (testing (if wal? "WAL" "default LMDB")
      (let [dir  (u/tmp-dir (str "isolated-tx-cache-" wal? "-"
                                 (UUID/randomUUID)))
            conn (d/get-conn dir schema {:wal? wal?})]
        (try
          (d/transact! conn [{:item/id 1 :item/value "initial"}])
          (let [published   @conn
                eavt-before (vec (:eavt published))
                avet-before (vec (:avet published))]
            ;; Existing identities use either the ordinary resolver or the
            ;; WAL identity specialization. Neither may mutate this published
            ;; DB value's transaction-local overlays.
            (d/transact! conn [{:item/id 1 :item/value "updated"}])
            (is (= eavt-before (vec (:eavt published))))
            (is (= avet-before (vec (:avet published))))
            (is (= "updated"
                   (:item/value (d/entity @conn [:item/id 1])))))
          (finally
            (d/close conn)
            (u/delete-files dir)))))))

(deftest giant-identity-values-use-logical-unique-lookups
  (let [giant-schema {:item/id    {:db/valueType :db.type/string
                                    :db/unique    :db.unique/identity}
                      :item/value {:db/valueType :db.type/string}}
        a            (apply str (repeat 600 \a))
        b            (apply str (repeat 600 \b))]
    (doseq [wal? [false true]]
      (testing (if wal? "WAL" "default LMDB")
        (let [dir  (u/tmp-dir (str "giant-identity-" wal? "-"
                                   (UUID/randomUUID)))
              conn (d/get-conn dir giant-schema {:wal? wal?})]
          (try
            (testing "giant identities stay out of the physical-key fast path"
              (is (nil? (db/prepare-blind-local-tx
                          @conn [{:item/id a} {:item/id b}]))))
            (d/transact! conn [{:item/id a :item/value "a"}
                               {:item/id b :item/value "b"}])
            (d/transact! conn [{:item/id b :item/value "updated"}])
            (is (= 2 (d/count-datoms @conn nil :item/id nil)))
            (is (= "updated"
                   (:item/value (d/entity @conn [:item/id b]))))
            (finally
              (d/close conn)
              (u/delete-files dir))))))))

(deftest ordered-ave-duplicate-appends-preserve-data
  (let [dir             (u/tmp-dir (str "ordered-ave-duplicates-"
                                        (UUID/randomUUID)))
        conn*           (atom nil)
        multiple-counts (atom [])
        buffer-sizes    (atom [])]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        ;; The second batch appends to duplicate trees created by the first.
        (binding [cpp/*ave-multiple-write-observer*
                  #(swap! multiple-counts conj %)
                  cpp/*ave-multiple-buffer-allocation-observer*
                  #(swap! buffer-sizes conj %)]
          (d/transact! conn (repeated-value-batch 0 256))
          (d/transact! conn (repeated-value-batch 256 256)))
        (is (= (repeat 6 256) (sort @multiple-counts)))
        (is (= [(* 256 Long/BYTES)] @buffer-sizes))
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

(deftest ave-multiple-write-starts-at-two-duplicates
  (let [dir             (u/tmp-dir (str "ave-multiple-threshold-"
                                        (UUID/randomUUID)))
        conn*           (atom nil)
        multiple-counts (atom [])
        buffer-sizes    (atom [])]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        (binding [cpp/*ave-multiple-write-observer*
                  #(swap! multiple-counts conj %)
                  cpp/*ave-multiple-buffer-allocation-observer*
                  #(swap! buffer-sizes conj %)]
          (testing "one duplicate stays on the scalar write path"
            (d/transact! conn (repeated-value-batch 0 1))
            (is (empty? @multiple-counts))
            (is (empty? @buffer-sizes)))
          (testing "two duplicates use one multiple write per shared value"
            (d/transact! conn (repeated-value-batch 1 2))
            (is (= (repeat 3 2) @multiple-counts))
            (is (= [(* 2 Long/BYTES)] @buffer-sizes)))
          (testing "the reusable buffer grows geometrically"
            (d/transact! conn (repeated-value-batch 3 3))
            (is (= (concat (repeat 3 2) (repeat 3 3))
                   @multiple-counts))
            (is (= [(* 2 Long/BYTES) (* 4 Long/BYTES)]
                   @buffer-sizes))))
        (doseq [attr [:item/id :item/value :item/left :item/right]]
          (is (= 6 (d/count-datoms @conn nil attr nil)))))
      (finally
        (when-let [conn @conn*]
          (d/close conn))
        (u/delete-files dir)))))

(deftest mixed-ave-run-sizes-apply-the-threshold-per-run
  (let [dir             (u/tmp-dir (str "ave-multiple-mixed-runs-"
                                        (UUID/randomUUID)))
        conn*           (atom nil)
        multiple-counts (atom [])
        buffer-sizes    (atom [])]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        (binding [cpp/*ave-multiple-write-observer*
                  #(swap! multiple-counts conj %)
                  cpp/*ave-multiple-buffer-allocation-observer*
                  #(swap! buffer-sizes conj %)]
          (d/transact!
            conn
            (mapv (fn [id]
                    {:item/id    id
                     :item/value (if (zero? (long id)) "one" "two")})
                  (range 3))))
        (is (= [2] @multiple-counts))
        (is (= [(* 2 Long/BYTES)] @buffer-sizes))
        (is (= 1 (d/count-datoms @conn nil :item/value "one")))
        (is (= 2 (d/count-datoms @conn nil :item/value "two"))))
      (finally
        (when-let [conn @conn*]
          (d/close conn))
        (u/delete-files dir)))))

(deftest appenddup-eligibility-can-split-one-ave-run
  (let [dir         (u/tmp-dir (str "ave-multiple-split-run-"
                                    (UUID/randomUUID)))
        conn*       (atom nil)
        write-flags (atom [])]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        (d/transact!
          conn
          (mapv (fn [id]
                  (cond-> {:item/id id}
                    (= id 4) (assoc :item/left true)))
                (range 9)))
        (binding [cpp/*ave-multiple-write-flags-observer*
                  #(swap! write-flags conj [%1 %2])]
          (d/transact!
            conn
            (mapv (fn [id]
                    {:db/id [:item/id id] :item/left true})
                  [0 1 2 3 5 6 7 8])))
        (is (= [[4 0] [4 DTLV/MDB_APPENDDUP]] @write-flags))
        (is (= 9 (d/count-datoms @conn nil :item/left true))))
      (finally
        (when-let [conn @conn*]
          (d/close conn))
        (u/delete-files dir)))))

(deftest one-shot-prepared-ave-writes-select-appenddup-per-run
  (let [dir          (u/tmp-dir (str "ave-multiple-per-run-appenddup-"
                                     (UUID/randomUUID)))
        conn*        (atom nil)
        write-flags  (atom [])]
    (try
      (let [conn (d/get-conn dir schema)]
        (reset! conn* conn)
        ;; Establish a high existing tail for :item/left and a low one for
        ;; :item/right. The next one-shot transaction is add-only, but not a
        ;; globally appendable entity batch.
        (d/transact!
          conn
          (mapv (fn [id]
                  (cond-> {:item/id id}
                    (= id 0) (assoc :item/right true)
                    (= id 7) (assoc :item/left true)))
                (range 8)))
        (binding [cpp/*ave-multiple-write-flags-observer*
                  #(swap! write-flags conj [%1 %2])]
          (d/transact!
            conn
            (into
              (mapv (fn [id]
                      {:db/id [:item/id id] :item/left true})
                    (range 4))
              (mapv (fn [id]
                      {:db/id [:item/id id] :item/right true})
                    (range 4 8)))))
        (is (= #{[4 0] [4 DTLV/MDB_APPENDDUP]}
               (set @write-flags)))
        (is (= 5 (d/count-datoms @conn nil :item/left true)))
        (is (= 5 (d/count-datoms @conn nil :item/right true))))
      (finally
        (when-let [conn @conn*]
          (d/close conn))
        (u/delete-files dir)))))

(deftest public-kv-writes-reject-the-internal-multiple-flag
  (let [dir (u/tmp-dir (str "reject-public-multiple-"
                            (UUID/randomUUID)))
        kv* (atom nil)]
    (try
      (let [kv (d/open-kv dir)]
        (reset! kv* kv)
        (d/open-dbi kv "values")
        (let [error (try
                      (d/transact-kv
                        kv "values" [[:put 1 2 #{:multiple}]] :long :long)
                      nil
                      (catch Exception e e))]
          (is (some? error))
          (is (re-find #":multiple" (ex-message error)))
          (is (nil? (d/get-value kv "values" 1 :long :long)))))
      (finally
        (when-let [kv @kv*]
          (d/close-kv kv))
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
