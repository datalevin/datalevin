;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.private
  "Fresh-store M0 opener. The lease precedes native/WAL open and all private
  handles share the collector. Existing stores require the later recovery step;
  this prototype deliberately refuses to reopen them. Public APIs are unchanged."
  (:require [clojure.java.io :as io]
            [datalevin.bits :as b]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.txlog :as wal])
  (:import [datalevin.lmdb KVTxData]
           [java.io Closeable]
           [java.nio ByteBuffer]))

(defn- close-wal! [state]
  (doseq [key [:segment-channel :sync-lock-channel]]
    (when-let [^Closeable channel (some-> (get state key) deref)]
      (.close channel))))

(defn- rows-cost
  "Conservative encoded-size estimate for rows about to become one WAL body.

  Charged before the encoder runs, so a request that cannot fit is rejected
  before it allocates an encoding rather than after."
  ^long [rows]
  (reduce (fn [^long total row]
            (+ total (long charge/encoded-row-descriptor)
               (long charge/per-key-staging-state)
               (alength ^bytes (nth row 2 nil))
               (if (= :del (nth row 0 nil))
                 0
                 (alength ^bytes (nth row 3 nil)))))
          charge/buffer-wrapper
          rows))

(defn- rmw-adapters
  "Concrete KV adapters for ordered read-modify-write preparation.

  Bodies work on encoded keys and values, because those are exactly what the WAL
  codec and the native writer consume. One native reader is pinned for the whole
  preparation, so every body in a batch reads the same snapshot, and it is
  returned even when a body fails."
  [raw]
  {:with-base
   (fn [f]
     (let [reader (cpp/get-pending-rtx raw)]
       (try
         (f (fn [descriptor dbi ^bytes key]
             (let [handle (i/get-dbi raw dbi false)]
               (l/put-read-key handle reader key :raw)
               (when-let [^ByteBuffer buffer (l/get-kv handle reader)]
                 ;; Charge this request for the detached native read before the
                 ;; copy is made, so a large value cannot bypass the bound.
                 (batch/charge! descriptor
                                (charge/array-bytes 1 (long (.remaining buffer))))
                 (b/read-buffer buffer :raw)))))
         (finally (i/return-rtx raw reader)))))
   :row-fn (fn [dbi op k v]
             (if (= op :del)
               [:del dbi k nil :raw]
               ;; The staged key/value are already encoded by the body, so the
               ;; row is raw on both sides and no codec re-encodes them.
               [:put dbi k v :raw :raw]))
   :fold-row (fn [row]
               ;; Blind rows are built by the caller, so accept both shapes this
               ;; codebase produces: a `kv-tx` record and the plain vector that
               ;; `:row-fn` builds for a body.
               (if (instance? KVTxData row)
                 (let [row ^KVTxData row]
                   [(.-dbi-name row) (.-op row) (.-k row) (.-v row)])
                 [(nth row 1) (nth row 0) (nth row 2) (nth row 3)]))
   :body-cost rows-cost
   :encode-body (fn [rows hooks] (wal/prepare-append-body rows hooks))})

(defn open!
  "Open/attach a private fresh-store prototype. Requires :dir and :db-identity.
  DBIs are opened before submission through the private :raw resource."
  [{:keys [dir] :as opts}]
  (env/open-batch!
   (assoc opts :open-runtime!
          (fn []
            (when (or (.exists (io/file dir "data.mdb"))
                      (.exists (io/file (or (:wal-dir opts) (str dir "/txlog")))))
              (throw (ex-info "M0 private opener requires a fresh store"
                              {:error :txlog/prototype-recovery-required})))
            (let [db (l/open-kv dir
                                (assoc opts :wal? false :snapshot-scheduler? false
                                       :flags (conj (or (:flags opts) c/default-env-flags)
                                                    :writemap :nosync)))
                  raw (kv/raw-lmdb db)
                  ;; Bind the native lifetime guard before any handle can read.
                  ;; Reader borrows then attach a lease to their native
                  ;; transaction, and `close-kv` fences and drains those borrows
                  ;; before freeing native resources instead of tearing the
                  ;; environment down under an active reader.
                  _ (vswap! (i/kv-info raw) assoc
                            :native-lifetime (lifetime/create))]
              (try
                (let [{:keys [state]} (wal/init-runtime-state
                                      (assoc opts :wal? true :wal-shared? false
                                             :wal-full-prefix? true) nil)]
                  (try
                    (let [collector (volatile! nil)
                          runtime (factory/executor
                                   state raw
                                   {:runtime-control
                                    {:check-admission! #(batch/check-serving! @collector)
                                     :on-failure! #(batch/fence! @collector %)}
                                    ;; Ordered read-modify-write is the only source
                                    ;; of state-dependent bodies; blind requests
                                    ;; keep working unchanged beside it.
                                    :rmw-opts (rmw-adapters raw)})]
                      {:executor (:executor runtime)
                       :bind! #(vreset! collector %)
                       :resources {:raw raw :wal-state state}
                       :close! (fn []
                                 (when ((:close! runtime))
                                   (i/close-kv db)
                                   (close-wal! state)
                                   true))})
                    (catch Throwable t (close-wal! state) (throw t))))
                (catch Throwable t (i/close-kv db) (throw t))))))))
