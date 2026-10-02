;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.private
  "Fresh-store M0 opener. The lease precedes native/WAL open and all private
  handles share the collector. Existing stores require the later recovery step;
  this prototype deliberately refuses to reopen them. Public APIs are unchanged."
  (:require [clojure.java.io :as io]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env]
            [datalevin.tx-group.batch.factory :as factory]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.txlog :as wal])
  (:import [java.io Closeable]))

(defn- close-wal! [state]
  (doseq [key [:segment-channel :sync-lock-channel]]
    (when-let [^Closeable channel (some-> (get state key) deref)]
      (.close channel))))

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
                                     :on-failure! #(batch/fence! @collector %)}})]
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
