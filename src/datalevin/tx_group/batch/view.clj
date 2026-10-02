;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.view
  "Private M0 standalone read path for the new write protocol.

  A read handle is bound once at open to its canonical runtime's native store and
  terminal serving flag; neither reference changes within that runtime. A
  point/range/count read made outside a transaction then adds exactly two
  terminal-serving checks around the native reader: one after the handle is
  bound and before native access, and one before the detached result is handed to
  the caller. The lifetime borrow, fence-before-drain ordering, reader reuse and
  publication check stay in `datalevin.binding.cpp`'s reader acquisition, so this
  namespace acquires no collector lock, waits for no branch and does not load
  `tx-state`, `.kv` or `.view`.

  A replacement runtime gets new cells and handles; a fenced runtime rejects
  before native access."
  (:require [datalevin.interface :as i]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.env :as env])
  (:import [java.util.concurrent.atomic AtomicBoolean]))

(deftype ReadHandle [raw ^AtomicBoolean serving])

(defn- terminal-error
  ^Throwable []
  (ex-info "Read runtime is not serving"
           {:error :txlog/runtime-closed
            :outcome :not-committed
            :retryable? false}))

(defn read-handle
  "Bind one read handle to a canonical new-protocol environment.

  Binds the native store and the collector's serving flag once, as the read
  algorithm requires. The returned handle is what every point/range/count call
  uses."
  [environment]
  (ReadHandle. (:raw (env/resources environment))
               (batch/serving-flag (env/collector environment))))

(defn get-value
  "Terminal-checked point read. See `datalevin.interface/get-value`."
  ([^ReadHandle handle dbi-name k]
   (get-value handle dbi-name k :data :data))
  ([^ReadHandle handle dbi-name k k-type]
   (get-value handle dbi-name k k-type :data))
  ([^ReadHandle handle dbi-name k k-type v-type]
   (let [^AtomicBoolean serving (.serving handle)]
     (when-not (.get serving) (throw (terminal-error)))
     (let [result (i/get-value (.raw handle) dbi-name k k-type v-type)]
       (when-not (.get serving) (throw (terminal-error)))
       result))))

(defn get-range
  "Terminal-checked eager range read. See `datalevin.interface/get-range`."
  ([^ReadHandle handle dbi-name k-range]
   (get-range handle dbi-name k-range :data :data))
  ([^ReadHandle handle dbi-name k-range k-type]
   (get-range handle dbi-name k-range k-type :data))
  ([^ReadHandle handle dbi-name k-range k-type v-type]
   (let [^AtomicBoolean serving (.serving handle)]
     (when-not (.get serving) (throw (terminal-error)))
     (let [result (i/get-range (.raw handle) dbi-name k-range k-type v-type)]
       (when-not (.get serving) (throw (terminal-error)))
       result))))

(defn range-count
  "Terminal-checked eager range count. See `datalevin.interface/range-count`."
  ([^ReadHandle handle dbi-name k-range]
   (range-count handle dbi-name k-range :data))
  ([^ReadHandle handle dbi-name k-range k-type]
   (let [^AtomicBoolean serving (.serving handle)]
     (when-not (.get serving) (throw (terminal-error)))
     (let [result (i/range-count (.raw handle) dbi-name k-range k-type)]
       (when-not (.get serving) (throw (terminal-error)))
       result))))
