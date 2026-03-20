;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.util
  "Shared HA helper functions used across runtime helper namespaces."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent ExecutorService TimeUnit]))

(defn long-max2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) a b)))

(defn long-max3 ^long
  [a b c]
  (long-max2 a (long-max2 b c)))

(defn long-max4 ^long
  [a b c d]
  (long-max2 (long-max2 a b) (long-max2 c d)))

(defn long-min2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (< a b) a b)))

(defn nonnegative-long-diff ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) (- a b) 0)))

(def ^:private ha-runtime-local-option-keys
  [:ha-node-id
   :ha-client-credentials
   :ha-fencing-hook
   :ha-clock-skew-hook])

(def ^:private ha-runtime-local-control-plane-option-keys
  [:local-peer-id
   :raft-dir])

(defn select-ha-runtime-local-opts
  "Select the node-local HA runtime config from a full HA opts map."
  [ha-opts]
  (let [ha-opts (or ha-opts {})
        local-opts (select-keys ha-opts ha-runtime-local-option-keys)
        runtime-control-plane (some-> (:ha-control-plane ha-opts)
                                      (select-keys
                                       ha-runtime-local-control-plane-option-keys))]
    (cond-> local-opts
      (seq runtime-control-plane)
      (assoc :ha-control-plane runtime-control-plane))))

(defn merge-ha-runtime-local-opts
  "Merge node-local HA runtime config back onto a store/runtime opts map without
  changing the shared persisted HA configuration."
  [store-opts runtime-opts]
  (let [store-opts            (or store-opts {})
        runtime-opts          (or runtime-opts {})
        local-runtime-opts    (select-ha-runtime-local-opts runtime-opts)
        local-opts            (select-keys local-runtime-opts
                                           ha-runtime-local-option-keys)
        runtime-control-plane (:ha-control-plane local-runtime-opts)
        store-control-plane   (:ha-control-plane store-opts)]
    (cond-> (merge store-opts local-opts)
      (seq runtime-control-plane)
      (assoc :ha-control-plane
             (merge (or store-control-plane {})
                    runtime-control-plane)))))

(defn effective-ha-runtime-local-opts
  "Return the best available node-local HA config from a runtime state map."
  [m]
  (merge-ha-runtime-local-opts
   (:ha-runtime-local-opts (or m {}))
   (some-> m :ha-runtime-opts select-ha-runtime-local-opts)))

(defn ordered-ha-members
  [m]
  (or (:ha-members-sorted m)
      (some->> (:ha-members m)
               (sort-by :node-id)
               vec)))

(defn ha-request-timeout-ms
  [m max-ms]
  (let [renew-ms (long (or (:ha-lease-renew-ms m)
                           c/*ha-lease-renew-ms*))
        rpc-timeout-ms (long (or (get-in m [:ha-control-plane :rpc-timeout-ms])
                                 0))
        budget-ms (max 1000 renew-ms rpc-timeout-ms)]
    (long (min (long max-ms) budget-ms))))

(defn ha-thread-label
  [db-name]
  (when (some? db-name)
    (let [label (-> (str db-name)
                    (s/replace #"[^A-Za-z0-9._-]+" "-")
                    (s/replace #"(^-+|-+$)" ""))]
      (when-not (s/blank? label)
        label))))

(defn shutdown-ha-executor!
  [^ExecutorService executor description context]
  (when executor
    (try
      (.shutdown executor)
      (when-not (.awaitTermination executor 100 TimeUnit/MILLISECONDS)
        (.shutdownNow executor))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (.shutdownNow executor)
        (log/warn e (str "Interrupted while stopping " description) context))
      (catch Throwable e
        (log/warn e (str "Failed to stop " description) context)))))
