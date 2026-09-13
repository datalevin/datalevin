;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.options
  "Storage option validation, persistence, and WAL/HA option normalization."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.constants :as c]
   [datalevin.inline :refer [update assoc]]
   [datalevin.interface
    :refer [transact-kv get-range get-value env-opts kv-info get-env-flags
            set-env-flags]]
   [datalevin.kv :as kv]
   [datalevin.lmdb :as lmdb]
   [datalevin.util :as u :refer [raise]]
   [datalevin.validate :as vld])
  (:import
   [java.util UUID]))

(declare load-opts)

(def async-secondary-index-option-keys
  #{:async-secondary-index-worker-max-jobs
    :async-secondary-index-worker-lease-ms
    :async-secondary-index-retry-base-ms
    :async-secondary-index-retry-max-ms})

(defn apply-option-mutations
  [opts kvs]
  (when-not (map? kvs)
    (raise "Option mutations must be a map" {:value kvs}))
  (reduce-kv
   (fn [m k v]
     (let [k' (c/canonical-wal-option-key k)]
       (vld/validate-option-mutation k' v)
       (-> m
           (dissoc k)
           (assoc k' v))))
   opts
   kvs))

(def ^:private nippy-meta-protocol-key
  :taoensso.nippy/meta-protocol-key)

(def ^:private legacy-ha-nil-sentinel-keys
  [:ha-mode
   :ha-control-plane
   :ha-members
   :ha-fencing-hook
   :ha-clock-skew-hook
   :ha-membership-hash])

(def ^:private non-persistable-ha-option-keys
  [:ha-node-id
   :ha-client-credentials
   :ha-fencing-hook
   :ha-clock-skew-hook])

(def raw-persist-open-opts-key
  :datalevin.storage/raw-persist-open-opts?)

(defn- encode-legacy-ha-nil-sentinels
  [opts]
  (reduce
    (fn [m k]
      (if (and (contains? m k) (nil? (get m k)))
        (assoc m k nippy-meta-protocol-key)
        m))
    (or opts {})
    legacy-ha-nil-sentinel-keys))

(defn- persistable-provider-spec
  [spec]
  (cond-> (or spec {})
    (map? spec) (dissoc :dir :embed-dir :api-key :headers)))

(defn- maybe-persistable-provider-spec
  [spec]
  (when spec
    (persistable-provider-spec spec)))

(defn- compact-persisted-kv-opts
  [opts]
  (let [opts (or opts {})
        kv-opts (c/canonicalize-wal-opts (or (:kv-opts opts) {}))
        compact-kv-opts
        (into {}
              (remove (fn [[k v]]
                        (and (not= k :wal?)
                             (contains? opts k)
                             (= v (get opts k)))))
              kv-opts)]
    (cond-> (dissoc opts :kv-opts)
      (contains? opts :kv-opts)
      (assoc :kv-opts compact-kv-opts))))

(defn- persistable-ha-control-plane-opts
  [cp]
  (cond-> (or cp {})
    (map? cp) (dissoc :local-peer-id :raft-dir)))

(defn- persistable-ha-opts
  [opts]
  (let [opts (apply dissoc (or opts {}) non-persistable-ha-option-keys)]
    (cond-> opts
      (contains? opts :ha-control-plane)
      (update :ha-control-plane persistable-ha-control-plane-opts))))

(defn store-visible-opts
  [opts]
  (-> (persistable-ha-opts opts)
      (dissoc :embedding-providers
              :embedding-domain-providers
              :runtime-opts
              raw-persist-open-opts-key)))

(defn- persistable-opts
  [opts]
  (let [opts (-> opts
                 compact-persisted-kv-opts
                 persistable-ha-opts
                 (dissoc :embedding-providers
                         :embedding-domain-providers
                         :runtime-opts
                         raw-persist-open-opts-key))
        opts (cond-> opts
               (contains? opts :embedding-opts)
               (assoc :embedding-opts
                      (maybe-persistable-provider-spec (:embedding-opts opts)))

               (contains? opts :embedding-domains)
               (assoc :embedding-domains
                      (when-let [domains (:embedding-domains opts)]
                        (into {}
                              (map (fn [[domain cfg]]
                                     [domain (persistable-provider-spec cfg)]))
                              domains))))]
    (cond-> opts
    true c/canonicalize-wal-opts
    true encode-legacy-ha-nil-sentinels)))

(defn transact-opts
  [lmdb opts]
  (let [opts (persistable-opts opts)
        current (some-> (load-opts lmdb) persistable-opts)]
    (when (not= current opts)
      (when (true? (:wal? opts))
        (let [flags (or (get-env-flags lmdb) #{})]
          (when (and (not (contains? flags :nosync))
                     (not (contains? flags :rdonly)))
            (set-env-flags lmdb #{:nosync} true))))
      (transact-kv
        lmdb (conj (for [[k v] opts]
                     (lmdb/kv-tx :put c/opts k v :attr :data))
                   (lmdb/kv-tx :put c/meta :last-modified
                               (System/currentTimeMillis) :attr :long))))))

(defn- raw-lmdb
  [db]
  db)

(defn transact-opts-raw
  [lmdb opts]
  (let [opts (persistable-opts opts)
        current (some-> (load-opts lmdb) persistable-opts)
        raw-db (raw-lmdb lmdb)]
    (when (not= current opts)
      (when (true? (:wal? opts))
        (let [flags (or (get-env-flags raw-db) #{})]
          (when (and (not (contains? flags :nosync))
                     (not (contains? flags :rdonly)))
            (set-env-flags raw-db #{:nosync} true))))
      (kv/transact-kv-without-txlog!
        raw-db
        (conj (for [[k v] opts]
                (lmdb/kv-tx :put c/opts k v :attr :data))
              (lmdb/kv-tx :put c/meta :last-modified
                          (System/currentTimeMillis) :attr :long))))))

(defn- normalize-legacy-ha-nil-sentinels
  [opts]
  (reduce
    (fn [m k]
      (if (= nippy-meta-protocol-key (get m k))
        (assoc m k nil)
        m))
    (or opts {})
    legacy-ha-nil-sentinel-keys))

(defn load-opts
  [lmdb]
  (-> (into {} (get-range lmdb c/opts [:all] :attr :data))
      c/canonicalize-wal-opts
      normalize-legacy-ha-nil-sentinels))

(defn sync-wal-runtime-opts!
  [lmdb opts]
  (let [opts (c/canonicalize-wal-opts opts)]
    (when (true? (:wal? opts))
      (let [runtime-opts (or (env-opts lmdb) {})
            info-v       (kv-info lmdb)
            wal-opts     (into {}
                               (filter (fn [[k _]]
                                         (c/wal-option-key? k)))
                               opts)
            runtime-missing?
            (some (fn [[k v]]
                    (not= v (get runtime-opts k)))
                  wal-opts)
            persisted-missing?
            (some (fn [[k v]]
                    (not= v
                          (get-value lmdb c/kv-info k :keyword :data)))
                  wal-opts)]
        (when (and info-v runtime-missing?)
          (vswap! info-v merge wal-opts))
        (when (and info-v
                   persisted-missing?
                   (not (contains? (or (get-env-flags lmdb) #{}) :rdonly)))
          (kv/transact-kv-without-txlog!
            lmdb
            (mapv (fn [[k v]]
                    (lmdb/kv-tx :put c/kv-info k v :keyword :data))
                  wal-opts)))))))

(defn propagate-top-level-txlog-opts-to-kv-opts
  [opts]
  (let [opts      (or opts {})
        kv-opts?  (contains? opts :kv-opts)
        kv-opts   (c/canonicalize-wal-opts (or (:kv-opts opts) {}))
        txlog-opts (into {}
                         (keep (fn [[k v]]
                                 (let [k' (c/canonical-wal-option-key k)]
                                   (when (and (c/wal-option-key? k)
                                              (not (contains? kv-opts k')))
                                     [k' v]))))
                         opts)]
    (cond-> (c/canonicalize-wal-opts opts)
      (or kv-opts? (seq txlog-opts))
      (assoc :kv-opts (if (seq txlog-opts)
                        (merge kv-opts txlog-opts)
                        kv-opts)))))

(def ^:private ha-wal-durability-profile :strict)

(defn- kv-wal-opts
  [opts]
  (when-let [kv-opts (:kv-opts opts)]
    (into {}
          (filter (fn [[k _]] (c/wal-option-key? k)))
          kv-opts)))

(defn- promote-kv-wal-opts
  [opts]
  (let [wal-opts (kv-wal-opts opts)]
    (cond-> opts
      (seq wal-opts) (merge wal-opts))))

(defn- ha-wal-durability-profile-for
  [opts]
  (let [profile (or (get-in opts [:kv-opts :wal-durability-profile])
                    (:wal-durability-profile opts)
                    ha-wal-durability-profile)]
    (when (= :relaxed profile)
      (raise "Consensus-lease HA requires :wal-durability-profile :strict or :extra"
               {:error :ha/validation
                :option :wal-durability-profile
                :value profile}))
    profile))

(defn- force-ha-wal-opts
  [opts]
  (let [profile (ha-wal-durability-profile-for opts)]
    (-> opts
        (assoc :wal? true
               :wal-durability-profile profile)
        (update :kv-opts
                (fn [kv-opts]
                  (assoc (or kv-opts {})
                         :wal? true
                         :wal-durability-profile profile))))))

(defn normalize-ha-open-opts
  [opts]
  (cond-> opts
    (= :consensus-lease (:ha-mode opts))
    force-ha-wal-opts

    (= :consensus-lease (:ha-mode opts))
    ;; Background sampling performs follower-local metadata writes. In HA mode
    ;; that extra local write traffic obscures replicated progress and can race
    ;; with follower replay. Keep it disabled on consensus-lease stores.
    (assoc :background-sampling? false)))

(defn- txlog-dir-path
  [dir]
  (str dir u/+separator+ "txlog"))

(defn existing-store?
  [dir]
  (or (u/file-exists (str dir u/+separator+ c/data-file-name))
      (u/file-exists (txlog-dir-path dir))))

(defn- default-store-opts
  "Default options for a newly created store."
  []
  {:validate-data?       false
   :auto-entity-time?    false
   :closed-schema?       false
   :background-sampling? c/*db-background-sampling?*
   :async-secondary-index-worker-max-jobs
   c/*async-secondary-index-worker-max-jobs*
   :async-secondary-index-worker-lease-ms
   c/*async-secondary-index-worker-lease-ms*
   :async-secondary-index-retry-base-ms
   c/*async-secondary-index-retry-base-ms*
   :async-secondary-index-retry-max-ms
   c/*async-secondary-index-retry-max-ms*
   :ha-mode c/*ha-mode*
   :ha-lease-renew-ms c/*ha-lease-renew-ms*
   :ha-lease-timeout-ms c/*ha-lease-timeout-ms*
   :ha-promotion-base-delay-ms c/*ha-promotion-base-delay-ms*
   :ha-promotion-rank-delay-ms c/*ha-promotion-rank-delay-ms*
   :ha-max-promotion-lag-lsn c/*ha-max-promotion-lag-lsn*
   :ha-demotion-drain-ms c/*ha-demotion-drain-ms*
   :ha-clock-skew-budget-ms c/*ha-clock-skew-budget-ms*
   :ha-control-plane c/*ha-control-plane*
   :wal?             c/*datalog-wal?*
   :wal-rollout-mode c/*wal-rollout-mode*
   :wal-rollback?    c/*wal-rollback?*
   :wal-durability-profile
   c/*datalog-wal-durability-profile*
   :wal-commit-marker? c/*wal-commit-marker?*
   :wal-commit-marker-version
   c/*wal-commit-marker-version*
   :wal-sync-mode            c/*wal-sync-mode*
   :wal-group-commit         c/*wal-group-commit*
   :wal-group-commit-ms      c/*wal-group-commit-ms*
   :wal-meta-flush-max-txs
   c/*wal-meta-flush-max-txs*
   :wal-meta-flush-max-ms
   c/*wal-meta-flush-max-ms*
   :wal-commit-wait-ms       c/*wal-commit-wait-ms*
   :wal-sync-adaptive?       c/*wal-sync-adaptive?*
   :wal-segment-max-bytes c/*wal-segment-max-bytes*
   :wal-segment-max-ms    c/*wal-segment-max-ms*
   :wal-segment-prealloc?
   c/*wal-segment-prealloc?*
   :wal-segment-prealloc-mode
   c/*wal-segment-prealloc-mode*
   :wal-segment-prealloc-bytes
   c/*wal-segment-prealloc-bytes*
   :wal-retention-bytes c/*wal-retention-bytes*
   :wal-retention-ms    c/*wal-retention-ms*
   :wal-retention-pin-backpressure-threshold-ms
   c/*wal-retention-pin-backpressure-threshold-ms*
   :wal-vec-checkpoint-interval-ms
   c/*wal-vec-checkpoint-interval-ms*
   :wal-vec-max-lsn-delta
   c/*wal-vec-max-lsn-delta*
   :wal-vec-max-buffer-bytes
   c/*wal-vec-max-buffer-bytes*
   :wal-vec-chunk-bytes
   c/*wal-vec-chunk-bytes*
   :db-name              (str (UUID/randomUUID))
   :cache-limit          512})

(defn- debug-open-opts
  [dir opts opts0 opts3]
  (when (= "1" (System/getenv "DTLV_DEBUG_STORAGE_OPEN"))
    (prn :storage-open
         {:dir dir
          :incoming-opts opts
          :persisted-opts (select-keys opts0
                                       [:ha-mode
                                        :db-name
                                        :db-identity
                                        :ha-node-id
                                        :ha-members
                                        :ha-control-plane
                                        :ha-demotion-drain-ms
                                        :ha-fencing-hook
                                        :wal?
                                        :kv-opts])
          :opts3 (select-keys opts3
                              [:ha-mode
                               :db-name
                               :db-identity
                               :ha-node-id
                               :ha-members
                               :ha-control-plane
                               :ha-demotion-drain-ms
                               :ha-fencing-hook
                               :wal?
                               :kv-opts])})))

(defn resolve-store-opts
  "Merge persisted, loaded, incoming, and default store options and validate."
  [dir incoming-opts0 opts persisted-opts loaded-opts]
  (let [opts0      (or persisted-opts loaded-opts {})
        opts1      (if (empty? opts0) (default-store-opts) opts0)
        opts2-base (-> (merge opts1 opts)
                       c/canonicalize-wal-opts
                       normalize-ha-open-opts
                       promote-kv-wal-opts)
        opts2      (-> (if (and (or (some? persisted-opts)
                                    (some? loaded-opts))
                                (empty? (or incoming-opts0 {})))
                         (propagate-top-level-txlog-opts-to-kv-opts
                           opts2-base)
                         opts2-base)
                       normalize-ha-open-opts
                       promote-kv-wal-opts)
        db-identity (or (:db-identity opts2)
                        (:db-name opts2)
                        (str (UUID/randomUUID)))
        opts3       (assoc opts2 :db-identity db-identity)]
    (vld/validate-ha-store-opts opts3)
    (vld/validate-secondary-index-worker-options opts3)
    (vld/validate-search-options opts3)
    (vld/validate-vector-options opts3)
    (vld/validate-embedding-options opts3)
    (vld/validate-idoc-options opts3)
    (debug-open-opts dir opts opts0 opts3)
    opts3))
