;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.validate
  "All validation functions for Datalevin.
   Pure checks that raise on invalid input — no data transformation."
  (:require
   [clojure.string :as s]
   [datalevin.interface :as i
    :refer [schema opts populated? visit-list-range]]
   [datalevin.index :as idx]
   [datalevin.datom :as d]
   [datalevin.udf :as udf]
   [datalevin.constants :as c]
   [datalevin.util :as u]
   [datalevin.bits :as b]
   [datalevin.lmdb :as lmdb])
  (:import
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]
   [datalevin.bits Retrieved]
   [datalevin.datom Datom]
   [datalevin.lmdb KVTxData]
   [java.nio.charset StandardCharsets]
   [java.security MessageDigest]))

;; ---- Storage / schema validators ----

(defn validate-closed-schema
  "Validate that attribute is defined in schema when :closed-schema? is true."
  [schema opts attr value]
  (when (and (opts :closed-schema?) (not (schema attr)))
    (u/raise "Attribute is not defined in schema when
`:closed-schema?` is true: " attr {:attr attr :value value})))

(defn validate-cardinality-change
  "Validate cardinality change from many to one."
  [store attr old new]
  (when (and (identical? old :db.cardinality/many)
             (identical? new :db.cardinality/one))
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (u/raise "Cardinality change is not allowed when data exist"
                 {:attribute attr})))))

(defn validate-value-type-change
  "Validate value type change when data exist.
   Allows migration from untyped (:data) to a specific type."
  [store attr old new]
  (when (not= old new)
    (when-let [props ((schema store) attr)]
      (let [old-vt (idx/value-type props)]
        (when-not (identical? old-vt :data)
          (let [low-datom  (d/datom c/e0 attr c/v0)
                high-datom (d/datom c/emax attr c/vmax)]
            (when (populated? store :ave low-datom high-datom)
              (u/raise "Value type change is not allowed when data exist"
                       {:attribute attr}))))))))

(defn violate-unique?
  "Check if adding uniqueness to an attribute would violate existing data."
  [lmdb idx-schema low-datom high-datom]
  (let [prev-v   (volatile! nil)
        violate? (volatile! false)
        visitor  (fn [kv]
                   (let [avg ^Retrieved (b/read-buffer (lmdb/k kv) :avg)
                         v   (idx/retrieved->v lmdb avg)]
                     (if (= @prev-v v)
                       (do (vreset! violate? true)
                           :datalevin/terminate-visit)
                       (vreset! prev-v v))))]
    (visit-list-range
      lmdb c/ave visitor
      [:closed (idx/index->k :ave idx-schema low-datom false)
       (idx/index->k :ave idx-schema high-datom true)] :avg
      [:closed c/e0 c/emax] :id)
    @violate?))

(defn validate-uniqueness-change
  "Validate uniqueness change is consistent with existing data."
  [store lmdb attr old new]
  (when (and (not old) new)
    (let [low-datom  (d/datom c/e0 attr c/v0)
          high-datom (d/datom c/emax attr c/vmax)]
      (when (populated? store :ave low-datom high-datom)
        (when (violate-unique? lmdb (schema store) low-datom high-datom)
          (u/raise "Attribute uniqueness change is inconsistent with data"
                   {:attribute attr}))))))

(def ^:private embedding-schema-keys
  [:db/embedding :db.embedding/domains :db.embedding/autoDomain])

(defn- populated-attribute?
  [store attr]
  (let [low-datom  (d/datom c/e0 attr c/v0)
        high-datom (d/datom c/emax attr c/vmax)]
    (populated? store :ave low-datom high-datom)))

(defn- validate-embedding-schema-change
  [store attr old-props new-props]
  (when-let [k (some (fn [k]
                       (when (not= (get old-props k) (get new-props k))
                         k))
                     embedding-schema-keys)]
    (when (populated-attribute? store attr)
      (u/raise "Embedding schema changes require an explicit rebuild"
               {:attribute attr :key k}))))

(defn validate-schema-mutation
  "Validate schema attribute changes (cardinality, value type, uniqueness)."
  [store lmdb attr old-props new-props]
  (doseq [[k v] new-props
          :let  [v' (old-props k)]]
    (case k
      :db/cardinality (validate-cardinality-change store attr v' v)
      :db/valueType   (validate-value-type-change store attr v' v)
      :db/unique      (validate-uniqueness-change store lmdb attr v' v)
      :pass-through))
  (validate-embedding-schema-change store attr old-props new-props))

(def ^:private boolean-opts
  #{:validate-data? :auto-entity-time? :closed-schema? :background-sampling?
    :wal? :wal-sync-adaptive?
    :wal-segment-prealloc? :wal-commit-marker?
    :wal-rollback?})

(def ^:private non-negative-int-opts
  #{:cache-limit
    :wal-group-commit
    :wal-group-commit-ms
    :wal-meta-flush-max-txs
    :wal-meta-flush-max-ms
    :ha-max-promotion-lag-lsn})

(def ^:private positive-int-opts
  #{:wal-commit-wait-ms
    :wal-replica-floor-ttl-ms
    :wal-retention-pin-backpressure-threshold-ms
    :wal-commit-marker-version
    :wal-segment-max-bytes
    :wal-segment-max-ms
    :wal-segment-prealloc-bytes
    :wal-retention-bytes
    :wal-retention-ms
    :wal-vec-checkpoint-interval-ms
    :wal-vec-max-lsn-delta
    :wal-vec-max-buffer-bytes
    :wal-vec-chunk-bytes
    :ha-node-id
    :ha-lease-renew-ms
    :ha-lease-timeout-ms
    :ha-promotion-base-delay-ms
    :ha-promotion-rank-delay-ms
    :ha-clock-skew-budget-ms})

(def ^:private keyword-enum-opts
  {:wal-durability-profile #{:strict :relaxed :extra}
   :wal-sync-mode          #{:fsync :fdatasync :extra :none}
   :wal-segment-prealloc-mode #{:native :none}
   :wal-rollout-mode       #{:active :rollback}})

(defn- positive-int?
  [x]
  (and (integer? x) (pos? ^long x)))

(defn- non-negative-int?
  [x]
  (and (integer? x) (not (neg? ^long x))))

(defn- non-blank-string?
  [x]
  (and (string? x) (not (s/blank? x))))

(def ^:private peer-id-pattern
  #"^(.+):([0-9]+)$")

(defn- validate-peer-id
  [peer-id where]
  (when-not (non-blank-string? peer-id)
    (u/raise "HA peer-id must be a non-blank host:port string"
             {:error :ha/validation
              :where where
              :peer-id peer-id}))
  (let [[_ host port-str] (re-matches peer-id-pattern peer-id)]
    (when (or (s/blank? host) (nil? port-str))
      (u/raise "HA peer-id must have host:port format"
               {:error :ha/validation
                :where where
                :peer-id peer-id}))
    (let [port (try
                 (Long/parseLong ^String port-str)
                 (catch NumberFormatException _
                   (u/raise "HA peer-id port must be a valid integer"
                            {:error :ha/validation
                             :where where
                             :peer-id peer-id
                             :port port-str})))]
      (when-not (<= 1 port 65535)
        (u/raise "HA peer-id port must be in [1, 65535]"
                 {:error :ha/validation
                  :where where
                  :peer-id peer-id
                  :port port}))))
  true)

(defn- validate-ha-member
  [m idx]
  (when-not (map? m)
    (u/raise "HA member must be a map"
             {:error :ha/validation
              :where :ha-members
              :index idx
              :value m}))
  (let [node-id  (:node-id m)
        endpoint (:endpoint m)]
    (when-not (positive-int? node-id)
      (u/raise "HA member :node-id must be a positive integer"
               {:error :ha/validation
                :where :ha-members
                :index idx
                :member m}))
    (when-not (non-blank-string? endpoint)
      (u/raise "HA member :endpoint must be a non-blank string"
               {:error :ha/validation
                :where :ha-members
                :index idx
                :member m})))
  true)

(defn- validate-ha-members-shape
  [members]
  (when-not (sequential? members)
    (u/raise "Option :ha-members expects a sequential collection"
             {:error :ha/validation
              :option :ha-members
              :value members}))
  (let [members (vec members)]
    (when (empty? members)
      (u/raise "Option :ha-members must be non-empty for consensus HA"
               {:error :ha/validation
                :option :ha-members}))
    (doseq [[idx m] (map-indexed vector members)]
      (validate-ha-member m idx))
    (let [node-ids (mapv :node-id members)
          sorted-ids (vec (sort node-ids))]
      (when-not (= (count node-ids) (count (set node-ids)))
        (u/raise "Option :ha-members contains duplicate :node-id values"
                 {:error :ha/validation
                  :option :ha-members
                  :node-ids node-ids}))
      (when-not (= node-ids sorted-ids)
        (u/raise "Option :ha-members must be ordered by ascending :node-id"
                 {:error :ha/validation
                  :option :ha-members
                  :node-ids node-ids
                  :expected-order sorted-ids})))
    members))

(defn- validate-ha-command-hook-shape
  [option hook-label hook]
  (when-not (map? hook)
    (u/raise (str "Option " option " expects a map")
             {:error :ha/validation
              :option option
              :value hook}))
  (let [cmd (:cmd hook)]
    (when-not (and (vector? cmd) (seq cmd) (every? non-blank-string? cmd))
      (u/raise (str "HA " hook-label
                    " hook :cmd must be a non-empty vector of non-blank strings")
               {:error :ha/validation
                :option option
                :hook hook})))
  (doseq [[k pred msg] [[:timeout-ms positive-int?
                         (str "HA " hook-label
                              " hook :timeout-ms must be a positive integer")]
                        [:retries non-negative-int?
                         (str "HA " hook-label
                              " hook :retries must be a non-negative integer")]
                        [:retry-delay-ms non-negative-int?
                         (str "HA " hook-label
                              " hook :retry-delay-ms must be a non-negative integer")]]]
    (when-not (pred (get hook k))
      (u/raise msg
               {:error :ha/validation
                :option option
                :hook hook})))
  true)

(defn- validate-ha-fencing-hook-shape
  [hook]
  (validate-ha-command-hook-shape
    :ha-fencing-hook "fencing" hook))

(defn- validate-ha-clock-skew-hook-shape
  [hook]
  (validate-ha-command-hook-shape
    :ha-clock-skew-hook "clock skew" hook))

(defn- validate-ha-voter
  [v idx]
  (when-not (map? v)
    (u/raise "HA control-plane voter must be a map"
             {:error :ha/validation
              :where :ha-control-plane
              :index idx
              :value v}))
  (validate-peer-id (:peer-id v) :ha-control-plane-voter)
  (when-not (or (true? (:promotable? v)) (false? (:promotable? v)))
    (u/raise "HA control-plane voter :promotable? must be boolean"
             {:error :ha/validation
              :where :ha-control-plane
              :index idx
              :voter v}))
  (if (:promotable? v)
    (when-not (positive-int? (:ha-node-id v))
      (u/raise "Promotable HA voter must include positive :ha-node-id"
               {:error :ha/validation
                :where :ha-control-plane
                :index idx
                :voter v}))
    (when (contains? v :ha-node-id)
      (u/raise "Non-promotable HA voter must not include :ha-node-id"
               {:error :ha/validation
                :where :ha-control-plane
                :index idx
                :voter v})))
  true)

(defn- validate-ha-control-plane-shape
  [cp]
  (when-not (map? cp)
    (u/raise "Option :ha-control-plane expects a map"
             {:error :ha/validation
              :option :ha-control-plane
              :value cp}))
  (when-not (= :sofa-jraft (:backend cp))
    (u/raise "Option :ha-control-plane :backend must be :sofa-jraft in V2"
             {:error :ha/validation
              :option :ha-control-plane
              :backend (:backend cp)}))
  (when-not (non-blank-string? (:group-id cp))
    (u/raise "Option :ha-control-plane :group-id must be a non-blank string"
             {:error :ha/validation
              :option :ha-control-plane
              :group-id (:group-id cp)}))
  (validate-peer-id (:local-peer-id cp) :ha-control-plane-local-peer)
  (doseq [[k v] [[:rpc-timeout-ms (:rpc-timeout-ms cp)]
                 [:election-timeout-ms (:election-timeout-ms cp)]
                 [:operation-timeout-ms (:operation-timeout-ms cp)]]]
    (when-not (positive-int? v)
      (u/raise "HA control-plane timeout must be a positive integer"
               {:error :ha/validation
                :option :ha-control-plane
                :field k
                :value v})))
  (let [voters (:voters cp)]
    (when-not (vector? voters)
      (u/raise "Option :ha-control-plane :voters must be a vector"
               {:error :ha/validation
                :option :ha-control-plane
                :voters voters}))
    (when (< (count voters) 3)
      (u/raise "Consensus HA requires at least 3 control-plane voters"
               {:error :ha/validation
                :option :ha-control-plane
                :voter-count (count voters)}))
    (doseq [[idx v] (map-indexed vector voters)]
      (validate-ha-voter v idx))
    (let [peer-ids (mapv :peer-id voters)
          local-peer-id (:local-peer-id cp)
          local-count (count (filter #(= local-peer-id %) peer-ids))
          promotable-voters (filter :promotable? voters)
          promotable-node-ids (mapv :ha-node-id promotable-voters)]
      (when-not (= (count peer-ids) (count (set peer-ids)))
        (u/raise "HA control-plane voter :peer-id values must be unique"
                 {:error :ha/validation
                  :option :ha-control-plane
                  :peer-ids peer-ids}))
      (when-not (= local-count 1)
        (u/raise "HA local control-plane peer must appear exactly once in :voters"
                 {:error :ha/validation
                  :option :ha-control-plane
                  :local-peer-id local-peer-id
                  :matches local-count}))
      (when-not (= (count promotable-node-ids)
                   (count (set promotable-node-ids)))
        (u/raise "Promotable HA voters must map one-to-one by :ha-node-id"
                 {:error :ha/validation
                  :option :ha-control-plane
                  :promotable-node-ids promotable-node-ids}))))
  cp)

(defn- canonical-ha-membership-payload
  [opts]
  (let [members (->> (:ha-members opts)
                     (sort-by :node-id)
                     (mapv (fn [{:keys [node-id endpoint]}]
                             (array-map :node-id node-id
                                        :endpoint endpoint))))
        voters  (->> (get-in opts [:ha-control-plane :voters])
                     (sort-by :peer-id)
                     (mapv (fn [{:keys [peer-id promotable? ha-node-id]}]
                             (if promotable?
                               (array-map :peer-id peer-id
                                          :promotable? true
                                          :ha-node-id ha-node-id)
                               (array-map :peer-id peer-id
                                          :promotable? false)))))
        mapping (->> (get-in opts [:ha-control-plane :voters])
                     (filter :promotable?)
                     (sort-by :ha-node-id)
                     (mapv (fn [{:keys [ha-node-id peer-id]}]
                             [ha-node-id peer-id])))]
    (array-map
     :version 2
     :ha-members members
     :control-plane-voters voters
     :promotable-mapping mapping)))

(defn derive-ha-membership-hash
  "Derive deterministic SHA-256 membership hash for consensus HA options."
  [opts]
  (let [payload (pr-str (canonical-ha-membership-payload opts))
        ^MessageDigest md (MessageDigest/getInstance "SHA-256")]
    (u/hexify (.digest md (.getBytes payload StandardCharsets/UTF_8)))))

(defn validate-ha-options
  "Validate consensus-lease HA options as a coherent set.
   Returns `opts` unchanged on success."
  [opts]
      (let [opts (or opts {})
            mode (:ha-mode opts)]
    (when (and (some? mode) (not= mode :consensus-lease))
      (u/raise "Option :ha-mode expects nil or :consensus-lease"
               {:error :ha/validation
                :option :ha-mode
                :value mode}))
    (when (= mode :consensus-lease)
      (let [node-id (:ha-node-id opts)
            members (validate-ha-members-shape (:ha-members opts))
            member-ids (mapv :node-id members)
            member-id-set (set member-ids)
            renew-ms (:ha-lease-renew-ms opts)
            timeout-ms (:ha-lease-timeout-ms opts)
            stale-leader-window-ms
            (when (and (integer? renew-ms) (integer? timeout-ms))
              (- (long timeout-ms) (long renew-ms)))
            base-delay-ms (:ha-promotion-base-delay-ms opts)
            rank-delay-ms (:ha-promotion-rank-delay-ms opts)
            max-lag-lsn (:ha-max-promotion-lag-lsn opts)
            clock-skew-budget-ms
            (long (or (:ha-clock-skew-budget-ms opts)
                      c/*ha-clock-skew-budget-ms*))
            cp (validate-ha-control-plane-shape (:ha-control-plane opts))
            voters (:voters cp)
            local-peer-id (:local-peer-id cp)
            local-voter (first (filter #(= local-peer-id (:peer-id %)) voters))
            promotable-node-id-set (->> voters
                                        (filter :promotable?)
                                        (map :ha-node-id)
                                        set)
            db-identity (:db-identity opts)]
        (when-not (positive-int? node-id)
          (u/raise "Option :ha-node-id must be a positive integer in consensus mode"
                   {:error :ha/validation
                    :option :ha-node-id
                    :value node-id}))
        (when-not (non-blank-string? db-identity)
          (u/raise "Option :db-identity must be a non-blank string in consensus mode"
                   {:error :ha/validation
                    :option :db-identity
                    :value db-identity}))
        (when-not (contains? member-id-set node-id)
          (u/raise "Option :ha-node-id must exist in :ha-members"
                   {:error :ha/validation
                    :option :ha-node-id
                    :ha-node-id node-id
                    :ha-members member-ids}))
        (when-not (and (positive-int? renew-ms)
                       (positive-int? timeout-ms)
                       (positive-int? base-delay-ms)
                       (positive-int? rank-delay-ms))
          (u/raise "Consensus HA timing options must be positive integers"
                   {:error :ha/validation
                    :renew-ms renew-ms
                    :timeout-ms timeout-ms
                    :base-delay-ms base-delay-ms
                    :rank-delay-ms rank-delay-ms}))
        (when-not (non-negative-int? max-lag-lsn)
          (u/raise "Option :ha-max-promotion-lag-lsn must be a non-negative integer"
                   {:error :ha/validation
                    :option :ha-max-promotion-lag-lsn
                    :value max-lag-lsn}))
        (when-not (positive-int? clock-skew-budget-ms)
          (u/raise "Option :ha-clock-skew-budget-ms must be a positive integer"
                   {:error :ha/validation
                    :option :ha-clock-skew-budget-ms
                    :value clock-skew-budget-ms}))
        (when (< (long timeout-ms) (unchecked-multiply 2 (long renew-ms)))
          (u/raise "Option :ha-lease-timeout-ms must be >= 2 * :ha-lease-renew-ms"
                   {:error :ha/validation
                    :ha-lease-timeout-ms timeout-ms
                    :ha-lease-renew-ms renew-ms}))
        (when (or (nil? stale-leader-window-ms)
                  (< stale-leader-window-ms 0)
                  (> (unchecked-multiply 2 (long clock-skew-budget-ms))
                     (long stale-leader-window-ms)))
          (u/raise
           (str "Option :ha-clock-skew-budget-ms is too large for the lease window; "
                "require 2 * :ha-clock-skew-budget-ms <= "
                "(:ha-lease-timeout-ms - :ha-lease-renew-ms)")
           {:error :ha/validation
            :ha-clock-skew-budget-ms clock-skew-budget-ms
            :ha-lease-timeout-ms timeout-ms
            :ha-lease-renew-ms renew-ms
            :stale-leader-window-ms stale-leader-window-ms}))
        (validate-ha-fencing-hook-shape (:ha-fencing-hook opts))
        (when (some? (:ha-clock-skew-hook opts))
          (validate-ha-clock-skew-hook-shape (:ha-clock-skew-hook opts)))
        (when-not (:promotable? local-voter)
          (u/raise "Local control-plane voter must be promotable in consensus mode"
                   {:error :ha/validation
                    :local-peer-id local-peer-id
                    :voter local-voter}))
        (when-not (= node-id (:ha-node-id local-voter))
          (u/raise "Local control-plane voter :ha-node-id must match :ha-node-id"
                   {:error :ha/validation
                    :ha-node-id node-id
                    :local-voter local-voter}))
        (let [missing (seq (sort (remove promotable-node-id-set member-id-set)))
              extras  (seq (sort (remove member-id-set promotable-node-id-set)))]
          (when (or missing extras)
            (u/raise "Promotable control-plane voters must map exactly to :ha-members node IDs"
                     {:error :ha/validation
                      :missing-promotable-voters missing
                      :extra-promotable-voters extras
                      :ha-members member-ids
                      :promotable-voters (sort promotable-node-id-set)})))
        (when-let [expected (:ha-membership-hash opts)]
          (when-not (non-blank-string? expected)
            (u/raise "Option :ha-membership-hash must be a non-blank string when provided"
                     {:error :ha/validation
                      :option :ha-membership-hash
                      :value expected}))
          (let [derived (derive-ha-membership-hash opts)]
            (when-not (= expected derived)
              (u/raise "Local HA membership hash does not match authoritative hash"
                       {:error :ha/validation
                        :option :ha-membership-hash
                        :expected expected
                        :derived derived})))))))
  opts)

(defn validate-option-mutation
  "Validate option key/value before commit."
  [k v]
  (let [k (c/canonical-wal-option-key k)]
    (cond
      (= k :ha-mode)
      (when-not (or (nil? v) (= :consensus-lease v))
        (u/raise "Option :ha-mode expects nil or :consensus-lease"
                 {:option k :value v}))

      (= k :ha-members)
      (when (some? v)
        (validate-ha-members-shape v))

      (= k :ha-control-plane)
      (when (some? v)
        (validate-ha-control-plane-shape v))

      (= k :ha-fencing-hook)
      (when (some? v)
        (validate-ha-fencing-hook-shape v))

      (= k :ha-clock-skew-hook)
      (when (some? v)
        (validate-ha-clock-skew-hook-shape v))

      (= k :ha-membership-hash)
      (when-not (or (nil? v) (non-blank-string? v))
        (u/raise "Option :ha-membership-hash expects nil or a non-blank string"
                 {:option k :value v}))

      (= k :db-identity)
      (when-not (non-blank-string? v)
        (u/raise "Option :db-identity expects a non-blank string"
                 {:option k :value v}))

      (boolean-opts k)
      (when-not (or (true? v) (false? v))
        (u/raise "Option " k " expects a boolean, got " v
                 {:option k :value v}))

      (non-negative-int-opts k)
      (when-not (and (integer? v) (not (neg? ^long v)))
        (u/raise "Option " k " expects a non-negative integer, got " v
                 {:option k :value v}))

      (positive-int-opts k)
      (when-not (and (integer? v) (pos? ^long v))
        (u/raise "Option " k " expects a positive integer, got " v
                 {:option k :value v}))

      (= k :db-name)
      (when-not (string? v)
        (u/raise "Option :db-name expects a string, got " v
                 {:option k :value v}))

      :else
      (when-let [allowed (keyword-enum-opts k)]
        (when-not (allowed v)
          (u/raise "Option " k " expects one of " allowed ", got " v
                   {:option k :value v :allowed allowed}))))))

;; ---- Key size validation ----

(def ^:private size-exempt-key-types
  "Key types that are either fixed-size or manage their own sizing internally.
   Datalog keys use the giant mechanism and never overflow."
  #{:long :id :int :short :byte :int-int :avg :attr :raw
    :float :double :boolean :instant :uuid
    :ints :bitmap :term-info :doc-info :pos-info :instant-pre-06})

(defn validate-key-size
  "Validate that a key does not exceed the LMDB max key size (511 bytes).
   For KV API use — Datalog keys use the giant mechanism and never overflow."
  [key key-type]
  (when (and key (not (size-exempt-key-types key-type)))
    (when (> ^long (b/measure-size key) c/+max-key-size+)
      (u/raise "Key cannot be larger than 511 bytes" {:input key}))))

;; ---- KV validation ----

(def ^:private kv-ops #{:put :del :put-list :del-list})

(defn validate-kv-op
  "Validate that the KV operation is a known operator."
  [op]
  (when-not (kv-ops op)
    (u/raise "Unknown kv transact operator: " op {})))

(defn validate-kv-key
  "Validate a KV key: must not be nil; optionally check data type."
  [k kt validate-data?]
  (when (nil? k)
    (u/raise "Key cannot be nil" {}))
  (when validate-data?
    (when-not (b/valid-data? k kt)
      (u/raise "Invalid data, expecting " kt " got " k {:input k}))))

(defn validate-kv-value
  "Validate a KV value: must not be nil; optionally check data type."
  [v vt validate-data?]
  (when (nil? v)
    (u/raise "Value cannot be nil" {}))
  (when validate-data?
    (when-not (b/valid-data? v vt)
      (u/raise "Invalid data, expecting " vt " got " v {:input v}))))

(defn validate-kv-tx-data
  "Validate a single KVTxData: op shape, key, value, and key size."
  [^KVTxData tx validate-data?]
  (let [op (.-op tx)
        k  (.-k tx)
        kt (.-kt tx)
        v  (.-v tx)
        vt (.-vt tx)]
    (validate-kv-op op)
    (validate-kv-key k kt validate-data?)
    (validate-key-size k kt)
    (case op
      :put      (validate-kv-value v vt validate-data?)
      :put-list (do (when-not (or (sequential? v) (instance? java.util.List v))
                      (u/raise "List value must be a sequential collection, got "
                               (if (nil? v) "nil" (type v)) {:input v}))
                    (doseq [vi v]
                      (validate-kv-value vi vt validate-data?)))
      :del-list (do (when-not (or (sequential? v) (instance? java.util.List v))
                      (u/raise "List value must be a sequential collection, got "
                               (if (nil? v) "nil" (type v)) {:input v}))
                    (when validate-data?
                      (doseq [vi v]
                        (when-not (b/valid-data? vi vt)
                          (u/raise "Invalid data, expecting " vt " got " vi
                                   {:input vi})))))
      :del      nil)))

;; ---- DB validators ----

(defn validate-schema-key
  "Validate a single schema key-value pair against expected values."
  [a k v expected]
  (when-not (or (nil? v) (contains? expected v))
    (u/raise "Bad attribute specification for " {a {k v}}
             ", expected one of " expected
             {:error     :schema/validation
              :attribute a
              :key       k
              :value     v})))

(def tuple-props #{:db/tupleAttrs :db/tupleTypes :db/tupleType})

(def ^:private embedding-metric-types
  #{:cosine :dot-product :euclidean :haversine :divergence :pearson
    :jaccard :hamming :tanimoto :sorensen :custom})

(defn- validate-embedding-domain-list
  [a domains]
  (let [ex-data {:error     :schema/validation
                 :attribute a
                 :key       :db.embedding/domains}]
    (when-not (sequential? domains)
      (u/raise a " :db.embedding/domains must be a sequential collection"
               ex-data))
    (when (empty? domains)
      (u/raise a " :db.embedding/domains cannot be empty" ex-data))
    (doseq [domain domains]
      (when-not (and (string? domain) (not (s/blank? domain)))
        (u/raise a " :db.embedding/domains entries must be non-blank strings"
                 (assoc ex-data :value domain))))))

(defn- validate-embedding-domain-config*
  [where domain config]
  (when-not (map? config)
    (u/raise "Embedding domain config must be a map"
             {:error :store/validation
              :where where
              :domain domain
              :value config}))
  (when-let [provider (:provider config)]
    (when-not (keyword? provider)
      (u/raise "Embedding provider id must be a keyword"
               {:error :store/validation
                :where where
                :domain domain
                :provider provider})))
  (when-let [dimensions (:dimensions config)]
    (when-not (positive-int? dimensions)
      (u/raise "Embedding dimensions must be a positive integer"
               {:error :store/validation
                :where where
                :domain domain
                :dimensions dimensions})))
  (when-let [metric-type (:metric-type config)]
    (when-not (embedding-metric-types metric-type)
      (u/raise "Embedding metric type is not supported"
               {:error :store/validation
                :where where
                :domain domain
                :metric-type metric-type})))
  (when-let [metadata (:embedding-metadata config)]
    (when-not (map? metadata)
      (u/raise "Embedding metadata must be a map"
               {:error :store/validation
                :where where
                :domain domain
                :metadata metadata}))))

(defn validate-embedding-options
  [opts]
  (when-let [embedding-opts (:embedding-opts opts)]
    (validate-embedding-domain-config* :embedding-opts nil embedding-opts))
  (when-let [embedding-domains (:embedding-domains opts)]
    (when-not (map? embedding-domains)
      (u/raise "Option :embedding-domains expects a map"
               {:error :store/validation
                :option :embedding-domains
                :value embedding-domains}))
    (doseq [[domain config] embedding-domains]
      (when-not (and (string? domain) (not (s/blank? domain)))
        (u/raise "Embedding domain names must be non-blank strings"
                 {:error :store/validation
                  :option :embedding-domains
                  :domain domain}))
      (validate-embedding-domain-config* :embedding-domains domain config)))
  (when-let [embedding-providers (:embedding-providers opts)]
    (when-not (map? embedding-providers)
      (u/raise "Option :embedding-providers expects a map"
               {:error :store/validation
                :option :embedding-providers
                :value embedding-providers})))
  opts)

(defn validate-schema
  "Validate full schema structure."
  [schema]
  (doseq [[a kv] schema]
    (let [comp? (:db/isComponent kv false)]
      (validate-schema-key a :db/isComponent (:db/isComponent kv) #{true false})
      (when (and comp? (not (identical? (:db/valueType kv) :db.type/ref)))
        (u/raise
          "Bad attribute specification for " a
          ": {:db/isComponent true} should also have {:db/valueType :db.type/ref}"
          {:error     :schema/validation
           :attribute a
           :key       :db/isComponent})))
    (validate-schema-key a :db/unique (:db/unique kv)
                         #{:db.unique/value :db.unique/identity})
    (validate-schema-key a :db/valueType (:db/valueType kv)
                         c/datalog-value-types)
    (validate-schema-key a :db/cardinality (:db/cardinality kv)
                         #{:db.cardinality/one :db.cardinality/many})
    (validate-schema-key a :db/fulltext (:db/fulltext kv)
                         #{true false})
    (validate-schema-key a :db/embedding (:db/embedding kv)
                         #{true false})
    (validate-schema-key a :db.embedding/autoDomain
                         (:db.embedding/autoDomain kv)
                         #{true false})

    (when (contains? kv :db.embedding/domains)
      (validate-embedding-domain-list a (:db.embedding/domains kv)))

    (when (and (:db/embedding kv)
               (not (identical? (:db/valueType kv) :db.type/string)))
      (u/raise "Bad attribute specification for embedding"
               {:error     :schema/validation
                :attribute a
                :key       :db/embedding
                :valueType (:db/valueType kv)}))

    ;; tuple should have one of tuple-props
    (when (and (identical? :db.type/tuple (:db/valueType kv))
               (not (some tuple-props (keys kv))))
      (u/raise
        "Bad attribute specification for " a
        ": {:db/valueType :db.type/tuple} should also have :db/tupleAttrs, :db/tupleTypes, or :db/tupleType"
        {:error     :schema/validation
         :attribute a
         :key       :db/valueType}))

    ;; :db/tupleAttrs is a non-empty sequential coll
    (when (contains? kv :db/tupleAttrs)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleAttrs}]
        (when (identical? :db.cardinality/many (:db/cardinality kv))
          (u/raise a " has :db/tupleAttrs, must be :db.cardinality/one" ex-data))

        (let [attrs (:db/tupleAttrs kv)]
          (when-not (sequential? attrs)
            (u/raise a " :db/tupleAttrs must be a sequential collection, got: " attrs ex-data))

          (when (empty? attrs)
            (u/raise a " :db/tupleAttrs can\u2019t be empty" ex-data))

          (doseq [attr attrs
                  :let [ex-data (assoc ex-data :value attr)]]
            (when (contains? (schema attr) :db/tupleAttrs)
              (u/raise a " :db/tupleAttrs can\u2019t depend on another tuple attribute: " attr ex-data))

            (when (identical? :db.cardinality/many (:db/cardinality (schema attr)))
              (u/raise a " :db/tupleAttrs can\u2019t depend on :db.cardinality/many attribute: " attr ex-data))))))

    (when (contains? kv :db/tupleType)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleType}
            attr    (:db/tupleType kv)]
        (when-not (c/datalog-value-types attr)
          (u/raise a " :db/tupleType must be a single value type, got: " attr ex-data))
        (when (identical? attr :db.type/tuple)
          (u/raise a " :db/tupleType cannot be :db.type/tuple" ex-data))))

    (when (contains? kv :db/tupleTypes)
      (let [ex-data {:error     :schema/validation
                     :attribute a
                     :key       :db/tupleTypes}
            attrs   (:db/tupleTypes kv)]
        (when-not (and (sequential? attrs) (< 1 (count attrs))
                       (every? c/datalog-value-types attrs)
                       (not (some #(identical? :db.type/tuple %) attrs)))
          (u/raise a " :db/tupleTypes must be a sequential collection of more than one value types, got: " attrs ex-data))))))

(defn validate-attr
  "Validate that an attribute is a keyword."
  [attr at]
  (when-not (keyword? attr)
    (u/raise "Bad entity attribute " attr " at " at ", expected keyword"
             {:error :transact/syntax, :attribute attr, :context at})))

(defn validate-val
  "Validate that a value is not nil."
  [v at]
  (when (nil? v)
    (u/raise "Cannot store nil as a value at " at
             {:error :transact/syntax, :value v, :context at})))

(defn validate-datom-unique
  "Validate unique constraint on a datom. Called from db.clj's validate-datom
   with pre-resolved unique? and found? predicates to avoid circular dependency."
  [unique? ^Datom datom found?]
  (let [a (.-a datom)
        v (.-v datom)]
    (when (and unique? (d/datom-added datom))
      (when-some [found (found?)]
        (u/raise "Cannot add " datom " because of unique constraint: " found
                 {:error     :transact/unique
                  :attribute a
                  :datom     datom})))
    v))

(defn validate-upserts
  "Throws if not all upserts point to the same entity.
   Returns single eid that all upserts point to, or null.
   tempid-fn? is a predicate that checks if a value is a tempid."
  [entity upserts tempid-fn?]
  (let [upsert-ids (reduce-kv
                     (fn [m a v->e]
                       (reduce-kv
                         (fn [m v e]
                           (assoc m e [a v]))
                         m v->e))
                     {} upserts)]
    (if (<= 2 (count upsert-ids))
      (let [[e1 [a1 v1]] (first upsert-ids)
            [e2 [a2 v2]] (second upsert-ids)]
        (u/raise "Conflicting upserts: " [a1 v1] " resolves to " e1 ", but " [a2 v2] " resolves to " e2
               {:error     :transact/upsert
                :assertion [e1 a1 v1]
                :conflict  [e2 a2 v2]}))
      (let [[upsert-id [a v]] (first upsert-ids)
            eid               (:db/id entity)]
        (when (and
                (some? upsert-id)
                (some? eid)
                (not (tempid-fn? eid))
                (not= upsert-id eid))
          (u/raise "Conflicting upsert: " [a v] " resolves to " upsert-id ", but entity already has :db/id " eid
                 {:error     :transact/upsert
                  :assertion [upsert-id a v]
                  :conflict  {:db/id eid}}))
        upsert-id))))

;; ---- Type validation ----

(defn validate-type
  "Validate that data matches declared value type when :validate-data? is set.
   Returns the value type."
  [store a v]
  (let [st-opts   (opts store)
        st-schema (schema store)
        vt        (idx/value-type (st-schema a))]
    (or (not (st-opts :validate-data?))
        (b/valid-data? v vt)
        (u/raise "Invalid data, expecting" vt " got " v {:input v}))
    vt))

;; ---- Transaction form validators ----

(defn validate-tempid-op
  "Validate that tempids are only used with :db/add."
  [tempid? op entity]
  (when (and tempid? (not (identical? op :db/add)))
    (u/raise "Can't use tempid in '" entity "'. Tempids are allowed in :db/add only"
             {:error :transact/syntax, :op entity})))

(defn validate-cas-value
  "Validate CAS compare-and-swap: existing value must match expected old value.
   For multival attrs, checks if ov is among datom values.
   For single-val attrs, checks if the single datom value equals ov."
  [multival? e a ov nv datoms]
  (if multival?
    (when-not (some (fn [^Datom d] (= (.-v d) ov)) datoms)
      (u/raise ":db.fn/cas failed on datom [" e " " a " " (map :v datoms) "], expected " ov
               {:error :transact/cas, :old datoms, :expected ov, :new nv}))
    (let [v (:v (first datoms))]
      (when-not (= v ov)
        (u/raise ":db.fn/cas failed on datom [" e " " a " " v "], expected " ov
                 {:error :transact/cas, :old (first datoms), :expected ov, :new nv})))))

(defn validate-patch-idoc-arity
  "Validate patchIdoc arity: expected 4 or 5 items."
  [argc entity op]
  (when-not (or (= argc 4) (= argc 5))
    (u/raise "Bad arity for :db.fn/patchIdoc, expected 4 or 5 items: "
             entity
             {:error :transact/syntax, :operation op, :tx-data entity})))

(defn validate-patch-idoc-type
  "Validate that the attribute has idoc value type."
  [value-type a]
  (when-not (identical? value-type :db.type/idoc)
    (u/raise "Attribute is not an idoc type: " a
             {:attribute a})))

(defn validate-patch-idoc-cardinality
  "Validate patchIdoc cardinality rules for old value."
  [many? old-v a]
  (when (and many? (nil? old-v))
    (u/raise "Idoc patch requires old value for cardinality many attribute: "
             a {:attribute a}))
  (when (and (not many?) (some? old-v))
    (u/raise "Idoc patch old value is only supported for cardinality many attribute: "
             a {:attribute a})))

(defn validate-patch-idoc-old-value
  "Validate that old value exists for cardinality-many idoc patch."
  [old-datom old-v a]
  (when-not old-datom
    (u/raise "Idoc patch old value not found: " old-v
             {:attribute a :value old-v})))

(defn validate-custom-tx-fn-value
  "Validate that a resolved entity has a fn? :db/fn attribute."
  [fun op entity]
  (when-not (fn? fun)
    (u/raise "Entity " op " expected to have :db/fn attribute with fn? value"
             {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))

(defn validate-installed-callable-entity
  "Validate stored function attributes on an entity map."
  [entity]
  (when (and (contains? entity :db/fn) (contains? entity :db/udf))
    (u/raise "Entity cannot have both :db/fn and :db/udf at " entity
             {:error :transact/syntax, :tx-data entity}))
  (when (contains? entity :db/udf)
    (let [descriptor (udf/descriptor (:db/udf entity))]
      (when-let [ident (:db/ident entity)]
        (when-not (= ident (:udf/id descriptor))
          (u/raise "Installed :db/udf id must match :db/ident at " entity
                   {:error      :transact/syntax
                    :tx-data    entity
                    :db/ident   ident
                    :descriptor descriptor})))
      descriptor)))

(defn validate-installed-udf-ident
  "Validate that an installed descriptor matches the entity ident when known."
  [ident descriptor at]
  (when (and ident (not= ident (:udf/id descriptor)))
    (u/raise "Installed :db/udf id must match :db/ident at " at
             {:error      :transact/syntax
              :db/ident   ident
              :descriptor descriptor
              :context    at})))

(defn validate-custom-tx-fn-entity
  "Validate that an entity exists for a custom transaction function."
  [ident op entity]
  (when-not ident
    (u/raise "Can\u2019t find entity for transaction fn " op
             {:error :transact/syntax, :operation :db.fn/call, :tx-data entity})))

(defn validate-tuple-direct-write
  "Validate that tuple attrs cannot be modified directly."
  [match? entity]
  (when-not match?
    (u/raise "Can\u2019t modify tuple attrs directly: " entity
             {:error :transact/syntax, :tx-data entity})))

(defn validate-tx-op
  "Validate that the operation is a known transaction operation."
  [op entity]
  (u/raise "Unknown operation at " entity ", expected :db/add, :db/retract, :db.fn/call, :db.fn/retractAttribute, :db.fn/retractEntity or an ident corresponding to an installed transaction function (e.g. {:db/ident <keyword> :db/fn <Ifn>} or {:db/ident <keyword> :db/udf <descriptor>}, usage of :db/ident requires {:db/unique :db.unique/identity} in schema)"
           {:error :transact/syntax, :operation op, :tx-data entity}))

(defn validate-tx-entity-type
  "Validate that the entity is a valid type (map, vector, or datom)."
  [entity]
  (u/raise "Bad entity type at " entity ", expected map, vector, or datom"
           {:error :transact/syntax, :tx-data entity}))

;; ---- Entity-id / lookup-ref validators ----

(defn validate-lookup-ref-shape
  "Validate that a lookup ref contains exactly 2 elements."
  [eid]
  (when (not= (count eid) 2)
    (u/raise "Lookup ref should contain 2 elements: " eid
             {:error :lookup-ref/syntax, :entity-id eid})))

(defn validate-lookup-ref-unique
  "Validate that a lookup ref attribute is marked as :db/unique."
  [unique? eid]
  (when-not unique?
    (u/raise "Lookup ref attribute should be marked as :db/unique: " eid
             {:error :lookup-ref/unique, :entity-id eid})))

(defn validate-entity-id-syntax
  "Validate entity id syntax: must be a number or lookup ref."
  [eid]
  (u/raise "Expected number or lookup ref for entity id, got " eid
           {:error :entity-id/syntax, :entity-id eid}))

(defn validate-map-entity-id-syntax
  "Validate :db/id in a map entity: must be a number, string, or lookup ref."
  [eid]
  (u/raise "Expected number, string or lookup ref for :db/id, got " eid
           {:error :entity-id/syntax, :entity-id eid}))

(defn validate-entity-id-exists
  "Validate that an entity id resolves to an existing entity."
  [result eid]
  (when-not result
    (u/raise "Nothing found for entity id " eid
             {:error     :entity-id/missing
              :entity-id eid})))

(defn validate-reverse-ref-attr
  "Validate that a reverse-ref attribute is a keyword."
  [attr]
  (when-not (keyword? attr)
    (u/raise "Bad entity attribute: " attr ", expected keyword"
             {:error :transact/syntax, :attribute attr})))

(defn validate-reverse-ref-type
  "Validate that a reverse attribute has :db/valueType :db.type/ref in schema."
  [ref? a eid vs]
  (when-not ref?
    (u/raise "Bad attribute " a ": reverse attribute name requires {:db/valueType :db.type/ref} in schema"
             {:error   :transact/syntax, :attribute a,
              :context {:db/id eid, a vs}})))

;; ---- Finalize-phase consistency validators ----

(defn validate-value-tempids
  "Validate that all tempids used as ref values were also used as entity ids.
   unused is a collection of tempid values that were never added as entities."
  [unused]
  (when (seq unused)
    (u/raise "Tempids used only as value in transaction: " (sort unused)
             {:error :transact/syntax, :tempids unused})))

(defn validate-upsert-retry-conflict
  "Validate that a tempid does not conflict during upsert retry.
   Raised when a tempid resolves to two different eids across retries."
  [eid tempid upserted-eid]
  (when eid
    (u/raise "Conflicting upsert: " tempid " resolves"
             " both to " upserted-eid " and " eid
             {:error :transact/upsert})))

(defn validate-upsert-conflict
  "Validate that an upserted eid does not conflict with an existing resolution.
   Raised when no unprocessed tempid is available to retry."
  [tempid e upserted-eid entity]
  (when-not tempid
    (u/raise "Conflicting upsert: " e " resolves to " upserted-eid
             " via " entity {:error :transact/upsert})))

(defn validate-tx-data-shape
  "Validate that tx-data is nil or a sequential collection."
  [tx-data]
  (when-not (or (nil? tx-data)
                (sequential? tx-data))
    (u/raise "Bad transaction data " tx-data ", expected sequential collection"
             {:error :transact/syntax, :tx-data tx-data})))

(defn validate-attr-deletable
  "Validate that an attribute can be deleted (no datoms exist for it)."
  [populated?]
  (when populated?
    (u/raise "Cannot delete attribute with datoms" {})))

(defn validate-datom-list
  "Validate that every element in a collection is a Datom."
  [datoms]
  (when-some [not-datom (first (drop-while d/datom? datoms))]
    (u/raise "init-db expects list of Datoms, got " (type not-datom)
             {:error :init-db})))
