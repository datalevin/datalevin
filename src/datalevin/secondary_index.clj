;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.secondary-index
  "Helpers for durable secondary index work."
  (:require
   [datalevin.constants :as c]
   [datalevin.lmdb :as lmdb]
   [datalevin.util :refer [raise]]))

(def supported-indexing-modes
  #{:sync :async})

(def default-indexing-mode
  :sync)

(def supported-job-types
  #{:fulltext :vector :embedding :idoc})

(def supported-job-ops
  #{:add :delete})

(def pending-status
  :pending)

(def completed-status
  :completed)

(def failed-status
  :failed)

(def running-status
  :running)

(defn normalize-indexing-mode
  [mode]
  (if (nil? mode)
    default-indexing-mode
    (if (supported-indexing-modes mode)
      mode
      (raise "Unsupported secondary indexing mode"
               {:mode mode
                :expected supported-indexing-modes}))))

(defn sync-indexing?
  [opts]
  (= :sync (normalize-indexing-mode (:indexing-mode opts))))

(defn async-indexing?
  [opts]
  (= :async (normalize-indexing-mode (:indexing-mode opts))))

(defn- required-job-value
  [job k]
  (let [v (get job k)]
    (when (nil? v)
      (raise "Secondary index job is missing required value"
               {:key k
                :job job}))
    v))

(defn make-job
  "Build a durable secondary index job record.

  `:tx` and `:ordinal` form a stable job id for the source transaction."
  [{:keys [domain ref value ordinal created-ms updated-ms]
    :as job}]
  (let [type (required-job-value job :type)
        op (required-job-value job :op)
        tx (long (required-job-value job :tx))
        ordinal (long (or ordinal 0))
        created-ms (long (or created-ms (System/currentTimeMillis)))
        updated-ms (long (or updated-ms created-ms))]
    (when-not (supported-job-types type)
      (raise "Unsupported secondary index job type"
               {:type type
                :expected supported-job-types}))
    (when-not (supported-job-ops op)
      (raise "Unsupported secondary index job op"
               {:op op
                :expected supported-job-ops}))
    {:job/id         [type domain tx ordinal]
     :job/type       type
     :job/domain     domain
     :job/op         op
     :job/ref        ref
     :job/value      value
     :job/tx         tx
     :job/ordinal    ordinal
     :job/status     (or (:status job) pending-status)
     :job/attempts   (long (or (:attempts job) 0))
     :job/created-ms created-ms
     :job/updated-ms updated-ms}))

(defn job-tx
  "Return an LMDB put operation that enqueues `job`."
  [job]
  (let [job (if (:job/id job) job (make-job job))]
    (lmdb/kv-tx :put c/secondary-index-jobs (:job/id job) job :data :data)))

(defn pending-job?
  [job]
  (= pending-status (:job/status job)))

(defn failed-job?
  [job]
  (= failed-status (:job/status job)))

(defn running-job?
  [job]
  (= running-status (:job/status job)))

(defn completed-job?
  [job]
  (= completed-status (:job/status job)))

(defn unfinished-job?
  [job]
  (not (completed-job? job)))

(defn completed-job
  ([job]
   (completed-job job (System/currentTimeMillis)))
  ([job now-ms]
   (assoc (dissoc job
                  :job/lease-owner
                  :job/lease-until-ms
                  :job/claimed-ms
                  :job/next-retry-ms)
          :job/status completed-status
          :job/completed-ms (long now-ms)
          :job/updated-ms (long now-ms)
          :job/last-error nil)))

(defn claimed-job
  ([job owner lease-until-ms]
   (claimed-job job owner lease-until-ms (System/currentTimeMillis)))
  ([job owner lease-until-ms now-ms]
   (assoc (dissoc job :job/next-retry-ms)
          :job/status running-status
          :job/lease-owner owner
          :job/lease-until-ms (long lease-until-ms)
          :job/claimed-ms (long now-ms)
          :job/updated-ms (long now-ms))))

(defn failed-job
  ([job error]
   (failed-job job error (System/currentTimeMillis)))
  ([job error now-ms]
   (failed-job job error now-ms nil))
  ([job error now-ms retry-delay-ms]
   (assoc (dissoc job
                  :job/lease-owner
                  :job/lease-until-ms
                  :job/claimed-ms)
          :job/status failed-status
          :job/attempts (inc (long (or (:job/attempts job) 0)))
          :job/updated-ms (long now-ms)
          :job/next-retry-ms (when retry-delay-ms
                               (+ (long now-ms) (long retry-delay-ms)))
          :job/last-error (or (ex-message error) (str error)))))
