;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.storage.jobs
  "Secondary-index job status aggregation and lease eligibility checks."
  (:refer-clojure :exclude [update assoc])
  (:require
   [datalevin.inline :refer [update assoc]]
   [datalevin.secondary-index :as si]))

(defn- max-long-value
  [a b]
  (if (some? a)
    (max (long a) (long b))
    (long b)))

(defn- min-long-value
  [a b]
  (if (some? a)
    (min (long a) (long b))
    (long b)))

(defn- latest-updated-job
  [a b]
  (if (or (nil? a)
          (< (long (or (:job/updated-ms a) 0))
             (long (or (:job/updated-ms b) 0))))
    b
    a))

(defn- maybe-update-stat
  [m k f v]
  (if (some? v)
    (update m k f v)
    m))

(defn secondary-index-status-init
  []
  {:total-count 0
   :pending-count 0
   :running-count 0
   :completed-count 0
   :failed-count 0})

(defn add-job-to-secondary-index-status
  [status job]
  (let [status (update status :total-count (fnil inc 0))
        tx (:job/tx job)
        status (maybe-update-stat status :last-enqueued-tx max-long-value tx)]
    (case (:job/status job)
      :pending
      (-> status
          (update :pending-count (fnil inc 0))
          (maybe-update-stat :oldest-pending-ms
                             min-long-value
                             (:job/created-ms job)))

      :completed
      (-> status
          (update :completed-count (fnil inc 0))
          (maybe-update-stat :last-completed-tx max-long-value tx))

      :running
      (-> status
          (update :running-count (fnil inc 0))
          (maybe-update-stat :oldest-running-ms
                             min-long-value
                             (:job/claimed-ms job))
          (maybe-update-stat :next-lease-ms
                             min-long-value
                             (:job/lease-until-ms job)))

      :failed
      (-> status
          (update :failed-count (fnil inc 0))
          (maybe-update-stat :last-failed-tx max-long-value tx)
          (maybe-update-stat :next-retry-ms
                             min-long-value
                             (:job/next-retry-ms job))
          (update :latest-failed-job latest-updated-job job))

      status)))

(defn finalize-secondary-index-status
  [now-ms status]
  (let [failed-job (:latest-failed-job status)
        oldest-ms (:oldest-pending-ms status)
        oldest-running-ms (:oldest-running-ms status)]
    (cond-> (dissoc status :latest-failed-job)
      failed-job
      (assoc :last-error (:job/last-error failed-job))

      oldest-ms
      (assoc :oldest-pending-age-ms
             (max 0 (- (long now-ms) (long oldest-ms))))

      oldest-running-ms
      (assoc :oldest-running-age-ms
             (max 0 (- (long now-ms) (long oldest-running-ms)))))))

(defn embedding-job-item
  [job]
  {:text (:job/value job)
   :ref (:job/ref job)
   :kind :document
   :domain (:job/domain job)})

(defn- due-failed-secondary-index-job?
  [now-ms job]
  (and (si/failed-job? job)
       (<= (long (or (:job/next-retry-ms job) 0))
           (long now-ms))))

(defn- expired-secondary-index-job-lease?
  [now-ms job]
  (and (si/running-job? job)
       (<= (long (or (:job/lease-until-ms job) 0))
           (long now-ms))))

(defn- previously-failed-secondary-index-job?
  [job]
  (pos? (long (or (:job/attempts job) 0))))

(defn claimable-secondary-index-job?
  [now-ms retry-failed? retry-due-only? reclaim-failed-running? job]
  (or (si/pending-job? job)
      (expired-secondary-index-job-lease? now-ms job)
      (and retry-failed?
           reclaim-failed-running?
           (si/running-job? job)
           (previously-failed-secondary-index-job? job))
      (and retry-failed?
           (si/failed-job? job)
           (or (not retry-due-only?)
               (due-failed-secondary-index-job? now-ms job)))))

(defn secondary-index-job-matches?
  [{:keys [tx type domain]} job]
  (and (or (nil? tx)
           (<= (long (:job/tx job)) (long tx)))
       (or (nil? type)
           (= type (:job/type job)))
       (or (nil? domain)
           (= domain (:job/domain job)))))

(defn claimed-secondary-index-job?
  [job owner]
  (and (si/running-job? job)
       (= owner (:job/lease-owner job))))
