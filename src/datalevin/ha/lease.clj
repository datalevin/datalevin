;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.lease
  "Pure consensus-lease helpers shared by HA control-plane adapters."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.ha.util :as hu]
   [datalevin.util :as u])
  (:import
   [java.util.concurrent TimeUnit]))

(def ^:private lease-prefix "/ha/v2/db/")
(def ^:private lease-suffix "/lease")
(def ^:private membership-hash-suffix "/ha/v2/membership-hash")

(defn- non-blank-string?
  [x]
  (and (string? x) (not (s/blank? x))))

(defn- require-non-blank-string
  [x where]
  (when-not (non-blank-string? x)
    (u/raise "HA value must be a non-blank string"
             {:error :ha/validation
              :where where
              :value x}))
  x)

(defn lease-key
  "Build canonical authoritative lease key:
   /<group-id>/ha/v2/db/<db-identity>/lease"
  [group-id db-identity]
  (str "/"
       (require-non-blank-string group-id :group-id)
       lease-prefix
       (require-non-blank-string db-identity :db-identity)
       lease-suffix))

(defn membership-hash-key
  "Build canonical authoritative membership hash key:
   /<group-id>/ha/v2/membership-hash"
  [group-id]
  (str "/"
       (require-non-blank-string group-id :group-id)
       membership-hash-suffix))

(defn validate-lease-key!
  "Validate the canonical lease-key inputs without constructing the key."
  [group-id db-identity]
  (require-non-blank-string group-id :group-id)
  (require-non-blank-string db-identity :db-identity)
  nil)

(defn validate-membership-hash-key!
  "Validate the canonical membership-hash-key inputs without constructing the key."
  [group-id]
  (require-non-blank-string group-id :group-id)
  nil)

(defn observed-term
  "Get observed authoritative term from a lease record, defaulting to 0."
  [observed-lease]
  (long (or (:term observed-lease) 0)))

(defn next-term
  "Compute next term from observed lease state using:
   max(observed-term, 0) + 1."
  [observed-lease]
  (u/long-inc (observed-term observed-lease)))

(defn lease-expired?
  "True when lease is absent or no longer valid at now-ms.
   Lease validity rule: now-ms < lease-until-ms."
  [lease now-ms]
  (let [lease-until-ms (:lease-until-ms lease)]
    (or (nil? lease)
        (nil? lease-until-ms)
        (>= (long now-ms) (long lease-until-ms)))))

(defn bootstrap-empty-lease?
  [lease]
  (let [term (long (or (:term lease) 0))]
    (and (or (nil? lease) (nil? (:leader-node-id lease)))
         (zero? term))))

(defn new-lease-record
  "Create a canonical authoritative lease record."
  [{:keys [db-identity leader-node-id leader-endpoint term lease-renew-ms
           lease-timeout-ms now-ms leader-last-applied-lsn]}]
  {:db-identity db-identity
   :leader-node-id leader-node-id
   :leader-endpoint leader-endpoint
   :term term
   :lease-until-ms (+ (long now-ms) (long lease-timeout-ms))
   :lease-renew-ms lease-renew-ms
   :updated-ms now-ms
   :leader-last-applied-lsn (long (or leader-last-applied-lsn 0))})

(defn ha-lease-local-remaining-ms
  [m now-ms now-nanos]
  (cond
    (integer? (:ha-lease-local-deadline-nanos m))
    (max 0
         (.toMillis TimeUnit/NANOSECONDS
                    (max 0
                         (- (long (:ha-lease-local-deadline-nanos m))
                            (long now-nanos)))))

    (integer? (:ha-lease-local-deadline-ms m))
    (max 0
         (- (long (:ha-lease-local-deadline-ms m))
            (long now-ms)))

    (integer? (:ha-lease-until-ms m))
    (max 0
         (- (long (:ha-lease-until-ms m))
            (long now-ms)))

    :else
    nil))

(defn ha-write-admission-lease-margin-ms
  [m]
  (long (max 0
             (long (or (:ha-write-admission-lease-margin-ms m)
                       c/*ha-write-admission-lease-margin-ms*
                       0)))))

(defn ha-write-admission-lease-margin-nanos
  [m]
  (.toNanos TimeUnit/MILLISECONDS
            (ha-write-admission-lease-margin-ms m)))

(defn ha-clock-skew-budget-ms ^long
  [m]
  (long (max 0
             (long (or (:ha-clock-skew-budget-ms m)
                       c/*ha-clock-skew-budget-ms*
                       0)))))

(defn ha-lease-expired-for-promotion?
  [m lease now-ms]
  (let [lease-until-ms (:lease-until-ms lease)]
    (or (nil? lease)
        (nil? lease-until-ms)
        (let [lease-until-ms (long lease-until-ms)
              skew-budget-ms (ha-clock-skew-budget-ms m)]
          (>= (long now-ms)
              (hu/saturated-long-add lease-until-ms
                                     skew-budget-ms))))))

(defn ha-renew-timeout-ms ^long
  [m now-ms now-nanos]
  (let [lease-timeout-ms   (long (or (:ha-lease-timeout-ms m)
                                     c/*ha-lease-timeout-ms*))
        request-timeout-ms (long (hu/ha-request-timeout-ms m lease-timeout-ms))
        remaining-ms       (ha-lease-local-remaining-ms m now-ms now-nanos)
        timeout-ms         (long (if (integer? remaining-ms)
                                   (let [remaining-ms (long remaining-ms)]
                                     (if (< remaining-ms request-timeout-ms)
                                       remaining-ms
                                       request-timeout-ms))
                                   request-timeout-ms))]
    (if (< timeout-ms 1) 1 timeout-ms)))
