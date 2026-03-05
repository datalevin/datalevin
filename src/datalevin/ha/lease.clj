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
   [datalevin.util :as u]))

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
