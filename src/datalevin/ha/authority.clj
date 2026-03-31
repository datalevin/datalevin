;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.authority
  "Consensus-lease authority observation helpers."
  (:require
   [datalevin.constants :as c]
   [datalevin.ha.control :as ctrl]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent TimeUnit]))

(defn authority-observation-from-state
  [m]
  {:lease                    (:ha-authority-lease m)
   :version                  (:ha-authority-version m)
   :authority-now-ms         (:ha-authority-now-ms m)
   :lease-local-deadline-ms  (:ha-lease-local-deadline-ms m)
   :lease-local-deadline-nanos (:ha-lease-local-deadline-nanos m)
   :authority-membership-hash (:ha-authority-membership-hash m)
   :db-identity-mismatch?    (true? (:ha-db-identity-mismatch? m))
   :membership-mismatch?     (true? (:ha-membership-mismatch? m))
   :observed-at-ms           (:ha-last-authority-refresh-ms m)})

(defn authority-lease-local-deadline-ms
  [lease authority-now-ms local-start-ms]
  (when (and (integer? authority-now-ms)
             (integer? local-start-ms))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-ms)
           (max 0
                (- (long lease-until-ms)
                   (long authority-now-ms))))))))

(defn authority-lease-local-deadline-nanos
  [lease authority-now-ms local-start-nanos]
  (when (and (integer? authority-now-ms)
             (integer? local-start-nanos))
    (let [lease-until-ms (:lease-until-ms lease)]
      (when (integer? lease-until-ms)
        (+ (long local-start-nanos)
           (.toNanos TimeUnit/MILLISECONDS
                     (max 0
                          (- (long lease-until-ms)
                             (long authority-now-ms)))))))))

(defn control-result-authority-observation?
  [result]
  (and (map? result)
       (contains? result :version)
       (contains? result :authority-now-ms)))

(defn control-result-authority-observation
  ([m local-start-ms local-start-nanos result]
   (control-result-authority-observation
    #(System/currentTimeMillis)
    m
    local-start-ms
    local-start-nanos
    result))
  ([ha-now-ms-fn m local-start-ms local-start-nanos
    {:keys [lease version authority-now-ms observed-at-ms] :as result}]
   (let [authority-membership-hash
         (if (contains? result :membership-hash)
           (:membership-hash result)
           (:ha-authority-membership-hash m))
         db-identity (:ha-db-identity m)
         db-identity-mismatch?
         (and lease (not= db-identity (:db-identity lease)))
         membership-mismatch?
         (and authority-membership-hash
              (not= authority-membership-hash (:ha-membership-hash m)))
         observed-at-ms (or observed-at-ms (ha-now-ms-fn))]
     {:lease lease
      :version version
      :authority-now-ms authority-now-ms
      :lease-local-deadline-ms
      (authority-lease-local-deadline-ms
       lease authority-now-ms local-start-ms)
      :lease-local-deadline-nanos
      (authority-lease-local-deadline-nanos
       lease authority-now-ms local-start-nanos)
      :authority-membership-hash authority-membership-hash
      :db-identity-mismatch? db-identity-mismatch?
      :membership-mismatch? membership-mismatch?
      :observed-at-ms observed-at-ms})))

(defn observe-authority-state
  ([m]
   (observe-authority-state m nil))
  ([m timeout-ms]
   (let [authority (:ha-authority m)
         db-identity (:ha-db-identity m)
         local-start-ms (System/currentTimeMillis)
         local-start-nanos (System/nanoTime)
         result (ctrl/read-state authority db-identity timeout-ms)]
     (control-result-authority-observation
      m
      local-start-ms
      local-start-nanos
      result))))

(defn observe-authority-state-compat
  [observe-authority-state-fn m timeout-ms]
  (let [observe-fn (or observe-authority-state-fn observe-authority-state)]
    (if (some? timeout-ms)
      (try
        (observe-fn m timeout-ms)
        (catch clojure.lang.ArityException e
          (if (re-find #"Wrong number of args" (or (ex-message e) ""))
            (observe-fn m)
            (throw e))))
      (observe-fn m))))

(defn apply-authority-observation
  [m {:keys [lease version authority-now-ms
             lease-local-deadline-ms lease-local-deadline-nanos
             authority-membership-hash
             db-identity-mismatch? membership-mismatch?
             observed-at-ms]}
   now-ms]
  (let [refresh-ms (or observed-at-ms now-ms)
        observed-term (:term lease)
        observed-owner-node-id (:leader-node-id lease)]
    (cond-> (assoc m
                   :ha-authority-lease lease
                   :ha-authority-version version
                   :ha-authority-now-ms authority-now-ms
                   :ha-lease-local-deadline-ms lease-local-deadline-ms
                   :ha-lease-local-deadline-nanos lease-local-deadline-nanos
                   :ha-authority-owner-node-id observed-owner-node-id
                   :ha-authority-term observed-term
                   :ha-lease-until-ms (:lease-until-ms lease)
                   :ha-authority-membership-hash authority-membership-hash
                   :ha-db-identity-mismatch? db-identity-mismatch?
                   :ha-membership-mismatch? membership-mismatch?
                   :ha-last-authority-refresh-ms refresh-ms)
      (and (= :leader (:ha-role m))
           (= (:ha-node-id m) observed-owner-node-id)
           (integer? observed-term)
           (or (not (integer? (:ha-leader-term m)))
               (<= (long (:ha-leader-term m))
                   (long observed-term))))
      (assoc :ha-leader-term (long observed-term)))))

(defn authority-read-error
  [e]
  {:reason :authority-read-failed
   :message (ex-message e)
   :data (ex-data e)})

(defn apply-authority-read-failure
  [m error]
  (assoc m
         :ha-authority-read-ok? false
         :ha-authority-read-error error))

(defn apply-authority-read-success
  [m]
  (assoc m
         :ha-authority-read-ok? true
         :ha-authority-read-error nil))

(defn ha-authority-read-fresh?
  [m now-ms]
  (let [ok? (true? (:ha-authority-read-ok? m))
        last-ms (:ha-last-authority-refresh-ms m)
        timeout-ms (long (or (:ha-lease-timeout-ms m)
                             c/*ha-lease-timeout-ms*))]
    (and ok?
         (or (not (integer? last-ms))
             (< (- (long now-ms) (long last-ms))
                timeout-ms)))))

(defn current-authority-observation
  [m now-ms]
  (when (ha-authority-read-fresh? m now-ms)
    (authority-observation-from-state m)))

(defn promotion-authority-observation
  [observe-authority-state-fn m now-ms]
  (or (current-authority-observation m now-ms)
      (observe-authority-state-compat observe-authority-state-fn m nil)))

(defn ha-authority-read-failure-details
  ([m]
   (ha-authority-read-failure-details m (System/currentTimeMillis)))
  ([m now-ms]
   (or (when (and (true? (:ha-authority-read-ok? m))
                  (integer? (:ha-last-authority-refresh-ms m))
                  (not (ha-authority-read-fresh? m now-ms)))
         {:reason :authority-read-stale
          :last-authority-refresh-ms (:ha-last-authority-refresh-ms m)
          :timeout-ms (long (or (:ha-lease-timeout-ms m)
                                c/*ha-lease-timeout-ms*))})
       (:ha-authority-read-error m)
       {:reason :authority-read-failed})))

(defn refresh-ha-authority-state
  ([deps db-name m]
   (refresh-ha-authority-state deps db-name m nil))
  ([{:keys [demote-ha-leader-fn ha-now-ms-fn observe-authority-state-fn]
     :or {ha-now-ms-fn #(System/currentTimeMillis)}}
    db-name m timeout-ms]
   (if-not (:ha-authority m)
     m
     (try
       (let [db-identity (:ha-db-identity m)
             now-ms (ha-now-ms-fn)
             observation (observe-authority-state-compat
                          observe-authority-state-fn
                          m
                          timeout-ms)
             {:keys [lease authority-membership-hash
                     db-identity-mismatch? membership-mismatch?]}
             observation
             m1 (-> m
                    (apply-authority-observation observation now-ms)
                    apply-authority-read-success)]
         (cond
           (and db-identity-mismatch? (= :leader (:ha-role m1)))
           (demote-ha-leader-fn db-name m1
                                :db-identity-mismatch
                                {:local-db-identity db-identity
                                 :authority-lease lease}
                                now-ms)

           (and membership-mismatch? (= :leader (:ha-role m1)))
           (demote-ha-leader-fn db-name m1
                                :membership-hash-mismatch
                                {:local-membership-hash (:ha-membership-hash m1)
                                 :authority-membership-hash
                                 authority-membership-hash}
                                now-ms)

           :else
           m1))
       (catch Exception e
         (let [error (authority-read-error e)
               failed-m (apply-authority-read-failure m error)]
           (log/warn e "HA read-lease failed"
                     {:db-name db-name
                      :ha-role (:ha-role m)})
           failed-m))))))

