;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.promotion
  "HA demotion and follower-to-leader promotion helpers."
  (:require
   [datalevin.constants :as c]
   [datalevin.ha.authority :as auth]
   [datalevin.ha.clock :as clock]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.lease :as lease]
   [datalevin.ha.util :as hu]
   [taoensso.timbre :as log]))

(def ^:private restart-ha-candidate-promotion
  ::restart-ha-candidate-promotion)

(defn demote-ha-leader
  [db-name m reason details now-ms]
  (if (= :leader (:ha-role m))
    (let [drain-ms (long (or (:ha-demotion-drain-ms m)
                             c/*ha-demotion-drain-ms*))]
      (log/warn "Demoting HA leader for DB" db-name
                {:reason reason
                 :details details
                 :drain-ms drain-ms})
      (-> m
          (assoc :ha-role :demoting
                 :ha-demotion-reason reason
                 :ha-demotion-details details
                 :ha-demotion-drain-ms drain-ms
                 :ha-demoted-at-ms now-ms
                 :ha-demotion-drain-until-ms (+ (long now-ms) drain-ms))
          (dissoc :ha-leader-term
                  :ha-leader-last-applied-lsn
                  :ha-leader-fencing-pending?
                  :ha-leader-fencing-observed-lease
                  :ha-leader-fencing-last-error)))
    m))

(defn maybe-wait-unreachable-leader-before-pre-cas!
  [m lease]
  (let [renew-ms (long (or (:ha-lease-renew-ms m) c/*ha-lease-renew-ms*))
        lease-until-ms (long (or (:lease-until-ms lease) 0))
        wait-until-ms (+ lease-until-ms renew-ms)
        now-ms (System/currentTimeMillis)
        wait-ms (long (max 0 (- wait-until-ms now-ms)))]
    {:wait-ms wait-ms
     :wait-until-ms wait-until-ms}))

(defn- clear-ha-candidate-state
  [m]
  (dissoc m
          :ha-candidate-since-ms
          :ha-candidate-delay-ms
          :ha-candidate-rank-index
          :ha-candidate-pre-cas-wait-until-ms
          :ha-candidate-pre-cas-observed-version
          :ha-promotion-wait-before-cas-ms))

(defn- ordered-ha-members
  [deps m]
  ((or (:ordered-ha-members-fn deps) hu/ordered-ha-members) m))

(defn- ha-now-ms
  [deps]
  ((or (:ha-now-ms-fn deps) #(System/currentTimeMillis))))

(defn- maybe-complete-ha-leader-fencing
  [deps m db-name]
  ((:maybe-complete-ha-leader-fencing-fn deps) m db-name))

(defn- fetch-leader-watermark-lsn
  [deps db-name m lease]
  ((:fetch-leader-watermark-lsn-fn deps) db-name m lease))

(defn- fresh-ha-promotion-local-last-applied-lsn
  [deps m lease]
  ((:fresh-ha-promotion-local-last-applied-lsn-fn deps) m lease))

(defn- ha-member-watermarks
  [deps db-name m endpoints]
  ((:ha-member-watermarks-fn deps) db-name m endpoints))

(defn- highest-reachable-ha-member-watermark
  [deps db-name m watermarks]
  ((:highest-reachable-ha-member-watermark-fn deps) db-name m watermarks))

(defn- normalize-leader-watermark-result
  [deps lease leader-watermark]
  ((:normalize-leader-watermark-result-fn deps) lease leader-watermark))

(defn- ha-promotion-lag-guard
  [deps m lease local-last-applied-lsn]
  ((:ha-promotion-lag-guard-fn deps) m lease local-last-applied-lsn))

(defn- ha-local-last-applied-lsn
  [deps m]
  ((:ha-local-last-applied-lsn-fn deps) m))

(defn- ha-promotion-rank-index
  [deps m]
  (let [node-id (:ha-node-id m)
        members (ordered-ha-members deps m)]
    (first
     (keep-indexed
      (fn [idx member]
        (when (= node-id (:node-id member))
          idx))
      members))))

(defn- ha-promotion-delay-ms
  [deps m]
  (let [base-ms (long (or (:ha-promotion-base-delay-ms m)
                          c/*ha-promotion-base-delay-ms*))
        rank-ms (long (or (:ha-promotion-rank-delay-ms m)
                          c/*ha-promotion-rank-delay-ms*))
        rank-idx (ha-promotion-rank-index deps m)]
    (when (integer? rank-idx)
      (+ base-ms (* (long rank-idx) rank-ms)))))

(defn- pre-cas-lag-input
  ([deps db-name m observed-lease now-ms]
   (pre-cas-lag-input deps db-name m observed-lease now-ms nil))
  ([deps db-name m observed-lease now-ms prefetched-leader-watermark]
   (let [authority-lsn (long (or (:leader-last-applied-lsn observed-lease) 0))
         lease-expired? (lease/ha-lease-expired-for-promotion?
                         m observed-lease now-ms)
         local-last-applied-lsn
         (fresh-ha-promotion-local-last-applied-lsn deps m observed-lease)
         member-watermarks
         (when (and lease-expired?
                    (nil? prefetched-leader-watermark))
           (ha-member-watermarks deps db-name m [(:leader-endpoint observed-lease)]))
         leader-watermark
         (cond
           member-watermarks
           (normalize-leader-watermark-result
            deps
            observed-lease
            (get member-watermarks
                 (:leader-endpoint observed-lease)
                 {:reachable? false
                  :reason :missing-endpoint}))

           prefetched-leader-watermark
           prefetched-leader-watermark

           :else
           (fetch-leader-watermark-lsn deps db-name m observed-lease))
         reachable? (true? (:reachable? leader-watermark))
         leader-lsn (if reachable?
                      (long (or (:last-applied-lsn leader-watermark)
                                authority-lsn))
                      0)
         reachable-member-watermark
         (when (or member-watermarks
                   (not reachable?)
                   (and lease-expired?
                        (< leader-lsn authority-lsn)))
           (highest-reachable-ha-member-watermark
            deps
            db-name
            m
            (or member-watermarks
                (cond-> {}
                  (string? (:leader-endpoint observed-lease))
                  (assoc (:leader-endpoint observed-lease)
                         leader-watermark)))))
         reachable-member-lsn
         (when reachable-member-watermark
           (long (or (:last-applied-lsn reachable-member-watermark) 0)))
         max-local-member-lsn
         (hu/long-max2 (or reachable-member-lsn 0)
                       local-last-applied-lsn)
         effective-lsn
         (cond
           (and lease-expired?
                reachable?
                (< leader-lsn authority-lsn))
           (hu/long-max2 leader-lsn max-local-member-lsn)

           reachable?
           (hu/long-max3 authority-lsn leader-lsn max-local-member-lsn)

           :else
           max-local-member-lsn)]
     {:effective-lease (assoc observed-lease
                              :leader-last-applied-lsn effective-lsn)
      :local-last-applied-lsn local-last-applied-lsn
      :lease-expired? lease-expired?
      :leader-endpoint-reachable? reachable?
      :authority-last-applied-lsn authority-lsn
      :leader-watermark-last-applied-lsn (when reachable? leader-lsn)
      :leader-watermark leader-watermark
      :reachable-member-last-applied-lsn reachable-member-lsn
      :reachable-member-watermark reachable-member-watermark})))

(defn- ha-rejoin-promotion-failure-details
  [deps m]
  {:reason :rejoin-in-progress
   :blocked-until-ms (:ha-rejoin-promotion-blocked-until-ms m)
   :authority-owner-node-id (:ha-authority-owner-node-id m)
   :local-last-applied-lsn (ha-local-last-applied-lsn deps m)
   :leader-last-applied-lsn
   (long (or (get-in m [:ha-authority-lease :leader-last-applied-lsn]) 0))
   :ha-follower-last-error (:ha-follower-last-error m)
   :ha-follower-degraded? (:ha-follower-degraded? m)})

(defn- clear-ha-rejoin-promotion-block
  [deps m]
  (-> m
      (assoc :ha-rejoin-promotion-blocked? false
             :ha-rejoin-promotion-blocked-until-ms nil
             :ha-rejoin-promotion-cleared-ms (ha-now-ms deps))
      (dissoc :ha-rejoin-started-at-ms)))

(defn- maybe-clear-ha-rejoin-promotion-block
  [deps m now-ms]
  (if-not (:ha-rejoin-promotion-blocked? m)
    m
    (let [blocked-until-ms (long (or (:ha-rejoin-promotion-blocked-until-ms m) 0))
          owner-node-id (:ha-authority-owner-node-id m)
          local-node-id (:ha-node-id m)
          leader-lsn (long (or (get-in m [:ha-authority-lease
                                          :leader-last-applied-lsn])
                               0))
          local-lsn (ha-local-last-applied-lsn deps m)
          authority-fresh? (auth/ha-authority-read-fresh? m now-ms)
          lag-lsn (hu/nonnegative-long-diff leader-lsn local-lsn)
          lag-ok? (<= lag-lsn
                      (long (or (:ha-max-promotion-lag-lsn m) 0)))
          synced? (and authority-fresh?
                       (integer? owner-node-id)
                       (not= owner-node-id local-node-id)
                       (not (:ha-follower-degraded? m))
                       (nil? (:ha-follower-last-error m))
                       lag-ok?)]
      (cond
        synced?
        (clear-ha-rejoin-promotion-block deps m)

        (and (pos? blocked-until-ms)
             (>= (long now-ms) blocked-until-ms))
        (clear-ha-rejoin-promotion-block deps m)

        :else
        m))))

(defn- fail-ha-candidate
  [m reason details]
  (-> m
      clear-ha-candidate-state
      (assoc :ha-role :follower
             :ha-promotion-last-failure reason
             :ha-promotion-failure-details details)))

(defn- ha-follower-degraded-blocks-promotion?
  [deps m now-ms]
  (and (true? (:ha-follower-degraded? m))
       (let [lag-check (ha-promotion-lag-guard
                        deps
                        m
                        (:ha-authority-lease m)
                        (ha-local-last-applied-lsn deps m))]
         (or (not= :wal-gap (:ha-follower-degraded-reason m))
             (not (auth/ha-authority-read-fresh? m now-ms))
             (not (:ok? lag-check))))))

(defn maybe-enter-ha-candidate
  [deps m now-ms]
  (cond
    (not (contains? #{:follower :candidate} (:ha-role m)))
    m

    (and (= :candidate (:ha-role m))
         (not (lease/ha-lease-expired-for-promotion?
               m
               (:ha-authority-lease m)
               now-ms)))
    (-> m
        clear-ha-candidate-state
        (assoc :ha-role :follower))

    (not= :follower (:ha-role m))
    m

    (not (lease/ha-lease-expired-for-promotion?
          m
          (:ha-authority-lease m)
          now-ms))
    m

    (or (true? (:ha-db-identity-mismatch? m))
        (true? (:ha-membership-mismatch? m))
        (not= (:ha-membership-hash m)
              (:ha-authority-membership-hash m)))
    m

    (clock/ha-clock-skew-promotion-block-reason m now-ms)
    (let [reason (clock/ha-clock-skew-promotion-block-reason m now-ms)]
      (assoc m
             :ha-promotion-last-failure reason
             :ha-promotion-failure-details
             (clock/ha-clock-skew-promotion-failure-details m now-ms)))

    (ha-follower-degraded-blocks-promotion? deps m now-ms)
    (assoc m
           :ha-promotion-last-failure :follower-degraded
           :ha-promotion-failure-details
           {:reason (:ha-follower-degraded-reason m)
            :details (:ha-follower-degraded-details m)})

    (true? (:ha-rejoin-promotion-blocked? m))
    (assoc m
           :ha-promotion-last-failure :rejoin-in-progress
           :ha-promotion-failure-details
           (ha-rejoin-promotion-failure-details deps m))

    (not (auth/ha-authority-read-fresh? m now-ms))
    (assoc m
           :ha-promotion-last-failure :authority-read-failed
           :ha-promotion-failure-details
           (auth/ha-authority-read-failure-details m now-ms))

    :else
    (if-let [delay-ms (ha-promotion-delay-ms deps m)]
      (assoc m :ha-role :candidate
             :ha-candidate-since-ms now-ms
             :ha-candidate-delay-ms delay-ms
             :ha-candidate-rank-index (ha-promotion-rank-index deps m))
      (fail-ha-candidate m :missing-promotion-rank
                         {:ha-node-id (:ha-node-id m)
                          :ha-members (:ha-members m)}))))

(defn- try-promote-with-cas
  [deps db-name m authority db-identity observed-lease version now-ms lag-check]
  (let [local-start-ms now-ms
        local-start-nanos (System/nanoTime)
        acquire
        (ctrl/try-acquire-lease
         authority
         {:db-identity db-identity
          :leader-node-id (:ha-node-id m)
          :leader-endpoint (:ha-local-endpoint m)
          :lease-renew-ms (:ha-lease-renew-ms m)
          :lease-timeout-ms (:ha-lease-timeout-ms m)
          :leader-last-applied-lsn (:local-last-applied-lsn lag-check)
          :now-ms local-start-ms
          :observed-version version
          :observed-lease observed-lease})]
    (if (:ok? acquire)
      (let [{acquired-lease :lease
             acquired-version :version
             :keys [term authority-now-ms]} acquire
            observed-at-ms (ha-now-ms deps)]
        (let [promoted-m
              (-> m
                  clear-ha-candidate-state
                  (assoc :ha-role :leader
                         :ha-leader-term term
                         :ha-authority-lease acquired-lease
                         :ha-authority-version acquired-version
                         :ha-authority-now-ms authority-now-ms
                         :ha-lease-local-deadline-ms
                         (auth/authority-lease-local-deadline-ms
                          acquired-lease authority-now-ms local-start-ms)
                         :ha-lease-local-deadline-nanos
                         (auth/authority-lease-local-deadline-nanos
                          acquired-lease authority-now-ms local-start-nanos)
                         :ha-authority-owner-node-id (:leader-node-id acquired-lease)
                         :ha-authority-term (:term acquired-lease)
                         :ha-lease-until-ms (:lease-until-ms acquired-lease)
                         :ha-last-authority-refresh-ms observed-at-ms
                         :ha-db-identity-mismatch? false
                         :ha-membership-mismatch? false
                         :ha-promotion-last-failure nil
                         :ha-promotion-failure-details nil
                         :ha-leader-fencing-pending? true
                         :ha-leader-fencing-observed-lease observed-lease
                         :ha-leader-fencing-last-error nil))]
          (maybe-complete-ha-leader-fencing deps promoted-m db-name)))
      (fail-ha-candidate m :lease-cas-failed {:acquire acquire}))))

(defn- finalize-ha-candidate-promotion
  [deps db-name m authority db-identity observed-lease version now-ms lag-input]
  (if-not (lease/ha-lease-expired-for-promotion? m observed-lease now-ms)
    (fail-ha-candidate m :lease-not-expired
                       {:lease observed-lease})
    (let [lag-check
          (ha-promotion-lag-guard deps
                                  m
                                  (:effective-lease lag-input)
                                  (:local-last-applied-lsn lag-input))]
      (if-not (:ok? lag-check)
        (fail-ha-candidate m :lag-guard-failed
                           {:phase :pre-cas
                            :lag lag-check
                            :leader-lag-input lag-input})
        (try-promote-with-cas
         deps
         db-name
         m
         authority
         db-identity
         observed-lease
         version
         now-ms
         lag-check)))))

(defn- maybe-promote-after-authority-observation
  [deps db-name m authority db-identity obs now-ms]
  (let [m1 (-> (auth/apply-authority-observation m obs now-ms)
               (dissoc :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms))
        observed-lease (:lease obs)
        version (:version obs)
        db-identity-mismatch? (:db-identity-mismatch? obs)
        membership-mismatch? (:membership-mismatch? obs)]
    (cond
      db-identity-mismatch?
      (fail-ha-candidate m1 :db-identity-mismatch
                         {:local-db-identity db-identity
                          :authority-lease observed-lease})

      membership-mismatch?
      (fail-ha-candidate m1 :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m1)
                          :ha-authority-membership-hash
                          (:authority-membership-hash obs)})

      (not (lease/ha-lease-expired-for-promotion? m1 observed-lease now-ms))
      (fail-ha-candidate m1 :lease-not-expired
                         {:lease observed-lease})

      :else
      (let [bootstrap-empty? (lease/bootstrap-empty-lease? observed-lease)
            leader-watermark (when-not bootstrap-empty?
                               (fetch-leader-watermark-lsn
                                deps
                                db-name
                                m1
                                observed-lease))
            reachable? (or bootstrap-empty?
                           (true? (:reachable? leader-watermark)))]
        (if reachable?
          (let [lag-input (pre-cas-lag-input deps
                                             db-name
                                             m1
                                             observed-lease
                                             now-ms
                                             leader-watermark)]
            (finalize-ha-candidate-promotion
             deps
             db-name
             m1
             authority
             db-identity
             observed-lease
             version
             now-ms
             lag-input))
          (let [wait-info ((or (:maybe-wait-unreachable-leader-before-pre-cas-fn deps)
                               maybe-wait-unreachable-leader-before-pre-cas!)
                           m1
                           observed-lease)
                wait-ms (long (or (:wait-ms wait-info)
                                  (:slept-ms wait-info)
                                  0))]
            (if (pos? wait-ms)
              (assoc m1
                     :ha-candidate-pre-cas-wait-until-ms
                     (:wait-until-ms wait-info)
                     :ha-candidate-pre-cas-observed-version version
                     :ha-promotion-wait-before-cas-ms wait-ms)
              (let [lag-input (pre-cas-lag-input deps
                                                 db-name
                                                 m1
                                                 observed-lease
                                                 now-ms
                                                 leader-watermark)]
                (finalize-ha-candidate-promotion
                 deps
                 db-name
                 m1
                 authority
                 db-identity
                 observed-lease
                 version
                 now-ms
                 lag-input)))))))))

(defn- maybe-resume-ha-candidate-pre-cas-wait
  [deps db-name m now-ms]
  (when-let [wait-until-ms (:ha-candidate-pre-cas-wait-until-ms m)]
    (let [remaining-ms (long (max 0 (- (long wait-until-ms) (long now-ms))))]
      (if (pos? remaining-ms)
        (assoc m :ha-promotion-wait-before-cas-ms remaining-ms)
        (let [observed-version (:ha-candidate-pre-cas-observed-version m)
              current-obs (auth/promotion-authority-observation
                           (:observe-authority-state-fn deps)
                           m
                           now-ms)
              m1 (dissoc m
                         :ha-candidate-pre-cas-wait-until-ms
                         :ha-candidate-pre-cas-observed-version
                         :ha-promotion-wait-before-cas-ms)
              now-ms-2 (ha-now-ms deps)]
          (if (= observed-version (:version current-obs))
            (maybe-promote-after-authority-observation
             deps
             db-name
             m1
             (:ha-authority m1)
             (:ha-db-identity m1)
             current-obs
             now-ms-2)
            restart-ha-candidate-promotion))))))

(defn attempt-ha-candidate-promotion
  [deps db-name m now-ms]
  (let [observed-lease (:ha-authority-lease m)]
    (cond
      (or (true? (:ha-db-identity-mismatch? m))
          (true? (:ha-membership-mismatch? m))
          (not= (:ha-membership-hash m)
                (:ha-authority-membership-hash m)))
      (fail-ha-candidate m :membership-hash-mismatch
                         {:ha-membership-hash (:ha-membership-hash m)
                          :ha-authority-membership-hash
                          (:ha-authority-membership-hash m)})

      (not (lease/ha-lease-expired-for-promotion? m observed-lease now-ms))
      (fail-ha-candidate m :lease-not-expired
                         {:lease observed-lease})

      (clock/ha-clock-skew-promotion-block-reason m now-ms)
      (let [reason (clock/ha-clock-skew-promotion-block-reason m now-ms)]
        (fail-ha-candidate m reason
                           (clock/ha-clock-skew-promotion-failure-details
                            m
                            now-ms)))

      (false? (:ha-authority-read-ok? m))
      (fail-ha-candidate m :authority-read-failed
                         (auth/ha-authority-read-failure-details m))

      :else
      (let [promotion-now-ms (ha-now-ms deps)]
        (maybe-promote-after-authority-observation
         deps
         db-name
         m
         (:ha-authority m)
         (:ha-db-identity m)
         (auth/promotion-authority-observation
          (:observe-authority-state-fn deps)
          m
          promotion-now-ms)
         promotion-now-ms)))))

(defn maybe-promote-ha-candidate
  [deps db-name m now-ms]
  (if (not= :candidate (:ha-role m))
    m
    (try
      (let [candidate-since-ms (long (or (:ha-candidate-since-ms m) now-ms))
            candidate-delay-ms (long (or (:ha-candidate-delay-ms m) 0))
            elapsed-ms (max 0 (- (long now-ms) candidate-since-ms))]
        (if (< elapsed-ms candidate-delay-ms)
          m
          (let [resume-result (maybe-resume-ha-candidate-pre-cas-wait
                               deps db-name m now-ms)]
            (cond
              (map? resume-result)
              resume-result

              (= restart-ha-candidate-promotion resume-result)
              (attempt-ha-candidate-promotion
               deps
               db-name
               (dissoc m
                       :ha-candidate-pre-cas-wait-until-ms
                       :ha-candidate-pre-cas-observed-version
                       :ha-promotion-wait-before-cas-ms)
               now-ms)

              :else
              (attempt-ha-candidate-promotion deps db-name m now-ms)))))
      (catch Exception e
        (fail-ha-candidate m :promotion-exception
                           {:message (ex-message e)})))))

(defn advance-ha-follower-or-candidate
  [deps db-name m]
  (let [state-now-ms (ha-now-ms deps)]
    (if (or ((:ha-demotion-draining?-fn deps) m state-now-ms)
            (not (contains? #{:follower :candidate} (:ha-role m))))
      m
      (let [m1 (maybe-clear-ha-rejoin-promotion-block deps m state-now-ms)
            m2 (maybe-enter-ha-candidate deps m1 state-now-ms)]
        (maybe-promote-ha-candidate deps db-name m2 (ha-now-ms deps))))))
