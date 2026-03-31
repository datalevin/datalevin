;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.clock
  "Clock-skew observation and gating helpers."
  (:require
   [clojure.string :as s]
   [datalevin.constants :as c]
   [datalevin.ha.util :as hu]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent Callable ExecutorService Future]))

(defn parse-ha-clock-skew-output
  [output]
  (let [trimmed (some-> output s/trim)]
    (cond
      (s/blank? trimmed)
      {:ok? false
       :reason :invalid-output
       :message "Clock skew hook returned blank output"
       :output output}

      :else
      (try
        {:ok? true
         :clock-skew-ms (Math/abs (long (Long/parseLong trimmed)))}
        (catch NumberFormatException _
          {:ok? false
           :reason :invalid-output
           :message "Clock skew hook output must be an integer millisecond value"
           :output output})))))

(defn- run-command-with-timeout
  [cmd env timeout-ms]
  (try
    (let [process-builder (ProcessBuilder. ^java.util.List (mapv str cmd))
          env-map (.environment process-builder)
          _ (doseq [[k v] env]
              (.put env-map (str k) (str v)))
          _ (.redirectErrorStream process-builder true)
          process (.start process-builder)
          finished? (.waitFor process
                              (long timeout-ms)
                              java.util.concurrent.TimeUnit/MILLISECONDS)]
      (if finished?
        (let [exit (.exitValue process)
              output (try
                       (slurp (.getInputStream process) :encoding "UTF-8")
                       (catch Exception _
                         ""))]
          {:ok? (zero? exit)
           :exit exit
           :output output})
        (do
          (.destroy process)
          (when-not (.waitFor process
                              200
                              java.util.concurrent.TimeUnit/MILLISECONDS)
            (.destroyForcibly process))
          {:ok? false
           :reason :timeout
           :timeout-ms timeout-ms})))
    (catch Exception e
      {:ok? false
       :reason :exception
       :message (ex-message e)})))

(defn run-ha-clock-skew-hook
  [db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        {:keys [cmd timeout-ms retries retry-delay-ms]} (:ha-clock-skew-hook m)]
    (if-not (and (vector? cmd) (seq cmd))
      {:ok? true
       :skipped? true
       :paused? false
       :reason :clock-skew-hook-unconfigured
       :budget-ms budget-ms}
      (let [env {"DTLV_DB_NAME" db-name
                 "DTLV_HA_NODE_ID" (str (or (:ha-node-id m) ""))
                 "DTLV_HA_ENDPOINT"
                 (str (or (:ha-local-endpoint m) ""))
                 "DTLV_CLOCK_SKEW_BUDGET_MS" (str budget-ms)}
            max-attempts (inc (long (or retries 0)))
            timeout-ms (long (or timeout-ms 3000))
            retry-delay-ms (long (or retry-delay-ms 1000))]
        (loop [attempt 1]
          (let [result (run-command-with-timeout cmd env timeout-ms)
                parsed (if (:ok? result)
                         (merge result
                                (parse-ha-clock-skew-output
                                 (:output result)))
                         result)
                paused? (if (:ok? parsed)
                          (> (long (or (:clock-skew-ms parsed) 0))
                             budget-ms)
                          true)]
            (if (:ok? parsed)
              (assoc parsed
                     :attempt attempt
                     :budget-ms budget-ms
                     :paused? paused?
                     :reason (if paused?
                               :clock-skew-budget-breached
                               :clock-skew-within-budget))
              (if (< attempt max-attempts)
                (do
                  (Thread/sleep retry-delay-ms)
                  (recur (u/long-inc attempt)))
                (assoc parsed
                       :attempt attempt
                       :budget-ms budget-ms
                       :paused? true)))))))))

(defn ha-clock-skew-hook-configured?
  [m]
  (let [cmd (get-in m [:ha-clock-skew-hook :cmd])]
    (and (vector? cmd) (seq cmd))))

(defn ha-clock-skew-check-fresh?
  [m now-ms]
  (or (not (ha-clock-skew-hook-configured? m))
      (when-let [last-check-ms
                 (when (integer? (:ha-clock-skew-last-check-ms m))
                   (long (:ha-clock-skew-last-check-ms m)))]
        (<= (hu/nonnegative-long-diff (long now-ms) last-check-ms)
            (long (max 100
                       (long (or (:ha-lease-renew-ms m)
                                 c/*ha-lease-renew-ms*))))))))

(defn ha-clock-skew-promotion-block-reason
  [m now-ms]
  (cond
    (true? (:ha-clock-skew-paused? m))
    :clock-skew-paused

    (and (ha-clock-skew-hook-configured? m)
         (not (ha-clock-skew-check-fresh? m now-ms))
         (true? (:ha-clock-skew-refresh-pending? m)))
    :clock-skew-check-pending

    (and (ha-clock-skew-hook-configured? m)
         (not (ha-clock-skew-check-fresh? m now-ms)))
    :clock-skew-check-stale))

(defn run-ha-clock-skew-hook-safe
  [run-ha-clock-skew-hook-fn db-name m budget-ms]
  (let [run-hook-fn (or run-ha-clock-skew-hook-fn run-ha-clock-skew-hook)]
    (try
      (run-hook-fn db-name m)
      (catch Exception e
        {:ok? false
         :paused? true
         :reason :exception
         :budget-ms budget-ms
         :message (ex-message e)
         :data (ex-data e)}))))

(defn submit-ha-clock-skew-refresh
  [run-ha-clock-skew-hook-fn db-name m budget-ms]
  (let [run-hook-fn (or run-ha-clock-skew-hook-fn run-ha-clock-skew-hook)
        task (reify Callable
               (call [_]
                 (run-ha-clock-skew-hook-safe
                  run-hook-fn db-name m budget-ms)))
        executor (:ha-probe-executor m)]
    (try
      (cond
        (instance? ExecutorService executor)
        (.submit ^ExecutorService executor ^Callable task)

        :else
        (future-call #(.call ^Callable task)))
      (catch Exception e
        (log/warn e "Failed to submit HA clock skew refresh"
                  {:db-name db-name
                   :ha-node-id (:ha-node-id m)})
        nil))))

(defn apply-ha-clock-skew-result
  [ha-now-ms-fn m budget-ms result]
  (assoc m
         :ha-clock-skew-budget-ms budget-ms
         :ha-clock-skew-refresh-future nil
         :ha-clock-skew-refresh-pending? false
         :ha-clock-skew-paused? (true? (:paused? result))
         :ha-clock-skew-last-check-ms (ha-now-ms-fn)
         :ha-clock-skew-last-observed-ms (:clock-skew-ms result)
         :ha-clock-skew-last-result result))

(defn realize-ha-clock-skew-refresh
  [ha-now-ms-fn m budget-ms]
  (let [refresh-future (:ha-clock-skew-refresh-future m)]
    (cond
      (and (instance? Future refresh-future)
           (.isDone ^Future refresh-future))
      (let [result (try
                     (.get ^Future refresh-future)
                     (catch Exception e
                       {:ok? false
                        :paused? true
                        :reason :exception
                        :budget-ms budget-ms
                        :message (ex-message e)
                        :data (ex-data e)}))]
        (apply-ha-clock-skew-result ha-now-ms-fn m budget-ms result))

      (instance? Future refresh-future)
      (assoc m
             :ha-clock-skew-budget-ms budget-ms
             :ha-clock-skew-refresh-pending? true)

      :else
      (assoc m
             :ha-clock-skew-budget-ms budget-ms
             :ha-clock-skew-refresh-pending? false))))

(defn refresh-ha-clock-skew-state
  [{:keys [demote-ha-leader-fn ha-now-ms-fn run-ha-clock-skew-hook-fn]
    :or {ha-now-ms-fn #(System/currentTimeMillis)}}
   db-name m]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        role (:ha-role m)
        hook-configured? (ha-clock-skew-hook-configured? m)
        applicable? (or (#{:follower :candidate} role)
                        (and (= :leader role) hook-configured?))]
    (cond
      (not applicable?)
      (assoc m
             :ha-clock-skew-budget-ms budget-ms
             :ha-clock-skew-refresh-future nil
             :ha-clock-skew-refresh-pending? false
             :ha-clock-skew-paused? false)

      (not hook-configured?)
      (apply-ha-clock-skew-result
       ha-now-ms-fn
       m
       budget-ms
       {:ok? true
        :skipped? true
        :paused? false
        :reason :clock-skew-hook-unconfigured
        :budget-ms budget-ms})

      :else
      (let [next-m (realize-ha-clock-skew-refresh ha-now-ms-fn m budget-ms)
            result (:ha-clock-skew-last-result next-m)]
        (cond
          (and (= :leader role)
               (true? (:ha-clock-skew-paused? next-m)))
          (demote-ha-leader-fn db-name next-m
                               :clock-skew-paused
                               {:budget-ms budget-ms
                                :clock-skew-result result}
                               (ha-now-ms-fn))

          (instance? Future (:ha-clock-skew-refresh-future next-m))
          next-m

          :else
          (let [refresh-future
                (submit-ha-clock-skew-refresh
                 run-ha-clock-skew-hook-fn
                 db-name
                 next-m
                 budget-ms)]
            (cond-> (assoc next-m
                           :ha-clock-skew-refresh-future refresh-future
                           :ha-clock-skew-refresh-pending?
                           (boolean refresh-future))
              (and (boolean refresh-future)
                   (not (integer? (:ha-clock-skew-last-check-ms next-m))))
              (assoc :ha-clock-skew-last-result
                     {:ok? false
                      :pending? true
                      :paused? false
                      :reason :clock-skew-check-pending
                      :budget-ms budget-ms}))))))))

(defn ha-clock-skew-promotion-failure-details
  [m now-ms]
  (let [budget-ms (long (or (:ha-clock-skew-budget-ms m)
                            c/*ha-clock-skew-budget-ms*))
        result (:ha-clock-skew-last-result m)
        reason (ha-clock-skew-promotion-block-reason m now-ms)
        base {:budget-ms budget-ms
              :last-check-ms (:ha-clock-skew-last-check-ms m)
              :check result}]
    (cond
      (= :clock-skew-paused reason)
      (if (= :clock-skew-budget-breached (:reason result))
        (assoc base
               :reason :clock-skew-budget-breached
               :clock-skew-ms (:clock-skew-ms result))
        (assoc base
               :reason (or (:reason result)
                           :clock-skew-check-failed)))

      (= :clock-skew-check-pending reason)
      (assoc base
             :reason :clock-skew-check-pending)

      (= :clock-skew-check-stale reason)
      (assoc base
             :reason :clock-skew-check-stale
             :age-ms
             (when (integer? (:ha-clock-skew-last-check-ms m))
               (hu/nonnegative-long-diff
                (long now-ms)
                (long (:ha-clock-skew-last-check-ms m)))))

      :else
      (assoc base
             :reason :clock-skew-check-failed))))
