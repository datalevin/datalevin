;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw.application
  "The native owner, its frozen replay history, and writer refresh after growth."
  (:require [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.lmdb :as l]
            [datalevin.tx-group.phase :as phase]
            [datalevin.util :as u])
  (:import [datalevin.utl WriterContext]
           [java.util ArrayList]))

(defrecord Application
  [raw view batch history failure opts active-view dispatched? applier])

(defn retain-failures!
  "Retain native failures even on value-only resolvers without a member view."
  [view failure]
  (.setTransactionFailed ^WriterContext (l/writer-context view)
                         (fn [t] (when-not @failure (vreset! failure t)))))

(defn create
  "Own one writer attempt. Only the active member view is borrowed for refresh;
  replay retains frozen row regions, never resolvers or user bodies."
  [raw view batch history failure opts]
  (->Application raw view batch history failure opts
                 (volatile! nil) (volatile! false)
                 (volatile! (cpp/prepared-row-applier view))))

(defn activate-member!
  "Borrow the current member's view until it leaves its scope."
  [^Application application view]
  (vreset! (:active-view application) view))

(defn active-member-view [^Application application]
  @(:active-view application))

(defn- refresh!
  [^Application application]
  (let [{:keys [raw view applier]} application]
    (doseq [view [view (active-member-view application)] :when view]
      (.setTransaction ^WriterContext (l/writer-context view) @(l/write-txn raw)))
    (vreset! applier (cpp/prepared-row-applier view))))

(defn apply-rows!
  "Apply a frozen region, replaying the ordered history after map growth.
  A rolled-back unique insert is removed from history before propagating."
  [^Application application rows]
  (let [{:keys [batch history opts applier dispatched?]} application]
    (when history (.add ^ArrayList history rows))
    (loop [replay? false]
      (let [states (l/transaction-states @(l/write-txn (:raw application)))
            failure (try
                      (if replay?
                        (do
                          (doseq [region history] (@applier region))
                          (l/restore-transaction-states! states (:view application)))
                        (@applier rows))
                      nil
                      (catch Throwable t t))]
        (when failure
          (when (and (l/blind-unique-collision? failure)
                     (:unique-inserts-rolled-back? (ex-data failure)))
            (when history
              (.remove ^ArrayList history (int (dec (.size ^ArrayList history)))))
            (throw failure))
          (if (and history (:resized (ex-data failure)))
            (do (phase/phase! :native-resized batch)
                (refresh! application)
                (l/enlist-transaction-states! @(l/write-txn (:raw application)) states)
                (recur true))
            (if-let [on-error! (:application-error! opts)]
              (on-error! failure)
              (throw failure))))))
    (phase/phase! (if @dispatched? :native-rows-tail :native-rows-prefix)
                  {:batch batch :rows (count rows)})))

(defn begin-dispatch! [^Application application]
  (vreset! (:dispatched? application) true))

(defn apply-with-frozen-commit-retry!
  "Retry commit-stage growth using frozen writes and the existing WAL receipt."
  [raw batch history prepare! before-commit!]
  (let [prepared? (volatile! false)
        states (volatile! nil)]
    (u/repeat-try-catch c/+in-tx-overflow-times+
      #(and history @prepared? (l/resized? %))
      (cpp/apply-native-once!
        raw
        (fn [wdb]
          (if @prepared?
            (let [apply! (cpp/prepared-row-applier wdb)]
              (l/enlist-transaction-states! @(l/write-txn raw) @states)
              (phase/phase! :native-resized batch)
              (doseq [rows history] (apply! rows))
              (l/restore-transaction-states! @states wdb))
            (let [result (prepare! wdb)]
              (vreset! states (l/transaction-states @(l/write-txn raw)))
              (vreset! prepared? true)
              result)))
        before-commit!))))
