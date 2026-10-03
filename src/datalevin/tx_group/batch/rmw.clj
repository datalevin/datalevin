;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.rmw
  "Ordered read-modify-write preparation for the new write protocol.

  One call runs every body of one sealed batch, in sealed FIFO order, on the
  elected execution thread, and returns the frozen dispatch plan:

    nil                              every member stayed blind
    {:weight w}                      accepted writes exist; the branches run
    {:weight 0 :values values}       no accepted writes; complete without WAL

  Each body sees the accepted prefix of its predecessors plus its own private
  staging, and nothing from a request that was rejected. A request-local failure
  discards only that request's staging; a batch-level failure, expiry, fence or
  interrupt cancels the whole undispatched batch.

  Blind members keep their caller-prepared rows and WAL body: only their
  visibility is folded in, so the physical group stays the one the caller
  prepared."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.stage :as stage])
  (:import [datalevin.tx_group.batch Batch Descriptor]))

;; Failures the engine itself raises. They are never a request's own business,
;; even though they carry the same `:outcome :not-committed` marker, so they
;; cancel the batch instead of rejecting one member.
(def ^:private engine-errors
  #{:txlog/runtime-fenced
    :txlog/runtime-closed
    :txlog/write-deadline-exceeded
    :txlog/write-interrupted})

(defn- request-rejection?
  "Whether this body failure rejects only its own request.

  Ordinary application failures, explicit aborts, validation failures and
  allowance exhaustion all carry `:outcome :not-committed`. Classification uses
  that marker and excludes the engine's own errors, so neither a broad
  `Throwable` catch nor an application's own timeout exception can turn an
  infrastructure failure into a recoverable rejection."
  [^Throwable t]
  (let [data (ex-data t)]
    (and (= :not-committed (:outcome data))
         (not (contains? engine-errors (:error data)))
         (not (batch/pre-dispatch-cancellation? t)))))

(defn- wal-body!
  "Encode one accepted member's frozen rows into its WAL body.

  The conservative estimate is charged first, so a body that would not fit is
  rejected before it is encoded rather than after; the encoding's own size is
  then charged, so the reserve never understates what the request owns."
  ^bytes [descriptor body-cost encode-body rows]
  (let [estimate (long (body-cost rows))]
    (batch/charge! descriptor estimate)
    (let [^bytes body (encode-body rows {})]
      (batch/charge! descriptor (max 0 (- (long (alength body)) estimate)))
      body)))

(defn- classify!
  "Run one body, freeze its result and classify the member.

  Returns `{:kind :write ...}`, `{:kind :read-only ...}` or
  `{:kind :rejected ...}`. A request-local failure reaches this caller as a
  rejection; anything else is rethrown so the batch fails and the runtime
  fences."
  [batch st descriptor view {:keys [row-fn body-cost encode-body]}]
  (try
    (let [result ((batch/op descriptor) view)]
      ;; Expiry, fencing and interruption are re-read after every body, so a
      ;; batch that ran out of time cancels instead of dispatching.
      (batch/check-preparation! batch)
      (if (stage/rejected? view)
        ;; The body caught its own allowance failure, or aborted explicitly.
        ;; Either way its request is already ineligible and its staging is
        ;; dropped below.
        {:kind :rejected :error (stage/rejection view)}
        (let [rows (stage/freeze! st descriptor row-fn)]
          (if (.isEmpty ^java.util.List rows)
            {:kind :read-only :result result}
            {:kind :write
             :result result
             :rows rows
             :wal-body (wal-body! descriptor body-cost encode-body rows)}))))
    (catch Throwable t
      (stage/invalidate! view)
      (if (request-rejection? t)
        (do (batch/check-preparation! batch)
            {:kind :rejected :error t})
        (throw t)))))

(defn- fold-blind!
  "Make the sealed blind rows visible to every later body.

  Walked in request order before any body runs, so a body sees the whole
  accepted prefix its predecessors wrote, blind or not."
  [st batch fold-row]
  (dotimes [idx (batch/batch-count batch)]
    (let [descriptor (batch/batch-at batch idx)]
      (when (nil? (batch/op descriptor))
        (doseq [row (:rows (batch/data descriptor))]
          (let [[dbi op k v] (fold-row row)]
            (stage/fold! st descriptor dbi op k v)))
        (stage/accept! st descriptor)))))

(defn- any-body?
  "Whether this batch owns at least one ordered body."
  [^Batch batch]
  (loop [idx 0]
    (if (>= idx (batch/batch-count batch))
      false
      (if (batch/op (batch/batch-at batch idx))
        true
        (recur (inc idx))))))

(defn- run-member!
  "Run one body, freeze its outcome and record it on the descriptor.

  Returns `:write` when this member ends with accepted physical writes, and
  `:other` for a read-only or rejected one. The descriptor's data and the
  batch's own value slot are both completed here, so the loop below only has to
  count."
  [batch st descriptor {:keys [base check row-fn body-cost encode-body]} values idx]
  (let [view (stage/view st descriptor base check)
        outcome (classify! batch st descriptor view
                            {:row-fn row-fn :body-cost body-cost
                             :encode-body encode-body})
        kind (:kind outcome)]
    (case kind
      :rejected
      (let [error (:error outcome)]
        (batch/set-data! descriptor {:rejection error})
        (aset values idx (batch/rejected error))
        :other)

      :read-only
      (do (batch/set-data! descriptor {:result (:result outcome)})
          (aset values idx (:result outcome))
          :other)

      :write
      (do (batch/set-data! descriptor {:rows (:rows outcome)
                                       :result (:result outcome)
                                       :wal-body (:wal-body outcome)})
          (aset values idx (:result outcome))
          :write))))

(defn- count-members!
  "Classify every member in sealed FIFO order.

  Returns the accepted write weight and the accepted member count, which is what
  the schedule and the compacted WAL body carrier are derived from."
  [batch st opts values]
  (loop [idx 0 weight 0 accepted 0]
    (if (>= idx (batch/batch-count batch))
      [weight accepted]
      (let [descriptor (batch/batch-at batch idx)]
        (if (nil? (batch/op descriptor))
          ;; Blind: its caller-prepared rows and WAL body are already final.
          (recur (inc idx) (inc weight) (inc accepted))
          (if (= :write (run-member! batch st descriptor opts values idx))
            (recur (inc idx) (inc weight) (inc accepted))
            (recur (inc idx) weight accepted)))))))

(defn prepare-batch!
  "Run this batch's ordered bodies and return its frozen dispatch plan.

  `opts`:
  - `:base` is `(fn [dbi key])`, reading one key from the native snapshot pinned
    for this preparation.
  - `:row-fn` is `(fn [dbi op key value])`, building one physical row from a
    staged write.
  - `:fold-row` is `(fn [row])`, splitting one caller-prepared blind row into
    `[dbi op key value]`.
  - `:body-cost` is `(fn [rows])`, a conservative estimate of the encoded size of
    those rows, charged before they are encoded.
  - `:encode-body` is `(fn [rows hooks])`, serializing rows into one WAL body.

  Publishing the accepted member count and the final schedule is the last step,
  so the dispatch transition and both branches always read values that describe
  the writes that will actually run."
  [batch {:keys [base row-fn fold-row body-cost encode-body]}]
  (if-not (any-body? batch)
    nil
    (let [n (batch/batch-count batch)
          st (stage/create)
          values (object-array n)
          check (fn [] (batch/check-preparation! batch))]
      (try
        (fold-blind! st batch fold-row)
        (let [[weight accepted] (count-members!
                                 batch st
                                 {:base base :check check
                                  :row-fn row-fn :body-cost body-cost
                                  :encode-body encode-body}
                                 values)]
          (batch/set-accepted-count! batch accepted)
          (batch/freeze-schedule! batch weight)
          {:weight weight
           ;; A batch with no accepted write still owes every member its own
           ;; value: its body result, or its own rejection.
           :values (when (zero? weight) values)})
        (finally
          (stage/release! st))))))