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
  (:import [datalevin.tx_group.batch Batch]))

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

  Application exceptions and explicit rejections are request-local. Errors,
  interruption and engine cancellations must escape; infrastructure failures
  are recorded at their call boundary and bypass this classification."
  [^Throwable t]
  (let [data (ex-data t)]
    (and (not (instance? Error t))
         (not (instance? InterruptedException t))
         (not (contains? engine-errors (:error data)))
         (not (batch/pre-dispatch-cancellation? t)))))

(defn- infrastructure-call
  "Record infrastructure failures even when a body catches a reader exception.
  Explicit request rejections, including reader allowance exhaustion, stay local."
  [failure f]
  (fn [& args]
    (try
      (apply f args)
      (catch Throwable t
        (when-not (and (= :not-committed (:outcome (ex-data t)))
                       (request-rejection? t))
          (compare-and-set! failure nil t))
        (throw t)))))

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
  "Run one body, materialize and encode its result, and classify the member.

  Returns `{:kind :write ...}`, `{:kind :read-only ...}` or
  `{:kind :rejected ...}`. A request-local failure reaches this caller as a
  rejection; anything else is rethrown so the collector cancels interruption
  before dispatch or fences an infrastructure failure."
  [batch st descriptor view {:keys [row-fn body-cost encode-body failure]}]
  (try
    (let [result ((batch/op descriptor) view)
          ;; Expiry, fencing and interruption are re-read after every body, so a
          ;; batch that ran out of time cancels instead of dispatching.
          _ (when-let [t @failure] (throw t))
          _ (batch/check-preparation! batch)]
      (if (stage/rejected? view)
        ;; The body caught its own allowance failure, or aborted explicitly.
        ;; Either way its request is already ineligible and its staging is
        ;; dropped above.
        {:kind :rejected :error (stage/rejection view)}
        (let [rows (stage/materialize! st descriptor row-fn)]
          (if (.isEmpty ^java.util.List rows)
            {:kind :read-only :result result}
            (let [wal-body (wal-body! descriptor body-cost encode-body rows)]
              ;; Accept only after the whole member is known to fit: rows built
              ;; and the WAL body encoded and charged. A failure above still
              ;; discards the private staging, so a rejected member never leaks
              ;; into a successor's reads.
              (stage/accept! st descriptor)
              {:kind :write
               :result result
               :rows rows
               :wal-body wal-body})))))
    (catch Throwable t
      (stage/invalidate! view)
      (if (and (nil? @failure) (request-rejection? t))
        (do (batch/check-preparation! batch)
            {:kind :rejected :error t})
        (throw (or @failure t))))))

(defn- publish-blind-member!
  "Make one blind member's sealed rows visible to later bodies.

  Folded in member order at the point this member is reached, so a body sees
  exactly the blind rows of its predecessors and never a later member's rows,
  even though those rows are already encoded in the physical group. A blind
  request is fully prepaid before admission, so this is pure visibility: it
  charges nothing and does not go through that descriptor's staging."
  [st fold-row descriptor]
  (doseq [row (:rows (batch/data descriptor))]
    (let [[dbi op k v] (fold-row row)]
      (stage/fold-blind! st dbi op k v)))
  nil)

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
  count.

  The view is invalidated once the outcome is known, on every exit including a
  failure, so a body that retained its view can neither stage after its request
  was decided nor reach the stage from another thread."
  [batch st descriptor {:keys [base check row-fn body-cost encode-body]} values idx]
  (let [failure (atom nil)
        guard #(infrastructure-call failure %)
        view (stage/view st descriptor (guard base) check)
        outcome (try
                  (classify! batch st descriptor view
                              {:row-fn (guard row-fn) :body-cost (guard body-cost)
                               :encode-body (guard encode-body) :failure failure})
                  (finally
                    (stage/invalidate! view)))
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

  Members are visited in publication order, and each accepted write becomes the
  newest layer of the accepted prefix before the next member runs, so a body
  reads its predecessors and nothing after it. Returns the accepted write weight
  and the accepted member count, which is what the schedule and the compacted
  WAL body carrier are derived from."
  [batch st {:keys [fold-row] :as opts} values]
  (loop [idx 0 weight 0 accepted 0]
    (if (>= idx (batch/batch-count batch))
      [weight accepted]
      (let [descriptor (batch/batch-at batch idx)]
        (if (nil? (batch/op descriptor))
          ;; Blind: its caller-prepared rows and WAL body are already final, so
          ;; only its visibility is published.
          (do (publish-blind-member! st fold-row descriptor)
              (recur (inc idx) (inc weight) (inc accepted)))
          (if (= :write (run-member! batch st descriptor opts values idx))
            (recur (inc idx) (inc weight) (inc accepted))
            (recur (inc idx) weight accepted)))))))

(defn- prepare!
  "Run one sealed batch's bodies against `base`, and return its frozen plan."
  [batch st base {:keys [row-fn fold-row body-cost encode-body]}]
  (let [n (batch/batch-count batch)
        values (object-array n)
        check (fn [] (batch/check-preparation! batch))]
    (try
      (let [[weight accepted] (count-members!
                               batch st
                               {:base base :check check :fold-row fold-row
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
        (stage/release! st)))))

(defn prepare-batch!
  "Run this batch's ordered bodies and return its frozen dispatch plan.

  `opts`:
  - `:with-base` is `(fn [f])`, calling `f` with the `(fn [descriptor dbi key])`
    reader while holding one pinned native snapshot for the whole preparation; it
    defaults to calling `f` with `:base` directly for a store that needs no
    snapshot.
  - `:base` is `(fn [descriptor dbi key])`, reading one key from the native
    snapshot pinned for this preparation. The store charges the descriptor and
    returns a detached copy before any such copy is made.
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
  [batch {:keys [with-base base] :as opts}]
  (if-not (any-body? batch)
    nil
    (let [run (fn [base-fn] (prepare! batch (stage/create) base-fn opts))]
      (if with-base
        (with-base run)
        (run base)))))
