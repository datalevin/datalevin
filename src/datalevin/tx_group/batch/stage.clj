;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.batch.stage
  "Batch-local ordered state for the new write protocol.

  One `Stage` lives for exactly one ordered preparation of one sealed batch, on
  the elected execution thread. It holds two disjoint layers:

    - the accepted prefix: the writes of every already accepted member, in
      sealed FIFO order, and
    - per-descriptor private staging: the writes of the member whose body is
      currently running.

  A body reads its own private writes first, then the accepted prefix, then the
  pinned native base for this preparation. It therefore never reads another
  request's unaccepted rows, and never observes its own writes twice.

  Nothing here is thread-safe and nothing escapes the preparation thread: the
  stage is discarded when preparation finishes. Every owned allocation is
  charged to the descriptor that caused it, before it happens."
  (:require [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge])
  (:import [java.util Arrays Comparator IdentityHashMap LinkedHashMap Map$Entry
            TreeMap]
           [java.util.concurrent.atomic AtomicBoolean AtomicReference]))

(defn- byte-comparator
  "Unsigned lexicographic order over encoded keys, which may hold any byte."
  ^Comparator [^Comparator comparator]
  (or comparator
      (reify Comparator
        (compare [_ a b] (Arrays/compareUnsigned ^bytes a ^bytes b)))))

;; A tombstone is an identity marker rather than a user value, so a delete can
;; never be confused with an absent key and no extra wrapper is allocated.
(def ^:private deleted (Object.))

(deftype Stage [^Comparator comparator
                ;; DBI -> TreeMap of key -> value, in accepted FIFO order.
                ^LinkedHashMap prefix
                ;; Descriptor identity -> DBI -> TreeMap of key -> value.
                ^IdentityHashMap privates])

(defn create
  "Create an empty stage ordered by `comparator`, unsigned bytes by default."
  ^Stage ([] (create nil))
  ([^Comparator comparator]
   (Stage. (byte-comparator comparator) (LinkedHashMap.) (IdentityHashMap.))))

(deftype View [^Stage stage
               descriptor
               ;; `(fn [descriptor dbi key])`, reading one key from the native base
               ;; pinned for this preparation, or nil when the store has no such
               ;; base. The store charges the descriptor for the detached copy.
               base
               ;; `(fn [])` re-reading the batch's cutoff and terminal state, or
               ;; nil for a store that needs no such check.
               check
               ^AtomicBoolean rejected?
               ^AtomicReference rejection
               ;; The thread that owns this view for the whole of its body, and
               ;; whether that body has already returned.
               owner
               ^AtomicBoolean valid?])

;; ---------------------------------------------------------------------------
;; Private staging

(defn- private-dbis
  "The requesting descriptor's per-DBI private key maps, or nil before its first
  write. Identity-keyed, so distinct descriptors can never collide."
  ^LinkedHashMap [^Stage stage descriptor]
  (.get ^IdentityHashMap (.privates stage) descriptor))

(defn- private-keys
  "The requesting descriptor's ordered private keys for `dbi`, or nil."
  ^TreeMap [^Stage stage descriptor dbi]
  (when-let [^LinkedHashMap by-dbi (private-dbis stage descriptor)]
    (.get by-dbi dbi)))

(defn- writable-keys!
  "The requesting descriptor's private key map for `dbi`, created on first use.

  Charged before the maps are created, so an allowance failure leaves this
  descriptor's staging exactly as it was."
  ^TreeMap [^Stage stage descriptor dbi]
  (let [^IdentityHashMap privates (.privates stage)
        existing (private-keys stage descriptor dbi)]
    (if (some? existing)
      existing
      (let [^LinkedHashMap by-dbi
            (or (private-dbis stage descriptor)
                (let [created (LinkedHashMap.)]
                  ;; The descriptor's own staging control carrier.
                  (batch/charge! descriptor charge/vector-wrapper)
                  (.put privates descriptor created)
                  created))
            keys (TreeMap. (.comparator stage))]
        (batch/charge! descriptor charge/ordered-map-container)
        (.put by-dbi dbi keys)
        keys))))

(defn- copy-bytes
  "A private copy of one byte array, for storage the stage's charges already
  cover."
  ^bytes [^bytes bs]
  (Arrays/copyOf bs (alength bs)))

(defn- own-bytes
  "A private copy of one byte array the stage retains or hands to a body, charged
  to the descriptor before the copy is made.

  The stage must never alias a caller's, a body's or the native snapshot's
  mutable bytes: a later mutation of the source would otherwise change an
  accepted or staged row, and a mutation of a value handed to a body would
  change a predecessor's native row after its WAL body was already encoded."
  ^bytes [descriptor ^bytes bs]
  (batch/charge! descriptor (charge/array-bytes 1 (alength bs)))
  (copy-bytes bs))

(defn- stage-write!
  "Record one write in the requesting descriptor's private staging.

  Charged before anything is stored, so a failure rejects this request with its
  earlier writes still private. The key and value are frozen into stage-owned
  copies, so mutating the caller's arrays afterwards cannot change staging."
  [stage descriptor dbi key value]
  (let [^TreeMap keys (writable-keys! stage descriptor dbi)
        new-key? (not (.containsKey keys key))
        _ (batch/charge! descriptor
                         (if new-key?
                           (Math/addExact (long charge/ordered-map-entry)
                                          (long charge/per-key-staging-state))
                           (long charge/ordered-map-entry)))
        owned-key (own-bytes descriptor key)
        owned-value (if (identical? value ::del) deleted (own-bytes descriptor value))]
    ;; A tombstone or a value: never nil, so `nil` keeps meaning "not staged".
    (.put keys owned-key owned-value)
    nil))

;; ---------------------------------------------------------------------------
;; The accepted prefix

(defn- prefix-keys
  "The accepted prefix's ordered keys for `dbi`, or nil when untouched."
  ^TreeMap [^Stage stage dbi]
  (.get ^LinkedHashMap (.prefix stage) dbi))

(defn- prefix-value
  "Read one key through the accepted prefix, then the native base.

  `base` receives this request's descriptor so the store can charge a native
  read's detached copy to it before the copy is made."
  [^Stage stage base descriptor dbi key]
  (let [^TreeMap keys (prefix-keys stage dbi)
        staged (when (some? keys) (.get keys key))]
    (if (nil? staged)
      (when base (base descriptor dbi key))
      (when-not (identical? deleted staged) staged))))

(defn- merge-charge
  "Charge for every prefix node this request's merge would create.

  Computed before the merge so the accepted prefix is only ever grown by a
  request already known to fit."
  ^long [^Stage stage descriptor]
  (let [^LinkedHashMap prefix (.prefix stage)
        ^LinkedHashMap by-dbi (private-dbis stage descriptor)]
    (if (nil? by-dbi)
      0
      (reduce
       (fn [^long total ^Map$Entry by-key]
         (let [dbi (key by-key)
               ^TreeMap keys (val by-key)]
           (reduce
            (fn [^long charged ^Map$Entry node]
              (if (and (.containsKey prefix dbi)
                       (.containsKey ^TreeMap (.get prefix dbi) (key node)))
                charged
                (Math/addExact charged
                               (Math/addExact (long charge/ordered-map-entry)
                                              (long charge/per-key-staging-state)))))
            (if (.containsKey prefix dbi)
              total
              (Math/addExact total charge/ordered-map-container))
            (.entrySet keys))))
       0
       (.entrySet by-dbi)))))

(defn- write-count
  "Distinct keys this request staged, which is also its physical row count."
  ^long [^LinkedHashMap by-dbi]
  (if (nil? by-dbi)
    0
    (reduce (fn [^long n ^TreeMap keys] (+ n (.size keys)))
            0
            (.values by-dbi))))

(defn- rows!
  "Materialize this request's staged writes as physical rows.

  `row-fn` receives `(dbi op key value)`, with `op` `:put` or `:del` and `value`
  nil for a delete. Keys stay in the requesting descriptor's insertion-ordered
  DBI order and each DBI's ordered key order, so one request's rows are stable.
  Charged before the rows are built."
  ^java.util.List [descriptor ^LinkedHashMap by-dbi row-fn]
  (let [n (write-count by-dbi)
        rows (java.util.ArrayList. (int n))]
    (when (pos? (long n))
      (batch/charge! descriptor
                     (Math/addExact (Math/multiplyExact (inc (long n))
                                                        (long charge/encoded-row-descriptor))
                                    (charge/vector-bytes (inc (long n))))))
    (when (some? by-dbi)
      (doseq [^Map$Entry by-key (.entrySet by-dbi)]
        (let [dbi (key by-key)]
          (doseq [^Map$Entry node (.entrySet ^TreeMap (val by-key))]
            (let [k (key node)
                  v (val node)]
              (.add rows (if (identical? deleted v)
                           (row-fn dbi :del k nil)
                           (row-fn dbi :put k v))))))))
    rows))

;; ---------------------------------------------------------------------------
;; Request-local views

(defn view
  "Create this descriptor's private staging view for one body run.

  The view belongs to the calling thread for exactly that body: it records its
  owner, and every transaction operation re-checks that ownership and the
  view's validity, so a body cannot pass its view to another thread or keep
  using it after it returned.

  `base` is `(fn [descriptor dbi key])`, reading one key from the native snapshot
  pinned for this preparation, or nil when the store has no native base. The
  store must charge the descriptor and return a detached copy. `check` is
  `(fn [])`, raising the batch's own pre-dispatch cancellation when this view
  observes expiry, fencing or interruption."
  ^View [^Stage stage descriptor base check]
  (View. stage descriptor base check
        (AtomicBoolean. false) (AtomicReference. nil)
        (Thread/currentThread) (AtomicBoolean. true)))

(defn rejected?
  "Whether this request was already rejected, so its writes can no longer
  become accepted."
  [^View view]
  (.get ^AtomicBoolean (.rejected? view)))

(defn mark-rejected!
  "Reject this request locally and record why. Sticky and idempotent."
  [^View view error]
  (.set ^AtomicBoolean (.rejected? view) true)
  (.compareAndSet ^AtomicReference (.rejection view) nil error)
  nil)

(defn rejection
  "This request's local rejection cause, or nil while it is still eligible."
  [^View view]
  (.get ^AtomicReference (.rejection view)))

(defn- unusable-view-error
  "The cause for using a view outside the body run that owns it.

  A view is single-threaded state on one stage, and it lives only as long as its
  own body: another thread would race the stage's owner-confined maps, and a
  returned body could still mutate this request's staging after its result was
  published and its reservation released. Both are this request's own misuse
  rather than the batch's, so it is a sticky request-local rejection."
  ^Throwable [^View view what]
  (if (not (identical? (.owner view) (Thread/currentThread)))
    (batch/request-rejection
     "A transaction view belongs to the thread running its own body"
     {:error :txlog/transaction-view-foreign-thread :retryable? false})
    (batch/request-rejection
     what {:error :txlog/transaction-view-invalidated :retryable? false})))

(defn check!
  "Re-check this view's owner thread and validity, then the batch's own cutoff
  and terminal state.

  Ownership and validity come first: they are pure mistakes by this request, so
  they must be raised even after the batch itself has been fenced. Cheap and
  lock-free, and the batch's checks raise the batch's own pre-dispatch
  cancellation rather than a request-local rejection: expiry, fencing and
  interruption cannot be attributed to one request's work."
  [^View view]
  (when-not (and (.get ^AtomicBoolean (.valid? view))
                 (identical? (.owner view) (Thread/currentThread)))
    (let [error (unusable-view-error view "A transaction view was used after its body returned")]
      (mark-rejected! view error)
      (throw error)))
  (when-let [check ^clojure.lang.IFn (.check view)]
    (check))
  nil)

(defn- invalidated!
  "Mark this view unusable for any further operation, and discard its staging."
  [^View view]
  (.set ^AtomicBoolean (.valid? view) false)
  nil)

(defn- rejected-error
  "The cause for using a view whose request was already rejected."
  ^Throwable [^View view what]
  (or (rejection view)
      (batch/request-rejection what {:error :txlog/pending-budget-exceeded
                                     :retryable? false})))

(defn tx-get
  "Read one encoded key through this request's own staging, the accepted prefix
  and then the native base. Returns nil when no key exists.

  The value is detached into a fresh copy before it is handed to the body, so a
  body can never mutate a predecessor's accepted bytes, and through them its
  already-encoded WAL row or native row. The native base charges and detaches its
  own copy before returning it; every value is charged here as well. Any charge
  failure is this request's own rejection."
  [^View view dbi key]
  (check! view)
  (let [^Stage stage (.stage view)
        descriptor (.descriptor view)
        ^TreeMap keys (private-keys stage descriptor dbi)
        staged (when (some? keys) (.get keys key))]
    (try
      (let [value (if (nil? staged)
                    (prefix-value stage (.base view) descriptor dbi key)
                    (when-not (identical? deleted staged) staged))]
        (when (some? value)
          (own-bytes descriptor value)))
      (catch Throwable t
        ;; A native-base read charges this request before it detaches the value;
        ;; a charge failure is this request's own rejection. Native errors are
        ;; left unmarked so the batch still fences.
        (when (= :txlog/pending-budget-exceeded (:error (ex-data t)))
          (mark-rejected! view t))
        (throw t)))))

(defn- staged!
  "Record one write, rejecting this request permanently if it does not fit.

  A charge failure is a request-local rejection: it is recorded on the view
  before it escapes, so catching it inside the body cannot make the request
  eligible again, and every write this request managed to stage stays private."
  [view dbi key value]
  (let [^View view view]
    (when (.get ^AtomicBoolean (.rejected? view))
      (throw (rejected-error view "Staging was used after its request was rejected")))
    (try
      (stage-write! (.stage view) (.descriptor view) dbi key value)
      (catch Throwable t
        (mark-rejected! view t)
        (throw t)))))

(defn tx-put!
  "Stage one encoded write in this request's private staging.

  A nil value is refused rather than staged: an absent value and a staged nil
  would be indistinguishable to this request's own reads, so the request is
  rejected instead of producing a row the codec cannot encode."
  [^View view dbi key value]
  (check! view)
  (when (nil? value)
    (let [error (batch/request-rejection
                 "A staged write needs a value; use tx-del! to remove a key"
                 {:error :txlog/invalid-staged-write :retryable? false})]
      (mark-rejected! view error)
      (throw error)))
  (staged! view dbi key value))

(defn tx-del!
  "Stage one encoded delete in this request's private staging."
  [^View view dbi key]
  (check! view)
  (staged! view dbi key ::del))

(defn tx-abort!
  "Reject this request explicitly, without affecting any other member.

  Sticky: catching the abort inside the body cannot make the request eligible
  again, and the view is unusable from here on, so the body cannot keep staging
  after it asked to abort."
  [^View view]
  (let [error (batch/request-rejection "Request aborted by its own body"
                                       {:error :txlog/request-aborted
                                        :retryable? true})]
    (mark-rejected! view error)
    (invalidated! view)
    (throw error)))

;; ---------------------------------------------------------------------------
;; Freezing and teardown

(defn- merge-into-prefix!
  "Publish this request's staged writes as the newest layer of the accepted
  prefix, then release its private staging."
  [^Stage stage descriptor ^LinkedHashMap by-dbi]
  (batch/charge! descriptor (merge-charge stage descriptor))
  (doseq [^Map$Entry by-key (.entrySet by-dbi)]
    (let [dbi (key by-key)
          ^TreeMap keys (val by-key)
          ^LinkedHashMap prefix (.prefix stage)
          target (or (.get prefix dbi)
                     (let [created (TreeMap. (.comparator stage))]
                       (.put prefix dbi created)
                       created))]
      (doseq [^Map$Entry node (.entrySet keys)]
        ;; `TreeMap.put` keeps the canonical key object already in the target, so
        ;; every layer shares one identity per key.
        (.put target (key node) (val node)))))
  (.remove ^IdentityHashMap (.privates stage) descriptor))

(defn accept!
  "Publish this request's staged writes as the newest prefix layer.

  Charged first, so the accepted prefix is only ever grown by a request already
  known to fit."
  [^Stage stage descriptor]
  (when-let [^LinkedHashMap by-dbi (private-dbis stage descriptor)]
    (merge-into-prefix! stage descriptor by-dbi))
  nil)

(defn fold-blind!
  "Publish one already-encoded blind row as the accepted prefix's newest layer.

  Blind requests are fully prepaid before admission, so folding their
  caller-prepared rows charges nothing: it keeps no private staging and adds no
  charge to the request that already paid for this row. The row is inserted in
  member order, its bytes are copied because the stage never aliases a caller's
  mutable arrays, and no request can be charged twice for the same row. `op` is
  `:put` or `:del`; the caller's row arrays stay the ones the WAL body and the
  native writer consume."
  [^Stage stage dbi op k v]
  (let [^LinkedHashMap prefix (.prefix stage)
        ^TreeMap target (or (.get prefix dbi)
                            (let [created (TreeMap. (.comparator stage))]
                              (.put prefix dbi created)
                              created))]
    (.put target (copy-bytes k) (if (= op :del) deleted (copy-bytes v))))
  nil)

(defn materialize!
  "Build this request's staged writes as physical rows without publishing them.

  Charged before the rows are built. The writes stay private until `accept!`, so
  a caller that must encode or validate the rows first can still discard them on
  failure instead of leaking them into the accepted prefix."
  ^java.util.List [^Stage stage descriptor row-fn]
  (let [^LinkedHashMap by-dbi (private-dbis stage descriptor)]
    (if (nil? by-dbi)
      (java.util.Collections/emptyList)
      (rows! descriptor by-dbi row-fn))))

(defn freeze!
  "Materialize this request's staged writes as physical rows and publish them as
  the newest layer of the accepted prefix.

  The rows are built and charged first, and the prefix grows only once the whole
  request is known to fit, so an allowance failure leaves both the accepted
  prefix and this request's rows unpublished. Its private staging is released,
  because its writes now belong to the prefix. Returns an empty list when this
  request staged nothing, which is what makes it a read-only member."
  ^java.util.List [^Stage stage descriptor row-fn]
  (let [rows (materialize! stage descriptor row-fn)]
    (accept! stage descriptor)
    rows))

(defn invalidate!
  "Discard this request's private staging and make its view unusable.

  Used for rejected requests, so their writes can never reach the accepted
  prefix or the physical rows, and on every body exit, so a retained view can
  never stage into a request whose result was already published. Idempotent."
  [^View view]
  (invalidated! view)
  (.remove ^IdentityHashMap (.privates (.stage view)) (.descriptor view))
  nil)

(defn release!
  "Drop every staged and accepted key reference once preparation finishes."
  [^Stage stage]
  (.clear ^LinkedHashMap (.prefix stage))
  (.clear ^IdentityHashMap (.privates stage))
  nil)