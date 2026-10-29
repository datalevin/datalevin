(ns datalevin.query.source
  "Embedded query-source SPI v2, introduced in Datalevin 1.2.8.

  Sources are caller-owned. Sessions and owned-batch cursors belong to one
  invocation. See doc/query-source-spi.md for the current contract and the
  Datalevin 1.2.9 requirements for fusion and boxed-integer interoperability.
  Optional whole-conjunction relational protocols are available in 1.2.10."
  (:require [datalevin.query.dispatch :as dispatch]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.schema :as schema]
            [datalevin.query.source.value :as value])
  (:import [java.lang ClassValue]))

(defprotocol IQuerySource
  (-source-info [source] "Return immutable metadata without provider I/O.")
  (-open-session [source request] "Authorize and acquire one read view.")
  (-plan-source [source session request] "Plan all eligible clauses without reads."))

(defn- source-predicate [protocol]
  (let [classes (vec (keep (fn [[type methods]] (when (and type methods) type)) (:impls protocol)))
        nil-source? (boolean (get-in protocol [:impls nil]))]
    (cond
      (and (empty? classes) (not nil-source?))
      (fn [input] (instance? datalevin.query.source.IQuerySource input))
      (= 1 (count classes))
      ;; A single class extension is common for an adapter. Class.isInstance
      ;; avoids a ClassValue lookup for every unrelated scalar native input.
      (let [^Class type (first classes)]
        (fn [input]
          (or (instance? datalevin.query.source.IQuerySource input)
              (.isInstance type input) (and nil-source? (nil? input)))))
      :else
      ;; Negative satisfies? walks the class hierarchy. Cache both outcomes;
      ;; ClassValue keeps arbitrary caller classes from being retained forever.
      (let [checks (proxy [ClassValue] [] (computeValue [_] (volatile! nil)))]
        (fn [input]
          (or (instance? datalevin.query.source.IQuerySource input)
              (if (nil? input) nil-source?
                  (let [result (.get ^ClassValue checks (class input))]
                    (if-some [known @result] known
                             (vreset! result (satisfies? protocol input)))))))))))

;; extend/extend-type updates the protocol Var. Publish a fresh predicate on
;; those rare updates, so native calls need no protocol-map lookup or cache
;; invalidation check. Var root updates use the same monitor as installation.
(locking #'IQuerySource
  (add-watch #'IQuerySource ::source-routing
             (fn [_ _ _ protocol]
               (dispatch/install-source-predicate! (source-predicate protocol) protocol)))
  (dispatch/install-source-predicate! (source-predicate IQuerySource) IQuerySource))

(defprotocol IQuerySourceSession
  (-session-info [session] "Return the acquired read-view metadata.")
  (-open-cursor [session request]
    "Open a selected plan in this session's view. Request is common context
    plus :alternative-id, :plan, :source-symbol, :definition-fingerprint,
    :scope-id and :region-id; dependent opens also contain :bindings;
    planning inputs remain in the session-owned plan handle.")
  (-cancel-session! [session reason] "Signal cancellation promptly; return nil.")
  (-close-session! [session] "Release this session and its cursors; return nil."))

(defprotocol IQuerySourceCursor
  (-next-batch! [cursor request] "Return one owned, bounded batch.")
  (-cursor-metrics [cursor] "Return cumulative metrics, including after close.")
  (-close-cursor! [cursor] "Release cursor resources idempotently; return nil."))

(defprotocol IRelationalQuerySource
  (-plan-relational [source request]
    "Optional pure planning of an entire flat relational request. Return nil
    to use fact accesses, or {:plan owned-value :columns request-columns}.
    Plans contain only owned value trees, never resources or callbacks.
    No provider I/O or session acquisition is permitted here."))

(defprotocol IRelationalQuerySession
  (-open-relational-cursor [session request]
    "Open a selected relational plan under the acquired authorization and
    snapshot. Return an ordinary owned-batch IQuerySourceCursor. Implementers
    validate plan scope against this session; SQL is never part of the SPI."))

(defn validate-source-info!
  "Validate a descriptor without acquisition; return it unchanged."
  [info]
  (schema/source-info! info))

(defn validate-type!
  "Validate a logical type descriptor; return it unchanged."
  [descriptor]
  (contract/type! descriptor))

(defn cancelled?
  "Return whether this invocation has been cancelled."
  [control] (control/cancelled? control))
(defn remaining-time-ms
  "Return nonnegative remaining time, or nil when there is no deadline."
  [control] (control/remaining-time-ms control))
(defn check-active!
  "Throw on cancellation, timeout, or a closed invocation; otherwise return nil."
  [control] (control/check-active! control))
(defn register-cancel!
  "Register a prompt cancellation callback, returning an opaque registration."
  [control cancel-fn]
  (control/register-cancel! control cancel-fn))
(defn unregister-cancel!
  "Remove a registration idempotently. An already dispatched callback may run."
  [registration]
  (control/unregister-cancel! registration))
(defn reserve!
  "Atomically reserve all requested resource charges, or throw without charging."
  [control charges] (control/reserve! control charges))
(defn release!
  "Release live charges idempotently; cumulative external requests are not refunded."
  [reservation] (control/release! reservation))

(defn request-limiter
  "Create a Closeable request limiter once per session. Share it across every
  alias and independent consumption group, including during session setup."
  [control constraints]
  (permissions/request-limiter control constraints))

(defn acquire-request!
  "Wait for an actual backend request permit under the shared query ledger,
  session concurrency, rolling rate, cancellation, and deadline. Close the
  returned permit in finally, including on provider errors. Pages, retries,
  and split requests each acquire a separate permit."
  [limiter]
  (permissions/acquire-request! limiter))
(defn batch-charge-bytes
  "Conservative retained charge for owned values. Custom values must implement
  datalevin.query.source.value/IQuerySourceValue; unknown values reject. The
  two-argument input-free shape allocates no ID state. Dependent batches supply
  one binding ID per row, including [] on an empty final batch."
  ([columns rows] (value/batch-charge-bytes columns rows))
  ([columns rows binding-ids]
   (value/dependent-batch-charge-bytes columns rows binding-ids)))

(defn binding-charge-bytes
  "Conservative retained charge for a nonempty dependent open's typed, unique
  binding tuples and IDs. Input-free opens omit bindings and never call this."
  [requires bindings]
  (contract/binding-columns! requires)
  (value/binding-charge-bytes requires bindings))
