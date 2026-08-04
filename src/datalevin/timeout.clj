;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.timeout
  "Timeout for Datalog query processing")

(def ^:dynamic *deadline*
  "When non nil, query pr pull will throw if its not done before *deadline*
  -- as returned by (System/currentTimeMillis) or (.now js/Date)"
  nil)

(defn to-deadline
  "Converts a timeout in milliseconds (or nil) to a deadline (or nil)."
  [timeout-in-ms]
  (some-> timeout-in-ms
          (#(+ ^long % ^long (System/currentTimeMillis)))))

(defn ^:no-doc effective-deadline
  "Return the earliest of the currently bound deadline and a query-local
  timeout. A nested query without its own timeout inherits its parent's
  deadline, and an explicit nested timeout cannot extend the parent deadline."
  [timeout-in-ms]
  (let [local-deadline (to-deadline timeout-in-ms)]
    (cond
      (nil? *deadline*)     local-deadline
      (nil? local-deadline) *deadline*
      :else                 (min (long *deadline*)
                                 (long local-deadline)))))

(defn time-left
  "Returns the remaining timeout in milliseconds, or nil when no deadline is
  bound."
  []
  (when *deadline*
    (max 0 (- ^long *deadline* ^long (System/currentTimeMillis)))))

(defn assert-time-left
  "Throws if timeout exceeded"
  []
  (when (some-> *deadline*
                (#(<= ^long % ^long (System/currentTimeMillis))))
    (throw
      (ex-info "Query and/or pull expression took too long to run."
               {:type :query/timeout
                :deadline *deadline*}))))
