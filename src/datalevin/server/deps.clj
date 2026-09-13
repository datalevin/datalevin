;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.deps
  "Wiring and validation helpers for server dependency maps.

  Dependency entries are stored as Vars rather than captured function
  values so that every consumer observes the same replacement behavior:
  redefining a var (including through `with-redefs`) is picked up at call
  time. Non-callback entries (maps, sets, ...) are stored as Vars too and
  read through `value`, so the rule is uniform across the whole map.

  A dependency contract is a map with two optional keys:

    :callbacks  a collection of keys that must be present and hold a
                callable value once resolved
    :values     a map of key -> predicate for non-callback entries

  `validate` is meant to run when a dependency map is constructed, so a
  missing or mistyped wire-up fails immediately with an actionable error
  instead of a confusing NPE deep in a request handler."
  (:require
   [datalevin.util :refer [raise]]))

(defn value
  "Resolve dependency `k` in `deps`, dereferencing a Var entry. Plain
  values are returned untouched so hand-built maps (tests, REPL) keep
  working."
  [deps k]
  (let [v (get deps k)]
    (if (var? v) @v v)))

(defn- present?
  [deps k]
  (contains? deps k))

(defn validate
  "Validate `deps` against `contract`, returning `deps` unchanged on
  success. `label` (usually a qualified keyword) names the contract in the
  raised error. Throws `ex-info` with `:error :server/deps-invalid` when
  keys are missing, callbacks are not callable, value entries fail their
  predicate, or (when `:strict?` is true) unexpected keys are present."
  ([label contract deps]
   (validate label contract deps {}))
  ([label contract deps {:keys [strict?]}]
   (let [{:keys [callbacks values]} contract
         callbacks  (vec callbacks)
         missing    (into []
                          (remove #(present? deps %))
                          (concat callbacks (keys values)))
         not-fn     (into []
                          (filter #(not (ifn? (value deps %))))
                          callbacks)
         invalid    (into []
                          (keep (fn [[k pred]]
                                  (when (and (present? deps k)
                                             (not (pred (value deps k))))
                                    [k (some-> (value deps k) class str)])))
                          values)
         unexpected (when strict?
                      (into []
                            (remove (set (concat callbacks (keys values))))
                            (keys deps)))]
     (when (or (seq missing) (seq not-fn) (seq invalid) (seq unexpected))
       (raise "Invalid server dependency map"
                {:error      :server/deps-invalid
                 :label      label
                 :missing    missing
                 :not-fn     not-fn
                 :invalid    invalid
                 :unexpected unexpected}))
     deps)))
