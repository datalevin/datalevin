;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-group.phase
  "Shared deterministic fault/trace seam for the new write protocol.

  Deliberately independent of `datalevin.tx-state`, so tracing the additive
  collector never loads the replaced pending engine. A disabled seam allocates
  no event payload: the default body is one root volatile dereference, and the
  observer is only invoked when a trace is installed.")

(defonce ^:private observer (volatile! nil))

(defn ^:redef phase!
  "Record one execution phase. Disabled production calls allocate nothing."
  [event context]
  (when-let [f @observer] (f event context)))

(defn observe!
  "Install a trace/fault observer and return a zero-argument uninstaller.

  Observers see the phase events the new collector emits: caller preparation,
  ready publication, batch sealing, ordered work, schedule selection, branch
  dispatch/completion, join and next activation. They must be cheap and must
  not retain the context passed to them."
  [f]
  (let [previous @observer]
    (vreset! observer f)
    (fn []
      (vreset! observer previous)
      nil)))

(defn observed?
  "Whether a trace observer is installed. Diagnostics only."
  []
  (some? @observer))
