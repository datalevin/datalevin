;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.resources
  "Resource ownership during server initialization and cleanup.")

(defn close-suppressing!
  "Attempt cleanup without replacing the original failure."
  [^Throwable failure close!]
  (try
    (close!)
    (catch Throwable cleanup-error
      (when-not (identical? failure cleanup-error)
        (.addSuppressed failure cleanup-error)))))

(defn with-acquired
  "Call f with (own! resource close!). On failure, close acquired resources
  in reverse order. On success, ownership transfers to the caller."
  [f]
  (let [cleanups (volatile! ())]
    (try
      (f (fn [resource close!]
           (vswap! cleanups conj #(close! resource))
           resource))
      (catch Throwable t
        (doseq [close! @cleanups]
          (close-suppressing! t close!))
        (throw t)))))

(defn close-all!
  "Attempt every cleanup and report the first failure with suppressed errors."
  [cleanups]
  (let [failure (volatile! nil)]
    (doseq [close! cleanups]
      (if-let [t @failure]
        (close-suppressing! t close!)
        (try (close!) (catch Throwable t (vreset! failure t)))))
    (when-let [t @failure] (throw t))))
