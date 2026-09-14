;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.server.client-op-cache
  "In-flight replay coordination with expiration ordered by completion."
  (:import
   [java.util ArrayDeque]
   [java.util.concurrent ConcurrentHashMap]))

(def ^:private retain-nanos 60000000000)

(defn- ^:redef now-nanos ^long [] (System/nanoTime))

(defn create []
  {:pending (ConcurrentHashMap.) :completed (ArrayDeque.)})

(defn prune!
  "Remove expired completions without traversing live or in-flight requests.
  Each completed entry is visited once when it expires."
  [{:keys [^ConcurrentHashMap pending ^ArrayDeque completed] :as cache}]
  (locking completed
    (let [now (now-nanos)]
      (loop []
        (when-let [entry (.peekFirst completed)]
          (let [finished (long (:completed-at-nanos @(:result-promise entry)))]
            (when (>= (unchecked-subtract now finished) (long retain-nanos))
              (.removeFirst completed)
              (.remove pending (get-in entry [:request :client-op-id]) entry)
              (recur)))))))
  cache)

(defn complete!
  "Publish a result and queue its expiration. In-flight entries are never
  queued. Taking the timestamp under the queue lock preserves completion order."
  [{:keys [^ArrayDeque completed]} entry result]
  (locking completed
    (deliver (:result-promise entry)
             (assoc result :completed-at-nanos (now-nanos)))
    (.addLast completed entry))
  nil)
