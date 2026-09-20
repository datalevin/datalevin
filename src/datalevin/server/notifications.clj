;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.notifications
  "Ephemeral, coalescing database change notifications."
  (:import [java.util UUID]))

(defn topic [] (atom (UUID/randomUUID)))

(defn publish!
  "Wake every waiter after a committed change. Retain only the latest token."
  [topic]
  (locking topic
    (reset! topic (UUID/randomUUID))
    (.notifyAll ^Object topic))
  nil)

(defn await-change
  "Return the current token, waiting at most timeout-ms if it has not changed.
  A nil token establishes a subscription without reporting historical writes."
  [topic token timeout-ms]
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (locking topic
      (loop []
        (let [current @topic
              remaining (- deadline (System/nanoTime))]
          (if (or (nil? token) (not= token current) (<= remaining 0))
            {:token current :changed? (and (some? token) (not= token current))}
            (do
              (.wait ^Object topic (quot remaining 1000000)
                     (int (rem remaining 1000000)))
              (recur))))))))
