;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.util
  "Shared HA helper functions used across runtime helper namespaces."
  (:require
   [clojure.string :as s]
   [taoensso.timbre :as log])
  (:import
   [java.util.concurrent ExecutorService TimeUnit]))

(defn long-max2 ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) a b)))

(defn nonnegative-long-diff ^long
  [a b]
  (let [a (long a)
        b (long b)]
    (if (> a b) (- a b) 0)))

(defn ha-thread-label
  [db-name]
  (when (some? db-name)
    (let [label (-> (str db-name)
                    (s/replace #"[^A-Za-z0-9._-]+" "-")
                    (s/replace #"(^-+|-+$)" ""))]
      (when-not (s/blank? label)
        label))))

(defn shutdown-ha-executor!
  [^ExecutorService executor description context]
  (when executor
    (try
      (.shutdown executor)
      (when-not (.awaitTermination executor 100 TimeUnit/MILLISECONDS)
        (.shutdownNow executor))
      (catch InterruptedException e
        (.interrupt (Thread/currentThread))
        (.shutdownNow executor)
        (log/warn e (str "Interrupted while stopping " description) context))
      (catch Throwable e
        (log/warn e (str "Failed to stop " description) context)))))
