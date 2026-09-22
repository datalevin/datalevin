;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.prepared
  "Connection-local prepared reads. Expansion precedes normal authorization,
  admission and transaction routing; readers are installed after those checks."
  (:require [datalevin.kv :as kv]
            [datalevin.prepared :as prepared]
            [datalevin.pull-api :as pull]
            [datalevin.pull-wire :as wire]
            [datalevin.query :as q]
            [datalevin.util :refer [raise]])
  (:import [java.nio.channels SelectionKey]
           [java.util LinkedHashMap]))

(deftype Entry [type args reader response-writer])

(defn- enabled? [^SelectionKey skey]
  (true? (get-in @(.attachment skey) [:wire-opts :prepared-read?])))

(defn- check-id! [id]
  (when-not (and (integer? id) (<= 1 id Long/MAX_VALUE))
    (raise "Invalid prepared read handle" {:error :prepared/invalid-handle})))

(defn expand-message
  "Restore routing fields from this connection's handle. Ignore caller-supplied
  preparation metadata. Deferred native requests are expanded again after
  authorized decoding supplies their actual key/entity identifier."
  [^SelectionKey skey message]
  (let [message (if (contains? (meta message) ::entry)
                  (vary-meta message dissoc ::entry)
                  message)]
    (if (= :execute-prepared (:type message))
      (let [id (:handle message)
            _ (check-id! id)
            ^LinkedHashMap cache (:prepared-handles @(.attachment skey))
            ^Entry entry (when (and (enabled? skey) cache) (.get cache id))]
        (when-not entry
          (raise "Prepared read handle is not registered on this connection"
                 {:error :prepared/missing :handle id}))
        (with-meta
          (cond-> {:type (.-type entry)
                   :args (assoc (.-args entry) 2 (:value message))
                   :writing? (:writing? message)}
            (:ha-read-min-tx message)
            (assoc :ha-read-min-tx (:ha-read-min-tx message)))
          (assoc (meta message) ::entry entry)))
      message)))

(defn reader!
  "Return a prepared reader, registering fixed arguments on a cold execution.
  Call only inside the original operation's authorized handler."
  [^SelectionKey skey {:keys [type args prepare-id] :as message}]
  (if-let [^Entry entry (::entry (meta message))]
    (.-reader entry)
    (when (and prepare-id (enabled? skey))
      (check-id! prepare-id)
      (when-not (case type
                  :get-value (= 6 (count args))
                  :pull (= 4 (count args))
                  :q (and (= 3 (count args))
                          (true? (get-in @(.attachment skey)
                                         [:wire-opts :prepared-query?])))
                  false)
        (raise "Unsupported prepared read" {:error :prepared/unsupported :type type}))
      (let [response-writer (when (and (= type :pull)
                                       (true? (get-in @(.attachment skey)
                                                      [:wire-opts :prepared-pull?])))
                              (wire/writer prepare-id))
            reader (case type
                     :get-value (kv/value-reader (nth args 1) (nth args 3)
                                                 (nth args 4) (nth args 5))
                     :pull (pull/pull-reader (nth args 1) (nth args 3) response-writer)
                     :q (q/query-reader (nth args 1)))
            attachment (.attachment skey)
            ^LinkedHashMap cache (or (:prepared-handles @attachment)
                                     (let [cache (prepared/handle-cache)]
                                       (vswap! attachment assoc :prepared-handles cache)
                                       cache))]
        (prepared/remember! cache prepare-id
                            (Entry. type (assoc args 2 nil) reader response-writer))
        reader))))

(defn response-written!
  "Acknowledge a layout only after writing the complete response frame."
  [^SelectionKey skey message]
  (when-let [^Entry entry (or (::entry (meta message))
                              (when-let [id (:prepare-id message)]
                                (when-let [^LinkedHashMap cache
                                           (:prepared-handles @(.attachment skey))]
                                  (.get cache id))))]
    (when-let [writer (.-response-writer entry)]
      (wire/response-written! writer))))
