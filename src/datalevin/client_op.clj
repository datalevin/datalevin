;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.client-op
  "Helpers for idempotent remote client write retries.

  Persisted record names and key prefixes stay unchanged for wire and store
  compatibility."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.lmdb :as l]
   [datalevin.native-value :as nv]
   [datalevin.util :as u])
  (:import
   [datalevin NativeValue]
   [java.security MessageDigest]
   [java.util UUID]))

(def ^:const kv-info-key-prefix "__ha-client-op__/")

(def ^:const tx-data-response-kind :tx-data)
(def ^:const tx-data+db-info-response-kind :tx-data+db-info)
(def ^:const kv-result-response-kind :kv-result)
(def ^:const command-complete-response-kind :command-complete)

(defn new-client-op-id
  []
  (str (UUID/randomUUID)))

(defn request-hash
  [payload]
  (let [^MessageDigest md (MessageDigest/getInstance "SHA-256")
        ^bytes payload-bytes (binding [nv/*wire-native-value* true]
                               (b/serialize payload))]
    (.update md payload-bytes)
    (u/hexify (.digest md))))

(defn tx-request-payload
  [request-type db-name txs simulated?]
  [request-type db-name simulated? (vec txs)])

(defn kv-request-payload
  [db-name dbi-name txs k-type v-type]
  [:transact-kv db-name dbi-name k-type v-type (vec txs)])

(defn kv-info-key
  [client-op-id]
  (str kv-info-key-prefix client-op-id))

(defn committed-record
  [request-type request-hash response-kind response]
  (cond-> {:version         1
           :request-type    request-type
           :request-hash    request-hash
           :response-kind   response-kind
           :response        response
           :completed-at-ms (System/currentTimeMillis)}
    ;; Native values are complete custom attribute values. Keep their response
    ;; snapshots as wire bytes; persisted records must not retain runtime IDs.
    (some #(instance? NativeValue (if (d/datom? %) (d/datom-v %) (nth % 2)))
          (:tx-data response))
    (-> (assoc :version 2
               :response-wire (binding [nv/*wire-native-value* true]
                                (b/serialize response)))
        (dissoc :response))))

(defn record-response
  "Restore a saved native response using the current database's receiver."
  [record]
  (if-let [payload (:response-wire record)]
    (binding [nv/*wire-native-value* true] (b/deserialize payload))
    (:response record)))

(defn committed-record-tx
  [client-op-id record]
  (l/kv-tx :put c/ha-client-ops (kv-info-key client-op-id) record :string :data))

(defn tx-meta
  [client-op-id request-type request-hash response-kind]
  {:client-op/id            client-op-id
   :client-op/request-type  request-type
   :client-op/hash          request-hash
   :client-op/response-kind response-kind})
