;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2.0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.ha.client-op
  "Compatibility wrapper for `datalevin.client-op`."
  (:require [datalevin.client-op :as cop]))

(def ^:const kv-info-key-prefix cop/kv-info-key-prefix)

(def ^:const tx-data-response-kind cop/tx-data-response-kind)
(def ^:const tx-data+db-info-response-kind cop/tx-data+db-info-response-kind)
(def ^:const kv-result-response-kind cop/kv-result-response-kind)
(def ^:const command-complete-response-kind cop/command-complete-response-kind)

(defn new-client-op-id
  []
  (cop/new-client-op-id))

(defn request-hash
  [payload]
  (cop/request-hash payload))

(defn tx-request-payload
  [request-type db-name txs simulated?]
  (cop/tx-request-payload request-type db-name txs simulated?))

(defn kv-request-payload
  [db-name dbi-name txs k-type v-type]
  (cop/kv-request-payload db-name dbi-name txs k-type v-type))

(defn kv-info-key
  [client-op-id]
  (cop/kv-info-key client-op-id))

(defn committed-record
  [request-type request-hash response-kind response]
  (cop/committed-record request-type request-hash response-kind response))

(defn committed-record-tx
  [client-op-id record]
  (cop/committed-record-tx client-op-id record))

(defn tx-meta
  [client-op-id request-type request-hash response-kind]
  (cop/tx-meta client-op-id request-type request-hash response-kind))
