;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.json-api
  (:require
   [datalevin.client :as dc]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.json-api.shared :as shared]))

(def ^:const json-api-version
  shared/json-api-version)

(def default-limits
  shared/default-limits)

(def ^:dynamic *json-api-limits*
  default-limits)

(defn ^:no-doc new-session-state
  []
  (shared/new-session-state))

(defn- register!
  ([prefix type obj]
   (shared/register! prefix type obj))
  ([session prefix type obj]
   (shared/register! session prefix type obj)))

(defn- rebind!
  ([h type new-obj]
   (shared/rebind! h type new-obj))
  ([session h type new-obj]
   (shared/rebind! session h type new-obj)))

(defn- current-session-state
  []
  (or shared/*session-state*
      (var-get #'shared/default-session-state)))

(defn- close-handle-resource!
  [{:keys [type obj]}]
  (case type
    :conn          (when-not (d/closed? obj)
                     (d/close obj))
    :kv            (when-not (d/closed-kv? obj)
                     (d/close-kv obj))
    :vec           (when-not (i/vec-closed? obj)
                     (d/close-vector-index obj))
    :client        (when-not (dc/disconnected? obj)
                     (dc/disconnect obj))
    :search-writer nil
    :search        nil
    nil))

(defn- release!
  ([h]
   (release! (current-session-state) h))
  ([session h]
   (let [handles ^java.util.Map (:handles session)]
     (when-let [entry (.get handles h)]
       (shared/unregister! session h)
       (close-handle-resource! entry))
     true)))

(defn- unregister!
  ([h]
   (shared/unregister! h))
  ([session h]
   (shared/unregister! session h)))

(defn- resolve-entry
  ([h]
   (shared/resolve-entry h))
  ([session h]
   (shared/resolve-entry session h)))

(defn- resolve-handle
  ([h]
   (shared/resolve-handle h))
  ([session h]
   (shared/resolve-handle session h)))

(defn- resolve-typed-handle
  [h expected-type]
  (let [{:keys [type obj]} (shared/resolve-entry h)]
    (if (= type expected-type)
      obj
      (throw (ex-info (str "Invalid or expired handle: " h)
                      (cond-> {:code   :invalid-handle
                               :handle h}
                        expected-type (assoc :expected expected-type)
                        type (assoc :actual type)))))))

(defn- resolve-conn
  [h]
  (resolve-typed-handle h :conn))

(defn- resolve-kv
  [h]
  (resolve-typed-handle h :kv))

(defn- resolve-client
  [h]
  (resolve-typed-handle h :client))

(defn- resolve-search
  [h]
  (resolve-typed-handle h :search))

(defn- resolve-search-writer
  [h]
  (resolve-typed-handle h :search-writer))

(defn- resolve-vec
  [h]
  (resolve-typed-handle h :vec))

(defn ^:no-doc clear-handles!
  ([]
   (clear-handles! (current-session-state)))
  ([session]
   (let [handles ^java.util.Map (:handles session)]
     (doseq [h (vec (.keySet handles))]
       (release! session h))
     true)))

(defn- with-limits
  [f]
  (binding [shared/*json-api-limits* *json-api-limits*]
    (f)))

(defn ^:no-doc exec-request
  ([request]
   (with-limits #(shared/exec-request request)))
  ([session request]
   (with-limits #(shared/exec-request session request))))

(defn exec
  [^String json]
  (with-limits #(shared/exec json)))
