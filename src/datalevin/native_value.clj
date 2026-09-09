(ns ^:no-doc datalevin.native-value
  "Native value snapshots for process-local spills and explicit wire readers."
  (:require [taoensso.nippy :as nippy])
  (:import [datalevin NativeValue]
           [java.io DataInput DataOutput]
           [java.util HashMap]))

(def ^:dynamic *spill-bindings* nil)
(def ^:dynamic *wire-native-value* false)
(def ^:dynamic *wire-reader*
  "Receiver-owned function of [type-name payload-bytes], returning a logical value."
  nil)

(defn spill-bindings
  "Create bindings owned by one spill collection, retaining no value payloads."
  ^HashMap []
  (HashMap.))

(defn- wire-native-value?
  [x]
  (and (vector? x)
       (= 2 (count x))
       (string? (first x))
       (bytes? (second x))))

(nippy/extend-freeze NativeValue :datalevin/native-spill
  [^NativeValue value ^DataOutput out]
  (let [id      (.codecId value)
        payload (.payload value)]
    (if *wire-native-value*
      (do
        ;; Runtime codec IDs have meaning only inside the sending process.
        (.writeUTF out "")
        (nippy/freeze-to-out! out [(.typeName value) payload]))
      (do
        (when-not (and *spill-bindings* (not-empty id))
          (throw (ex-info "Cannot serialize a native value outside its spill store"
                          {:type-name (.typeName value)})))
        (let [bindings ^HashMap *spill-bindings*]
          (when-not (.containsKey bindings id)
            (.put bindings id (.withPayload value (byte-array 0))))
          (.writeUTF out id)
          (nippy/freeze-to-out! out payload))))))

(nippy/extend-thaw :datalevin/native-spill
  [^DataInput in]
  (let [id    (.readUTF in)
        data  (nippy/thaw-from-in! in)]
    (if (empty? id)
      (do
        (when-not (and *wire-native-value* (wire-native-value? data))
          (throw (ex-info "Malformed native wire value or wrong decoding context"
                          {:codec-id id})))
        (when-not *wire-reader*
          (throw (ex-info "Missing receiver native type binding"
                          {:type-name (first data)})))
        (*wire-reader* (first data) (second data)))
      (if-let [^NativeValue binding
               (when (and (not *wire-native-value*) *spill-bindings*)
                 (.get ^HashMap *spill-bindings* id))]
        (if (bytes? data)
          (.withPayload binding data)
          (throw (ex-info "Malformed native value payload in spill binding mode"
                          {:codec-id id
                           :payload-type (type data)})))
        (throw (ex-info "Malformed native value or missing spill binding"
                        {:codec-id id :payload data}))))))
