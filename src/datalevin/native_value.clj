(ns ^:no-doc datalevin.native-value
  "Runtime bindings for native values in disposable, process-local spill stores."
  (:require [taoensso.nippy :as nippy])
  (:import [datalevin NativeValue]
           [java.io DataInput DataOutput]
           [java.util HashMap]))

(def ^:dynamic *spill-bindings* nil)

(defn spill-bindings
  "Create bindings owned by one spill collection, retaining no value payloads."
  ^HashMap []
  (HashMap.))

(nippy/extend-freeze NativeValue :datalevin/native-spill
  [^NativeValue value ^DataOutput out]
  (when-not *spill-bindings*
    (throw (ex-info "Cannot serialize a native value outside its spill store"
                    {:type-name (.typeName value)})))
  (let [id       (.codecId value)
        bindings ^HashMap *spill-bindings*]
    (when-not (.containsKey bindings id)
      (.put bindings id (.withPayload value (byte-array 0))))
    (.writeUTF out id)
    (nippy/freeze-to-out! out (.payload value))))

(nippy/extend-thaw :datalevin/native-spill
  [^DataInput in]
  (let [id (.readUTF in)]
    (if-let [^NativeValue binding
             (when *spill-bindings* (.get ^HashMap *spill-bindings* id))]
      (.withPayload binding (nippy/thaw-from-in! in))
      (throw (ex-info "Missing native value binding for spill store"
                      {:codec-id id})))))
