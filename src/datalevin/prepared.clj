;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.prepared
  "Reusable operations. A preparation owns metadata, never a transaction
  or a borrowed buffer."
  (:import [clojure.lang IFn]
           [java.util LinkedHashMap]
           [java.util.concurrent.atomic AtomicLong]))

(deftype PreparedRead [run view-run]
  IFn
  (invoke [_ input] (run input))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defn prepared-read
  ([run] (PreparedRead. run nil))
  ([run view-run] (PreparedRead. run view-run)))

(deftype PreparedWrite [run scope-run]
  IFn
  (invoke [_ input] (run input nil))
  (invoke [_ input tx-meta] (run input tx-meta))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))

(defn prepared-write [run scope-run]
  (PreparedWrite. run scope-run))

(defn execute
  "Execute a prepared operation with its key, entity identifier or query inputs.
  The three-argument arity supplies an explicit view or connection. Reads and KV
  updates accept local views; Datalog writes accept local or remote transaction
  connections, so one preparation can be reused across transactions."
  ([prepared input]
   (if (instance? PreparedWrite prepared)
     (.invoke ^PreparedWrite prepared input)
     (.invoke ^PreparedRead prepared input)))
  ([prepared view input]
   (if (instance? PreparedWrite prepared)
     ((.-scope-run ^PreparedWrite prepared) view input nil)
     (if-some [view-run (.-view-run ^PreparedRead prepared)]
       (view-run view input)
       (throw (ex-info "Prepared read does not support an explicit view"
                       {:type :prepared/no-view})))))
  ([prepared view input tx-meta]
   (if (instance? PreparedWrite prepared)
     ((.-scope-run ^PreparedWrite prepared) view input tx-meta)
     (throw (ex-info "Transaction metadata requires a prepared write"
                     {:type :prepared/not-write})))))

(def ^:const max-handles 256)

(defn handle-cache
  "Bounded by remember!, and accessed only by its owning connection."
  ^LinkedHashMap []
  (LinkedHashMap. 16 0.75 true))

(defn remember! [^LinkedHashMap cache id value]
  (.put cache id value)
  (when (> (.size cache) max-handles)
    (let [iterator (.iterator (.keySet cache))]
      (.next iterator)
      (.remove iterator)))
  value)

(defonce ^:private ^AtomicLong ids (AtomicLong.))

(deftype Request [^long id args metadata])

(defn request
  "The changing input of a prepared wire request is at argument index 2."
  [args]
  (let [id (.incrementAndGet ids)]
    (Request. id args {::id id})))
