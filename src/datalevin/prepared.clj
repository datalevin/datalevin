;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.prepared
  "Reusable read operations. A prepared read owns metadata, never a transaction
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

(defn execute
  "Execute a prepared read with its key, entity identifier or query inputs.
  The three-argument arity executes a local preparation against an explicit
  view, such as a transaction's DB view or KV handle, so one preparation can
  be reused across transactions."
  ([^PreparedRead prepared input]
   (.invoke prepared input))
  ([^PreparedRead prepared view input]
   (if-some [view-run (.-view-run prepared)]
     (view-run view input)
     (throw (ex-info "Prepared read does not support an explicit view"
                     {:type :prepared/no-view})))))

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
  "The changing argument of point reads, pulls and queries is at wire index 2."
  [args]
  (let [id (.incrementAndGet ids)]
    (Request. id args {::id id})))
