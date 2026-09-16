;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.protocol.context
  "Reusable wire codec state owned by an exclusively used connection.
  Nippy references remain local to a single message, including failed messages."
  (:require [datalevin.bits :as b]
            [datalevin.native-value :as nv]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl])
  (:import [taoensso.nippy.impl CacheState]
           [java.util Map List]))

(def ^:dynamic *context* nil)

(defprotocol ICodecContext
  (bindings [context])
  (wire-bindings [context mode allowlist])
  (acquire-cache! [context])
  (release-cache! [context cache]))

(defn- cache-size ^long [^CacheState cache]
  (max (max (.size ^Map (.-freeze-idxs cache)) (.size ^Map (.-kw-idxs cache)))
       (max (.size ^List (.-thaw-vals cache))
            (max (.size ^Map (.-seen-kws cache)) (.size ^List (.-seen-log cache))))))

(defn- clear-cache! [^CacheState cache]
  ;; Nippy 3.9 CacheState: clear references as well as the two dictionary flags.
  (.clear ^Map (.-freeze-idxs cache))
  (when-not (.isEmpty ^Map (.-kw-idxs cache))
    (.clear ^Map (.-kw-idxs cache)))
  (.clear ^List (.-thaw-vals cache))
  (when-not (.isEmpty ^Map (.-seen-kws cache))
    (.clear ^Map (.-seen-kws cache)))
  (.clear ^List (.-seen-log cache))
  (aset-long (.-mut cache) 0 0)
  (aset-long (.-mut cache) 1 0))

(deftype CodecContext [^:unsynchronized-mutable ^CacheState cache
                      ^:unsynchronized-mutable active?
                      ^:unsynchronized-mutable allowlist
                      ^:unsynchronized-mutable binding-map
                      ^:unsynchronized-mutable freeze-binding-map
                      ^:unsynchronized-mutable thaw-binding-map]
  ICodecContext
  (bindings [this]
    ;; Security policy follows the current caller, even when a pool connection
    ;; moves between threads or databases. Do not capture native readers here.
    (let [current (b/serialization-allowlist)]
      (when (or (nil? binding-map) (not (identical? current allowlist)))
        (set! allowlist current)
        (set! binding-map {#'*context* this
                          #'nv/*wire-native-value* true
                          #'nippy/*freeze-serializable-allowlist* current
                          #'nippy/*thaw-serializable-allowlist* current}))
      binding-map))
  (wire-bindings [_ mode current]
    ;; Server threads keep only *context* bound across requests. Wire mode and
    ;; Java serialization policy apply to the codec, never to handler/storage work.
    (case mode
      :freeze
      (do
        (when (or (nil? freeze-binding-map)
                  (not (identical? current
                                   (get freeze-binding-map
                                        #'nippy/*freeze-serializable-allowlist*))))
          (set! freeze-binding-map
                {#'nv/*wire-native-value* true
                 #'nippy/*freeze-serializable-allowlist* current}))
        freeze-binding-map)
      :thaw
      (do
        (when (or (nil? thaw-binding-map)
                  (not (identical? current
                                   (get thaw-binding-map
                                        #'nippy/*thaw-serializable-allowlist*))))
          (set! thaw-binding-map
                {#'nv/*wire-native-value* true
                 #'nippy/*thaw-serializable-allowlist* current}))
        thaw-binding-map)))
  (acquire-cache! [_]
    (if active?
      ;; A custom serializer can recursively encode another wire message.
      (impl/new-cache-state)
      (do
        (when (or (nil? cache)
                  (not (identical? nippy/*shared-dict* (.-dict cache))))
          (set! cache (impl/new-cache-state)))
        (set! active? true)
        cache)))
  (release-cache! [_ state]
    (when (identical? cache state)
      (if (> (cache-size cache) 4096)
        ;; Do not retain oversized backing arrays after an exceptional frame.
        (set! cache nil)
        (clear-cache! cache))
      (set! active? false))))

(defn create [] (CodecContext. nil false nil nil nil nil))

(defmacro with-wire-bindings [mode allowlist & body]
  `(let [allowlist# ~allowlist
         bindings# (if-let [context# *context*]
                     (wire-bindings context# ~mode allowlist#)
                     {#'nv/*wire-native-value* true
                      ~(case mode
                         :freeze `(var nippy/*freeze-serializable-allowlist*)
                         :thaw `(var nippy/*thaw-serializable-allowlist*))
                      allowlist#})]
     (clojure.lang.Var/pushThreadBindings bindings#)
     (try ~@body (finally (clojure.lang.Var/popThreadBindings)))))

(defmacro with-context [context & body]
  `(do
     (clojure.lang.Var/pushThreadBindings (bindings ~context))
     (try ~@body (finally (clojure.lang.Var/popThreadBindings)))))

(defmacro with-cache [fresh-cache & body]
  `(let [context# *context*]
     (if context#
       (let [cache# (acquire-cache! context#)]
         (try
           (impl/with-cache* cache# ~@body)
           (finally (release-cache! context# cache#))))
       (impl/with-cache* ~fresh-cache ~@body))))
