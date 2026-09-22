;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.protocol.context
  "Reusable wire codec state owned by an exclusively used connection.
  Nippy references remain local to a single message, including failed messages."
  (:require [datalevin.bits :as b]
            [datalevin.native-value :as nv]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.impl :as impl])
  (:import [clojure.lang Associative ISeq]
           [datalevin.io WireCompression]
           [taoensso.nippy.impl CacheState]
           [java.util Map List LinkedHashMap]))

(def ^:dynamic *context* nil)

(deftype ^:no-doc CachedBindings [^Associative bindings ^ISeq entries]
  Associative
  (seq [_] entries)
  (count [_] (.count bindings))
  (empty [_] (.empty bindings))
  (cons [_ value] (.cons bindings value))
  (equiv [_ other] (.equiv bindings other))
  (containsKey [_ key] (.containsKey bindings key))
  (entryAt [_ key] (.entryAt bindings key))
  (assoc [_ key value] (.assoc bindings key value))
  (valAt [_ key] (.valAt bindings key))
  (valAt [_ key not-found] (.valAt bindings key not-found)))

(defn cached-bindings
  "Cache the immutable entry traversal of a reused thread-binding map.
  Var/pushThreadBindings still creates fresh binding cells on every call."
  ^Associative [bindings]
  (CachedBindings. bindings (seq (apply list bindings))))

(defprotocol ICodecContext
  (bindings [context])
  (wire-bindings [context mode allowlist])
  (acquire-cache! [context])
  (release-cache! [context cache])
  (prepared-handles [context])
  (acquire-compression! [context])
  (release-compression! [context compression]))

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
                      ^:unsynchronized-mutable thaw-binding-map
                      ^:unsynchronized-mutable ^WireCompression compression
                      ^:unsynchronized-mutable compression-active?
                      ^LinkedHashMap handles]
  java.io.Closeable
  (close [_]
    (when compression (.close compression)))
  ICodecContext
  (prepared-handles [_] handles)
  (acquire-compression! [_]
    (if compression-active?
      (WireCompression.)
      (do
        (when-not compression (set! compression (WireCompression.)))
        (set! compression-active? true)
        compression)))
  (release-compression! [_ state]
    (if (identical? compression state)
      (set! compression-active? false)
      (.close ^WireCompression state)))
  (bindings [this]
    ;; Security policy follows the current caller, even when a pool connection
    ;; moves between threads or databases. Do not capture native readers here.
    (let [current (b/serialization-allowlist)]
      (when (or (nil? binding-map) (not (identical? current allowlist)))
        (set! allowlist current)
        (set! binding-map
              (cached-bindings
                {#'*context* this
                 #'nv/*wire-native-value* true
                 #'nippy/*freeze-serializable-allowlist* current
                 #'nippy/*thaw-serializable-allowlist* current})))
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
                (cached-bindings
                  {#'nv/*wire-native-value* true
                   #'nippy/*freeze-serializable-allowlist* current})))
        freeze-binding-map)
      :thaw
      (do
        (when (or (nil? thaw-binding-map)
                  (not (identical? current
                                   (get thaw-binding-map
                                        #'nippy/*thaw-serializable-allowlist*))))
          (set! thaw-binding-map
                (cached-bindings
                  {#'nv/*wire-native-value* true
                   #'nippy/*thaw-serializable-allowlist* current})))
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

(defn create
  ([] (create nil))
  ([handles] (CodecContext. nil false nil nil nil nil nil false handles)))

(defmacro with-compression [[state] & body]
  `(let [context# *context*
         ~state (if context# (acquire-compression! context#) (WireCompression.))]
     (try ~@body
          (finally
            (if context# (release-compression! context# ~state)
                (.close ^WireCompression ~state))))))

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
