;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.server.connection
  "Own the socket and thread for each accepted connection."
  (:require
   [datalevin.buffer :as bf]
   [datalevin.constants :as c]
   [datalevin.protocol :as p]
   [datalevin.protocol.context :as codec]
   [datalevin.server.resources :as resources])
  (:import
   [java.nio.channels SelectionKey ServerSocketChannel SocketChannel]
   [java.util Set UUID]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean]))

(defn- connection-key
  "Retain the handlers' channel/attachment API without registering the blocking
  socket with a selector. Read readiness belongs exclusively to its thread."
  ^SelectionKey [^SocketChannel ch]
  (let [valid? (AtomicBoolean. true)]
    (proxy [SelectionKey] []
      (channel [] ch)
      (selector [] nil)
      (isValid [] (and (.get valid?) (.isOpen ch)))
      (cancel [] (.set valid? false))
      (interestOps
        ([] 0)
        ([_] (throw (UnsupportedOperationException. "Connection owns its reads"))))
      (readyOps [] 0))))

(defn close!
  "Cancel a connection by closing its socket and interrupting its owner. Keep
  the thread registered until it exits so shutdown can still join it."
  [^SelectionKey key]
  (let [{:keys [connection-id connection-threads connections]} (some-> key .attachment deref)]
    (.cancel key)
    (try
      (.close (.channel key))
      (finally
        (when connections (.remove ^Set connections (.channel key)))
        (when-let [^Thread thread (and connection-threads
                                      (.get ^ConcurrentHashMap connection-threads
                                            connection-id))]
          (when-not (identical? thread (Thread/currentThread))
            (.interrupt thread)))))))

(defn accept!
  "Start one thread for an accepted socket. Publish its ID, key and thread
  before starting it; remove ownership only after the read loop and cleanup exit."
  ([listener connections connection-keys connection-threads serve!]
   (accept! listener connections connection-keys connection-threads serve!
            (constantly nil)))
  ([^SelectionKey listener
    ^Set connections ^ConcurrentHashMap connection-keys
    ^ConcurrentHashMap connection-threads serve! context-fn]
   (when-let [^SocketChannel ch (.accept ^ServerSocketChannel (.channel listener))]
     (let [id (UUID/randomUUID)]
       (try
         (.configureBlocking ch true)
         (let [key (connection-key ch)
               thread (Thread.
                        ^Runnable
                        (fn []
                          (try
                            (serve! key)
                            (finally
                              (try (close! key)
                                   (finally
                                     (.remove connection-keys id)
                                     (.remove connection-threads id))))))
                        (str "datalevin-connection-" id))]
           (.attach key (volatile! {:read-bf (bf/allocate-buffer c/+buffer-size+)
                                    :write-bf (bf/allocate-buffer c/+buffer-size+)
                                    :wire-opts (p/default-wire-opts)
                                    :request-decoder (p/request-decoder)
                                    :codec-context (codec/create)
                                    :context (context-fn key)
                                    :connection-id id
                                    :connection-threads connection-threads
                                    :connections connections}))
           (.add connections ch)
           (.put connection-keys id key)
           (.put connection-threads id thread)
           (.start thread))
         (catch Throwable t
           (resources/close-suppressing! t #(.close ch))
           (.remove connections ch)
           (.remove connection-keys id)
           (.remove connection-threads id)
           (throw t)))))))
