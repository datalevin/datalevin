(ns ^:no-doc datalevin.test-adapter.rust
  "A synchronous, bounded Nippy process bridge. No reference engine calls."
  (:require
   [datalevin.util :refer [raise]]
   [taoensso.nippy :as nippy]
   [datalevin.test-adapter :as adapter])
  (:import
   [java.io DataInputStream DataOutputStream]
   [java.lang ProcessBuilder ProcessBuilder$Redirect]
   [java.util.concurrent Callable ExecutionException ExecutorService Executors
    ThreadFactory TimeUnit TimeoutException]))

(def ^:private ^:const max-frame-bytes (* 16 1024 1024))

(deftype ^:private DbHandle [session id])

(defn- read-frame [^DataInputStream input]
  (let [size (.readInt input)]
    (when-not (<= 1 size max-frame-bytes)
      (raise "Invalid Rust adapter frame size" {:size size}))
    (let [bytes (byte-array size)]
      (.readFully input bytes)
      (nippy/fast-thaw bytes))))

(defn- write-frame! [^DataOutputStream output value]
  (let [bytes (nippy/fast-freeze value)
        size (alength bytes)]
    (when (> size max-frame-bytes)
      (adapter/unsupported! :adapter (:op value)
                            "Rust adapter request exceeds the frame limit"))
    (.writeInt output size)
    (.write output bytes)
    (.flush output)))

(defn- stop-peer! [{:keys [process input output worker stopped]}]
  (when (compare-and-set! stopped false true)
    ;; Kill before closing streams: another thread may be blocked in read/write.
    (.destroy ^Process process)
    (when-not (.waitFor ^Process process 100 TimeUnit/MILLISECONDS)
      (.destroyForcibly ^Process process)
      (.waitFor ^Process process 1 TimeUnit/SECONDS))
    (.shutdownNow ^ExecutorService worker)
    (doseq [stream [input output]]
      (try (.close ^java.io.Closeable stream) (catch Exception _)))))

(defn- exchange! [{:keys [worker timeout-ms stopped] :as peer} f]
  (locking peer
    (when @stopped
      (raise "Rust adapter process is closed" {}))
    (let [task (.submit ^ExecutorService worker ^Callable (bound-fn [] (f)))]
      (try
        (.get task (long timeout-ms) TimeUnit/MILLISECONDS)
        (catch TimeoutException error
          (stop-peer! peer)
          (throw (ex-info "Rust adapter request timed out"
                          {:timeout-ms timeout-ms} error)))
        (catch ExecutionException error
          (stop-peer! peer)
          (throw (ex-info "Rust adapter transport failed" {} (.getCause error))))))))

(defn- argument [session operation value]
  (if (instance? DbHandle value)
    (if (identical? session (.-session ^DbHandle value))
      {:kind :db :id (.-id ^DbHandle value)}
      (raise "Database belongs to a different Rust adapter session"
                      {:operation operation}))
    {:kind :value :value value}))

(defn- result-value [session operation response]
  (when-not (map? response)
    (raise "Rust adapter response must be a map" {:operation operation}))
  (case (:status response)
    :ok
    (let [{:keys [kind id] :as result} (:result response)
          expects-db? (contains? #{:empty-db :db-with} operation)]
      (cond
        (and expects-db? (= :db kind) (int? id) (pos? (long id)))
        (DbHandle. session id)

        (and (not expects-db?) (= :value kind) (contains? result :value))
        (:value result)

        :else (raise "Invalid Rust adapter result descriptor"
                              {:operation operation :result result})))
    :unsupported
    (adapter/unsupported! :rust operation
                          (or (:message response) "Unsupported Rust operation"))
    :error
    (let [{:keys [class message data]} (:error response)]
      (when-not (and (string? class) (string? message))
        (raise "Invalid Rust adapter error descriptor" {:response response}))
      (throw (case class
               "java.lang.IllegalArgumentException" (IllegalArgumentException. message)
               "java.lang.IllegalStateException" (IllegalStateException. message)
               "clojure.lang.ExceptionInfo" (ex-info message (or data {}))
               (adapter/unsupported! :adapter operation
                                     (str "Exception class is not mapped: " class)))))
    (raise "Invalid Rust adapter response status" {:response response})))

(defrecord RustBackend [peer session info]
  adapter/Backend
  (invoke! [_ operation arguments]
    (let [request {:op operation
                   :args (mapv #(argument session operation %) arguments)}
          response (exchange! peer
                              #(do (write-frame! (:output peer) request)
                                   (read-frame (:input peer))))]
      (result-value session operation response)))
  (stop! [_] (stop-peer! peer))
  (backend-info [_] info))

(defn start
  "Start an already-built peer using an argv vector, without a shell."
  ([command] (start command 30000))
  ([command timeout-ms]
   (when-not (and (vector? command) (seq command) (every? string? command)
                  (int? timeout-ms) (pos? (long timeout-ms)))
     (raise "Expected a peer command vector and positive timeout" {}))
   (let [process (.start (doto (ProcessBuilder. ^java.util.List command)
                          (.redirectError ProcessBuilder$Redirect/INHERIT)))
         worker (Executors/newSingleThreadExecutor
                  (reify ThreadFactory
                    (newThread [_ task]
                      (doto (Thread. task "datalevin-test-adapter-io")
                        (.setDaemon true)))))
         peer {:process process
               :input (DataInputStream. (.getInputStream process))
               :output (DataOutputStream. (.getOutputStream process))
               :worker worker :timeout-ms timeout-ms :stopped (atom false)}]
     (try
       (let [hello (exchange! peer #(read-frame (:input peer)))]
         (when-not (and (= "datalevin-test-adapter" (:protocol hello))
                        (= 1 (:version hello)) (= :rust (:backend hello)))
           (raise "Incompatible Rust adapter handshake" {:hello hello}))
         (->RustBackend peer (Object.)
                        {:backend :rust :runtime :rust :command command :peer hello}))
       (catch Throwable error
         (stop-peer! peer)
         (throw error))))))
