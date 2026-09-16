(ns ycsb-bench.server
  "Own the loopback server used by a benchmark case."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.server :as server])
  (:import [datalevin.server Server]
           [java.lang ProcessHandle]
           [java.net InetSocketAddress]
           [java.nio.channels ServerSocketChannel]
           [java.nio.file Files StandardCopyOption]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(def defaults
  {:server-mode :process :server-heap-mb 4096
   :server-workers 16 :server-queue-size 1024
   :server-transaction-threads 16 :server-background-threads 4
   :server-transaction-lock-timeout-ms 1000 :server-startup-timeout-ms 120000})

(defn server-options [root opts]
  {:root (str root "/server") :host "127.0.0.1" :port 0 :verbose false
   :worker-threads (:server-workers opts)
   :worker-queue-size (:server-queue-size opts)
   :transaction-threads (:server-transaction-threads opts)
   :background-threads (:server-background-threads opts)
   :transaction-lock-timeout-ms (:server-transaction-lock-timeout-ms opts)})

(defn- runtime-info [^Server srv opts]
  (let [socket (.-server-socket srv)
        address (.getLocalAddress ^ServerSocketChannel socket)
        execution (.-execution srv)
        connection-threads? (and (contains? execution :connection-threads)
                                 (not (:routing execution)))]
    {:pid (.pid (ProcessHandle/current))
     :port (.getPort ^InetSocketAddress address)
     :java-version (System/getProperty "java.version")
     :max-heap-bytes (.maxMemory (Runtime/getRuntime))
     :source-resources (into (sorted-map)
                             (map (fn [path] [path (some-> (io/resource path) str)]))
                             ["datalevin/server.clj" "datalevin/server/dispatch.clj"
                              "datalevin/client.clj" "datalevin/remote.clj"])
     :execution-keys (vec (sort (keys execution)))
     :configuration (cond-> (assoc (dissoc opts :root :port)
                                   :request-execution (if connection-threads?
                                                        :connection-thread :worker-pool)
                                   :transaction-execution (if (:transactions execution)
                                                            :worker-pool :connection-thread))
                      connection-threads? (dissoc :worker-threads :worker-queue-size))}))

(defn -main [config-path ready-path]
  (let [opts (edn/read-string (slurp config-path))
        srv (server/create opts)
        hook (Thread. ^Runnable #(server/stop srv) "ycsb-server-shutdown")]
    (try
      (.addShutdownHook (Runtime/getRuntime) hook)
      (server/start srv)
      (let [ready (io/file ready-path)
            pending (io/file (str ready-path ".pending"))]
        (spit pending (pr-str (runtime-info srv opts)))
        (Files/move (.toPath pending) (.toPath ready)
                    (into-array StandardCopyOption [StandardCopyOption/ATOMIC_MOVE])))
      ;; Closing the parent's pipe, including on parent death, stops the child.
      (.read System/in)
      (finally
        (try (server/stop srv)
             (finally
               (.removeShutdownHook (Runtime/getRuntime) hook)
               (shutdown-agents)))))))

(defn- stop-process! [^Process process]
  (let [interrupted? (volatile! (Thread/interrupted))
        await! (fn [ms]
                 (try (.waitFor process (long ms) TimeUnit/MILLISECONDS)
                      (catch InterruptedException _
                        (vreset! interrupted? true)
                        false)))]
    (try
      (try (.close (.getOutputStream process)) (catch java.io.IOException _))
      (when-not (await! 10000)
        (.destroy process)
        (when-not (await! 5000)
          (.destroyForcibly process)
          (when-not (await! 5000)
            (throw (ex-info "Benchmark server did not terminate" {:pid (.pid process)})))))
      (finally
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defn- start-process! [root opts]
  (let [config (io/file root "server-config.edn")
        ready (io/file root "server-ready.edn")
        log (io/file root "server.log")
        java (str (io/file (System/getProperty "java.home") "bin" "java"))
        args [java (str "-Xms" (:server-heap-mb opts) "m")
              (str "-Xmx" (:server-heap-mb opts) "m")
              "--add-opens=java.base/java.lang=ALL-UNNAMED"
              "--add-opens=java.base/java.nio=ALL-UNNAMED"
              "--add-opens=java.base/java.util=ALL-UNNAMED"
              "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
              "--enable-native-access=ALL-UNNAMED"
              "-cp" (System/getProperty "java.class.path")
              "clojure.main" "-m" "ycsb-bench.server" (str config) (str ready)]
        _ (spit config (pr-str (server-options root opts)))
        process (-> (ProcessBuilder. ^java.util.List args)
                    (.redirectErrorStream true)
                    (.redirectOutput log)
                    (.start))]
    (try
      (let [deadline (+ (System/nanoTime) (* (long (:server-startup-timeout-ms opts)) 1000000))]
        (loop []
          (cond
            (not (.isAlive process))
            (throw (ex-info "Benchmark server exited before readiness"
                            {:exit (.exitValue process)}))
            (.exists ready)
            {:info (assoc (edn/read-string (slurp ready)) :placement :separate-process)
             :process process :close #(stop-process! process)}
            (>= (System/nanoTime) deadline)
            (throw (ex-info "Timed out starting benchmark server" {}))
            :else (do (Thread/sleep 25) (recur)))))
      (catch Throwable t
        (try (stop-process! process)
             (catch Throwable cleanup (.addSuppressed t cleanup)))
        (throw (ex-info "Unable to start benchmark server"
                        {:server-log (str/join "\n" (take-last 40 (str/split-lines (slurp log))))}
                        t))))))

(defn start!
  "Start an owned server. Its close function must run before deleting root."
  [root opts]
  (let [opts (merge defaults opts)]
    (if (= :process (:server-mode opts))
      (start-process! root opts)
      (let [config (server-options root opts)
            srv (server/create config)]
        (try
          (server/start srv)
          {:info (assoc (runtime-info srv config) :placement :same-process)
           :close #(server/stop srv)}
          (catch Throwable t
            (try (server/stop srv) (catch Throwable cleanup (.addSuppressed t cleanup)))
            (throw t)))))))
