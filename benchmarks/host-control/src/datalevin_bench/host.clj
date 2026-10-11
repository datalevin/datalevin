(ns datalevin-bench.host
  "Pause the current user's macOS media-analysis daemons while a benchmark runs.

  `mediaanalysisd` and `photoanalysisd` can consume substantial CPU and skew
  timing. This namespace stops the ones owned by the current user, watches for
  replacements during a run, and resumes exactly those it stopped afterward.
  Daemons already stopped before the run are left untouched. On other platforms
  it is a no-op."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s])
  (:import [java.util.concurrent CountDownLatch TimeUnit]))

(def ^:private daemons ["mediaanalysisd" "photoanalysisd"])

(defn macos? []
  (s/starts-with? (System/getProperty "os.name" "") "Mac"))

(defn- sh
  "Run a helper command. Returns {:exit :out}. Process execution can itself be
  denied (for example by a sandbox); that is reported like a non-zero exit so
  host control degrades to a no-op instead of failing the benchmark."
  [& args]
  (try
    (let [p    (-> (ProcessBuilder. ^java.util.List (vec args))
                   (.redirectErrorStream true)
                   (.start))
          out  (with-open [r (io/reader (.getInputStream p))] (slurp r))
          code (.waitFor p)]
      {:exit code :out out})
    (catch Exception e
      {:exit 1 :out (str (.getMessage e))})))

(defn- current-uid []
  (let [r (sh "id" "-u")]
    (when (zero? (:exit r)) (s/trim (:out r)))))

(def ^:private uid (delay (current-uid)))

(defn- daemon-pids []
  (let [r (sh "pgrep" "-u" (or @uid "") "-x" (s/join "|" daemons))]
    (if (zero? (:exit r))
      (->> (s/split-lines (:out r))
           (map s/trim)
           (remove s/blank?)
           (keep #(try (Long/parseLong %) (catch Exception _ nil))))
      [])))

(defn- stopped? [pid]
  (s/starts-with? (s/trim (:out (sh "ps" "-o" "state=" "-p" (str pid)))) "T"))

(defn pause!
  "SIGSTOP the current user's media-analysis daemons. Returns the pids paused,
  which `resume!` should be given. Daemons that cannot be paused (for example
  because they are owned by another user or protected) are reported and
  skipped."
  []
  (if-not (macos?)
    []
    (vec
     (for [pid (daemon-pids)
           :when  (not (stopped? pid))
           :let   [result (sh "kill" "-STOP" (str pid))]
           :when  (do
                    (when-not (zero? (:exit result))
                      (println (str "Could not pause media analysis pid " pid ": "
                                    (s/trim (:out result)))))
                    (zero? (:exit result)))]
       pid))))

(defn resume!
  "SIGCONT exactly the pids returned by `pause!`."
  [paused]
  (when (macos?)
    (doseq [pid paused]
      (sh "kill" "-CONT" (str pid))))
  nil)

(defn start-pause-monitor!
  "Pause current daemons and check every 250 ms for replacement processes.
  Returns a session for `stop-pause-monitor!`; only successfully stopped PIDs
  are recorded."
  []
  (let [mac? (macos?)
        paused (atom (if mac? (pause!) []))
        stop (CountDownLatch. 1)
        monitor (when mac?
                  (Thread.
                    ^Runnable
                    (fn []
                      (try
                        (loop []
                          (when-not (.await stop 250 TimeUnit/MILLISECONDS)
                            (let [replacement-pids (pause!)]
                              (swap! paused #(vec (distinct (into % replacement-pids)))))
                            (recur)))
                        (catch InterruptedException _)))
                    "datalevin-bench-media-monitor"))]
    (when monitor
      (.setDaemon ^Thread monitor true)
      (.start ^Thread monitor))
    {:paused paused :stop stop :thread monitor}))

(defn stop-pause-monitor!
  "Join the monitor before resuming its recorded PIDs. Preserve the calling
  thread's interruption status while completing cleanup. Returns resumed PIDs."
  [{:keys [paused stop thread]}]
  (let [interrupted? (volatile! (Thread/interrupted))]
    (try
      (.countDown ^CountDownLatch stop)
      (when thread
        (loop []
          (when (.isAlive ^Thread thread)
            (try (.join ^Thread thread)
                 (catch InterruptedException _ (vreset! interrupted? true)))
            (recur))))
      (let [pids @paused]
        (resume! pids)
        pids)
      (finally
        (when @interrupted? (.interrupt (Thread/currentThread)))))))

(defmacro with-paused-media
  "Run `body` while watching for media-analysis daemons, then stop the monitor
  and resume the PIDs this call paused, including when the body throws."
  [& body]
  `(let [session# (start-pause-monitor!)
         paused# @(:paused session#)]
     (println (if (seq paused#)
                (str "Paused media analysis daemons: " (pr-str paused#))
                "No media analysis daemons paused."))
     (try
       ~@body
       (finally
         (let [resumed# (stop-pause-monitor! session#)]
           (when (seq resumed#)
             (println (str "Resumed media analysis daemons: " (pr-str resumed#)))))))))
