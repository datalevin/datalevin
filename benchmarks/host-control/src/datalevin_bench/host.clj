(ns datalevin-bench.host
  "Pause the current user's macOS media-analysis daemons while a benchmark runs.

  `mediaanalysisd` and `photoanalysisd` can consume substantial CPU and skew
  timing. This namespace stops the ones owned by the current user for the
  duration of a measurement and resumes exactly those afterward, so a daemon
  that was already stopped before the run is left untouched. On other platforms
  it is a no-op."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]))

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

(defn- daemon-pids [daemon]
  (let [r (sh "pgrep" "-u" (or (current-uid) "") "-x" daemon)]
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
     (for [daemon daemons
           pid    (daemon-pids daemon)
           :when  (not (stopped? pid))
           :let   [result (sh "kill" "-STOP" (str pid))]
           :when  (do
                    (when-not (zero? (:exit result))
                      (println (str "Could not pause " daemon " pid " pid ": "
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

(defmacro with-paused-media
  "Run `body` with media-analysis daemons paused, resuming the ones this call
  paused when `body` finishes (normally or not)."
  [& body]
  `(let [paused# (pause!)]
     (println (if (seq paused#)
                (str "Paused media analysis daemons: " (pr-str paused#))
                "No media analysis daemons paused."))
     (try
       ~@body
       (finally
         (resume! paused#)
         (when (seq paused#)
           (println (str "Resumed media analysis daemons: " (pr-str paused#))))))))
