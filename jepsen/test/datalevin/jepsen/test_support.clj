(ns datalevin.jepsen.test-support
  (:require
   [clojure.string :as str]
   [taoensso.timbre :as log])
  (:import
   [ch.qos.logback.classic Level Logger]
   [org.slf4j LoggerFactory]))

(def ^:private default-test-log-level :error)

(def ^:private logback-logger-names
  ["ROOT"
   "datalevin"
   "datalevin.server"
   "datalevin.ha"
   "jepsen"
   "jepsen.cli"
   "jepsen.core"])

(def ^:private noisy-logback-logger-names
  ["com.alipay"
   "com.alipay.sofa"
   "com.alipay.remoting"])

(defn- parse-log-level
  [s]
  (case (some-> s
                str/trim
                (str/replace-first #"^:" "")
                str/lower-case)
    "trace" :trace
    "debug" :debug
    "info"  :info
    "warn"  :warn
    "error" :error
    "fatal" :fatal
    default-test-log-level))

(defn- test-log-level
  []
  (parse-log-level (System/getenv "DTLV_JEPSEN_LOG_LEVEL")))

(defn- env-log-level?
  []
  (some-> (System/getenv "DTLV_JEPSEN_LOG_LEVEL")
          str/trim
          not-empty
          boolean))

(defn- logback-level
  [level]
  (Level/toLevel
   (case level
     :fatal "ERROR"
     (str/upper-case (name level)))))

(defn- logback-loggers
  [logger-names]
  (keep
   (fn [^String logger-name]
     (let [logger (LoggerFactory/getLogger logger-name)]
       (when (instance? Logger logger)
         [logger-name logger])))
   logger-names))

(defn quiet-logs-fixture
  [f]
  (let [old-timbre-config log/*config*
        level             (test-log-level)
        slf4j-level       (logback-level level)
        old-logback-levels (into {}
                                 (map (fn [[logger-name ^Logger logger]]
                                        [logger-name (.getLevel logger)]))
                                 (logback-loggers
                                  (concat logback-logger-names
                                          noisy-logback-logger-names)))]
    (try
      (log/set-min-level! level)
      (doseq [[_ ^Logger logger] (logback-loggers logback-logger-names)]
        (.setLevel logger slf4j-level))
      (doseq [[_ ^Logger logger] (logback-loggers noisy-logback-logger-names)]
        (.setLevel logger (if (env-log-level?)
                            slf4j-level
                            Level/OFF)))
      (f)
      (finally
        (log/set-config! old-timbre-config)
        (doseq [[logger-name ^Logger logger]
                (logback-loggers
                 (concat logback-logger-names noisy-logback-logger-names))]
          (.setLevel logger (get old-logback-levels logger-name)))))))
