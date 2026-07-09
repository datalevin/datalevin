(ns test-jar.client-quickstart-check
  (:require
   [clojure.java.shell :as sh]
   [clojure.string :as str]
   [datalevin.constants :as c]
   [datalevin.server :as srv]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.net ServerSocket]
   [java.util UUID]))

(def ^:private expected-output
  ["Client id:"
   "Databases:"
   "Open info:"
   "System query result: java-quickstart-"
   "Connected clients: ["])

(defn- allocate-port
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn- run-client-quickstart!
  [classpath uri]
  (let [{:keys [exit out err]}
        (sh/sh "java" "-cp" classpath "ClientQuickStart"
               :env (assoc (into {} (System/getenv)) "DATALEVIN_URI" uri))
        exit-code (long exit)]
    (print out)
    (binding [*out* *err*]
      (print err))
    (when-not (zero? exit-code)
      (throw (ex-info "ClientQuickStart failed."
                      {:exit exit
                       :out  out
                       :err  err})))
    (doseq [expected expected-output]
      (when-not (str/includes? out expected)
        (throw (ex-info "ClientQuickStart output missing expected content."
                        {:expected expected
                         :out      out}))))
    (flush)))

(defn -main
  [& _]
  (let [classpath (System/getenv "CLIENT_QUICKSTART_CLASSPATH")]
    (when (str/blank? classpath)
      (throw (ex-info "CLIENT_QUICKSTART_CLASSPATH is required." {})))
    (log/set-min-level! :report)
    (let [port   (allocate-port)
          root   (u/tmp-dir (str "java-client-quickstart-" (UUID/randomUUID)))
          server (binding [c/*db-background-sampling?* false]
                   (srv/create {:port port
                                :root root}))
          uri    (str "dtlv://datalevin:datalevin@localhost:" port)]
      (try
        (srv/start server)
        (run-client-quickstart! classpath uri)
        (finally
          (srv/stop server)
          (u/delete-files root)))))
  (shutdown-agents)
  (System/exit 0))
