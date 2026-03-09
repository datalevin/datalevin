(ns datalevin.client-quickstart-check
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [datalevin.test.core :as tdc]))

(def ^:private expected-output
  ["Client id:"
   "Databases:"
   "Open info:"
   "System query result: java-quickstart-"
   "Connected clients: ["])

(defn -main
  [& _]
  (let [cp (System/getenv "CLIENT_QUICKSTART_CLASSPATH")]
    (when (str/blank? cp)
      (throw (ex-info "CLIENT_QUICKSTART_CLASSPATH is required." {})))
    (tdc/server-fixture
      (fn []
        (let [{:keys [exit out err]} (sh/sh "java" "-cp" cp "ClientQuickStart")]
          (print out)
          (binding [*out* *err*]
            (print err))
          (when-not (zero? exit)
            (throw (ex-info "ClientQuickStart failed."
                            {:exit exit
                             :out  out
                             :err  err})))
          (doseq [expected expected-output]
            (when-not (str/includes? out expected)
              (throw (ex-info "ClientQuickStart output missing expected content."
                              {:expected expected
                               :out      out})))))))
    (flush)
    (shutdown-agents)
    (System/exit 0)))
