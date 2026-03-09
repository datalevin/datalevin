(ns datalevin.jepsen.cli
  (:require
   [clojure.string :as str]
   [datalevin.jepsen.core :as core]
   [jepsen.cli :as cli]))

(def ^:private supported-nemesis-labels
  (->> (concat [:none :failover :pause :partition :asymmetric :degraded
                :rejoin :quorum :clock-skew]
               core/supported-nemeses)
       (map name)
       sort
       (str/join ", ")))

(def cli-opts
  [[nil "--control-backend BACKEND" "HA control-plane backend: in-memory or sofa-jraft"
    :parse-fn keyword
    :default :in-memory
    :validate [#{:in-memory :sofa-jraft}
               "Must be one of: in-memory, sofa-jraft"]]

   [nil "--db-name NAME" "Database name to create for the workload"
    :default "jepsen-append"]

   [nil "--work-dir DIR" "Base work directory for local cluster data"]

   [nil "--keep-work-dir" "Keep local cluster work directories after success"]

   [nil "--key-count NUM" "Number of active workload keys when applicable"
    :default 8
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--account-balance NUM" "Initial per-account balance for the bank workload"
    :default 100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-transfer NUM" "Maximum transfer amount for the bank workload"
    :default 5
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Upper bound for append values per key"
    :default 32
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--min-txn-length NUM" "Minimum transaction length"
    :default 1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-txn-length NUM" "Maximum transaction length"
    :default 1
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "Comma-separated faults. Supported faults and aliases are shown in the validation error."
    :parse-fn core/parse-nemesis-spec
    :default []
    :validate [(partial every? core/supported-nemeses)
               (str "Supported faults: " supported-nemesis-labels)]]

   ["-r" "--rate HZ" "Approximate request rate"
    :default 20
    :parse-fn read-string
    :validate [pos? "Must be a positive number"]]

   ["-w" "--workload NAME" "Workload to run"
    :parse-fn keyword
    :default :append
    :validate [core/workloads (cli/one-of core/workloads)]]

   ["-v" "--verbose" "Enable verbose Datalevin server logging"]])

(defn all-tests
  [opts]
  (for [i (range (:test-count opts))]
    (core/datalevin-test (assoc opts :test-index i))))

(defn -main
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  core/datalevin-test
                                         :opt-spec cli-opts})
                   (cli/test-all-cmd {:tests-fn all-tests
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
