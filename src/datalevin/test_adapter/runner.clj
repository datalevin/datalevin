(ns ^:no-doc datalevin.test-adapter.runner
  "Run selected original test vars with their namespace fixtures."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.java.shell :as shell]
   [clojure.string :as str]
   [clojure.test :as t]
   [datalevin.test-adapter :as adapter])
  (:import
   [java.security MessageDigest]
   [java.util HexFormat]))

(defn validate-selection [selection]
  (let [tests (:tests selection)
        symbols (mapv :var tests)]
    (when-not (and (= 1 (:version selection)) (seq tests) (vector? tests)
                   (every? qualified-symbol? symbols)
                   (= (count symbols) (count (distinct symbols))))
      (throw (ex-info "Expected a version 1 selection of distinct qualified test vars"
                      {:selection selection})))
    selection))

(defn- var-symbol [v]
  (symbol (str (ns-name (:ns (meta v)))) (str (:name (meta v)))))

(defn- printable [value]
  (binding [*print-length* 100 *print-level* 20] (pr-str value)))

(defn- error-info [^Throwable error]
  {:class (.getName (class error)) :message (.getMessage error)
   :data (printable (ex-data error))})

(defn- event-info [{:keys [type actual] :as event}]
  (cond-> (select-keys event [:type :file :line :message])
    (contains? event :expected) (assoc :expected (printable (:expected event)))
    (contains? event :actual) (assoc :actual (if (instance? Throwable actual)
                                             (error-info actual)
                                             (printable actual)))
    (seq t/*testing-contexts*) (assoc :contexts (vec (reverse t/*testing-contexts*)))
    (= :error type) (assoc :unsupported (adapter/unsupported-data actual))))

(defn- status [{:keys [began? ended? assertions unsupported errors]}]
  (cond
    (seq unsupported) :unsupported
    (or (seq errors) (some #(= :error (:type %)) assertions)) :errored
    (some #(= :fail (:type %)) assertions) :failed
    (and began? ended? (seq assertions)) :passed
    :else :not-run))

(defn run-selection!
  "Backend binding precedes require, including top-level test setup. Owns and
  stops the backend. Use separate JVMs for actual cross-backend comparisons."
  [backend selection]
  (validate-selection selection)
  (let [selected (mapv :var (:tests selection))
        states (atom (zipmap selected (repeat {:assertions [] :unsupported [] :errors []})))
        errors (atom [])
        current (atom nil)
        scope (atom nil)
        info (adapter/backend-info backend)
        record-error! (fn [stage error]
                        (let [entry (assoc (error-info error) :stage stage :namespace @scope)]
                          (swap! errors conj entry)
                          (when-let [sym @current]
                            (swap! states update-in [sym :errors] conj entry))))
        report! (fn [{:keys [type var] :as event}]
                  (let [sym (or (some-> var var-symbol) @current)]
                    (case type
                      :begin-test-var (swap! states assoc-in [sym :began?] true)
                      :end-test-var (swap! states assoc-in [sym :ended?] true)
                      (:pass :fail :error)
                      (if sym
                        (swap! states update-in [sym :assertions] conj (event-info event))
                        (swap! errors conj (assoc (event-info event) :stage :fixture)))
                      nil)))]
    (binding [adapter/*backend* backend
              adapter/*on-unsupported*
              (fn [data]
                (doseq [sym (if @current [@current]
                               (filter #(= @scope (symbol (namespace %))) selected))]
                  (swap! states update-in [sym :unsupported] conj data)))
              t/report report!
              t/*report-counters* (ref t/*initial-report-counters*)
              t/*testing-vars* (list)
              t/*testing-contexts* (list)]
      (try
        (doseq [ns-sym (distinct (map #(symbol (namespace %)) selected))]
          (reset! scope ns-sym)
          (let [symbols (filterv #(= ns-sym (symbol (namespace %))) selected)
                vars (try
                       (require ns-sym)
                       (mapv (fn [sym]
                               (let [v (ns-resolve ns-sym (symbol (name sym)))]
                                 (when-not (and (var? v) (:test (meta v))
                                                (= sym (var-symbol v)))
                                   (throw (ex-info "Selected test var does not exist"
                                                   {:test sym})))
                                 (swap! states update sym merge
                                        (select-keys (meta v) [:file :line]))
                                 v)) symbols)
                       (catch Throwable error
                         (record-error! :namespace-load error)
                         nil))]
            (when vars
              (let [metadata (meta (the-ns ns-sym))
                    once-fixture (t/join-fixtures (::t/once-fixtures metadata))
                    each-fixture (t/join-fixtures (::t/each-fixtures metadata))]
                (try
                  (once-fixture
                    (fn []
                      (doseq [v vars]
                        (reset! current (var-symbol v))
                        (try
                          (each-fixture #(t/test-var v))
                          (catch Throwable error (record-error! :each-fixture error))
                          (finally (reset! current nil))))))
                  (catch Throwable error (record-error! :once-fixture error)))))))
        (finally
          (reset! scope nil)
          (try (adapter/stop! backend)
               (catch Throwable error (record-error! :backend-stop error))))))
    (let [results (mapv (fn [sym]
                          (let [state (get @states sym)]
                            (assoc state :var sym :status (status state)
                                   :assertion-counts (frequencies (map :type (:assertions state))))))
                        selected)
          passed? (and (empty? @errors) (every? #(= :passed (:status %)) results))]
      {:version 1 :selection (:id selection) :backend info
       :coverage (:coverage selection) :selected selected
       :status (if passed? :passed :failed)
       :exit-code (if passed? 0 1)
       :test-counts (frequencies (map :status results))
       :assertion-counts (apply merge-with + {} (map :assertion-counts results))
       :tests results :errors @errors})))

(defn- sha256 [file]
  (when (.isFile (io/file file))
    (with-open [input (io/input-stream file)]
      (let [digest (MessageDigest/getInstance "SHA-256")
            buffer (byte-array 8192)]
        (loop []
          (let [size (.read input buffer)]
            (when (pos? size)
              (.update digest buffer 0 size)
              (recur))))
        (.formatHex (HexFormat/of) (.digest digest))))))

(defn- revision [directory]
  (let [{:keys [exit out err]} (shell/sh "git" "-C" directory "rev-parse" "HEAD")
        changes (shell/sh "git" "-C" directory "status" "--porcelain" "--untracked-files=no")]
    (if (zero? (long exit))
      {:revision (str/trim out)
       :tracked-dirty? (if (zero? (long (:exit changes))) (not (str/blank? (:out changes))) :unknown)}
      {:revision nil :error (str/trim err)})))

(defn- provenance [selection manifest peer]
  {:release-pinned? false
   :tests (revision ".")
   :reference (revision "checkouts/datalevin")
   :rust-source (revision "checkouts/datalevin")
   :adapter (revision "checkouts/datalevin")
   :selection-sha256 (sha256 manifest)
   :source-sha256
   (into {} (for [file (distinct
                        (concat ["project.clj"
                                 "checkouts/datalevin/src/datalevin/test_adapter.clj"
                                 "checkouts/datalevin/src/datalevin/test_adapter/reference.clj"
                                 "checkouts/datalevin/src/datalevin/test_adapter/rust.clj"
                                 "checkouts/datalevin/src/datalevin/test_adapter/runner.clj"
                                 "checkouts/datalevin/src/rust/test-adapter/Cargo.toml"
                                 "checkouts/datalevin/src/rust/test-adapter/Cargo.lock"
                                 "checkouts/datalevin/src/rust/test-adapter/src/main.rs"]
                                (keep :file (:tests selection))
                                (mapcat :support-files (:tests selection))))]
              [file (sha256 file)]))
   :peer-sha256 (when peer (sha256 peer))
   :java (System/getProperty "java.version") :clojure (clojure-version)
   :direct-linking (System/getProperty "clojure.compiler.direct-linking")})

(defn -main [& args]
  (let [[backend-name manifest-path report-path peer & extra] args
        report (try
                 (when-not (and (contains? #{"reference" "rust"} backend-name)
                                manifest-path report-path (empty? extra)
                                (if (= "rust" backend-name) peer (nil? peer)))
                   (throw (ex-info
                           "Usage: reference|rust selection.edn report.edn [rust-peer-executable]" {})))
                 (let [selection (validate-selection (edn/read-string (slurp manifest-path)))
                       provenance (provenance selection manifest-path peer)
                       backend (if (= "reference" backend-name)
                                 ((requiring-resolve 'datalevin.test-adapter.reference/start))
                                 ((requiring-resolve 'datalevin.test-adapter.rust/start) [peer]))]
                   (assoc (run-selection! backend selection) :provenance provenance))
                 (catch Throwable error
                   {:version 1 :status :failed :exit-code 1
                    :backend {:backend backend-name}
                    :errors [(assoc (error-info error) :stage :startup)]}))]
    (when report-path
      (io/make-parents report-path)
      (spit report-path (str (pr-str report) "\n")))
    (doseq [{:keys [var status assertion-counts unsupported]} (:tests report)]
      (println var (name status) (pr-str assertion-counts))
      (doseq [gap unsupported] (println " " (pr-str gap))))
    (doseq [error (:errors report)] (binding [*out* *err*] (println (pr-str error))))
    (println backend-name (name (:status report)) "report:" report-path)
    (shutdown-agents)
    (System/exit (:exit-code report))))
