#!/usr/bin/env clojure

"USAGE: ./release.clj <new-version>"

(def new-v (first *command-line-args*))

(assert (re-matches #"\d+\.\d+\.\d+" (or new-v ""))
        "Use ./release.clj <new-version>")
(println "Releasing version" new-v)

(require '[clojure.string :as str])
(require '[clojure.java.shell :as sh])
(import '[java.time LocalDate])
(import '[java.io File])

(defn update-file [f fn]
  (print "Updating" (str f "...")) (flush)
  (spit f (fn (slurp f)))
  (println "OK"))

(defn current-version []
  (second (re-find #"def version \"([0-9\.]+)\"" (slurp "project.clj"))))

(def ^:dynamic *env* {})
(def dtlvtest-dir "../dtlvtest")
(def federation-dir "../federation")
(def cljdoc-dir "../cljdoc")
(def repo-dir (.getCanonicalPath (File. ".")))

(defn sh [& args]
  (apply println "Running" (if (empty? *env*) "" (str :env " " (sort (keys *env*)))) args)
  (let [res (apply sh/sh
                   (concat args [:env (merge (into {} (System/getenv)) *env*)]))]
    (if (== 0 (:exit res))
      (do
        (println (:out res))
        (:out res))
      (binding [*out* *err*]
        (println "Process" args "exited with code" (:exit res))
        (println (:out res))
        (println (:err res))
        (throw (ex-info
                 (str "Process" args "exited with code" (:exit res)) res))))))

(defn update-version []
  (println "\n\n[ Updating version number ]\n")
  (let [old-v    (current-version)
        old->new #(str/replace % old-v new-v)]
    (update-file "CHANGELOG.md"
                 #(str/replace % "# WIP" (str "# " new-v " ("
                                              (.toString (LocalDate/now))
                                              ")")))
    (update-file "project.clj" #(str/replace-first % old-v new-v))
    (update-file "test-jar/deps.edn" old->new)
    (update-file "test-jar/test-uber.sh" old->new)
    (update-file "doc/install.md" old->new)
    (update-file "doc/dtlv.md" old->new)
    (update-file "examples/java/README.md" old->new)
    (update-file "src/datalevin/constants.clj" old->new)
    (update-file "bindings/javascript/package.json" old->new)
    (update-file "bindings/javascript/package-lock.json" old->new)
    (update-file "bindings/python/pyproject.toml" old->new)
    (update-file "bindings/python/src/datalevin/__init__.py" old->new)
    (update-file "README.md" old->new)))

(defn make-commit []
  (println "\n\n[ Making a commit ]\n")
  (sh "git" "add"
      "CHANGELOG.md"
      "project.clj"
      "test-jar/deps.edn"
      "test-jar/test-uber.sh"
      "doc/install.md"
      "doc/dtlv.md"
      "examples/java/README.md"
      "src/datalevin/constants.clj"
      "bindings/javascript/package.json"
      "bindings/javascript/package-lock.json"
      "bindings/python/pyproject.toml"
      "bindings/python/src/datalevin/__init__.py"
      "README.md")

  (sh "git" "commit" "-m" (str "Version " new-v))
  (sh "git" "tag" "-l" new-v)
  (sh "git" "push" "origin" "master"))

(defn cljdoc-check []
  (println "\n\n[ Checking cljdoc analysis ]\n")
  (when-not (.isDirectory (File. cljdoc-dir))
    (throw (ex-info "Sibling cljdoc checkout not found"
                    {:dir cljdoc-dir})))
  (sh "lein" "with-profile" "core-release" "do" "clean," "pom," "jar")
  (sh "clojure" "-T:build" "compile-java" :dir cljdoc-dir)
  (sh "clojure" "-M"
      "-i" (str repo-dir "/script/datalevin/release_cljdoc.clj")
      "-e" (pr-str
              (list 'datalevin.release-cljdoc/check!
                    {:project "datalevin/datalevin"
                     :version new-v
                     :jar (str repo-dir "/target/datalevin-" new-v ".jar")
                     :pom (str repo-dir "/pom.xml")
                     :scm-url repo-dir
                     :scm-rev "HEAD"}))
      :dir cljdoc-dir))

(defn run-binding-tests []
  (println "\n\n[ Running Java binding tests ]\n")
  (sh "test-jar/test-java.sh")

  (println "\n\n[ Building runtime jar for JavaScript and Python tests ]\n")
  (sh "clojure" "-T:build" "vendor-jar" ":native-platform" "all")

  (println "\n\n[ Running JavaScript binding tests ]\n")
  (sh "npm" "ci" :dir "bindings/javascript")
  ;; Each integration test worker starts a JVM; bound simultaneous startups.
  (sh "npm" "test" "--" "--test-concurrency=2" :dir "bindings/javascript")

  (println "\n\n[ Running Python binding tests ]\n")
  (let [python "bindings/python/.venv/bin/python"]
    (when-not (.isFile (File. python))
      (sh "python3" "-m" "venv" "bindings/python/.venv"))
    (sh python "-m" "pip" "--disable-pip-version-check" "install"
        "--editable" "./bindings/python[dev]")
    (sh python "-m" "pytest" "bindings/python/tests" "-q")))

(defn- postgres-test-url! []
  (let [url (System/getenv "POSTGRES_TEST_URL")]
    (when (str/blank? url)
      (throw (ex-info "Set POSTGRES_TEST_URL to a disposable PostgreSQL database for the release tests" {})))
    url))

(defn- run-tpc-and-federation-tests [postgres-url]
  ;; Each test alias loads its own Clojure dependencies.
  (println "\n\n[ Preparing Java classes for TPC and federation tests ]\n")
  (sh "clojure" "-T:build" "compile-java")

  (println "\n\n[ Running TPC-C benchmark tests ]\n")
  (binding [*env* (assoc *env* "TPCC_TEST_PG_URL"
                         (or (System/getenv "TPCC_TEST_PG_URL") postgres-url))]
    (sh "clojure" "-M:test" :dir "benchmarks/tpcc-bench"))

  (println "\n\n[ Running TPC-H benchmark tests ]\n")
  (sh "clojure" "-M:test" :dir "benchmarks/tpch-bench")

  (println "\n\n[ Running federation tests ]\n")
  (sh "clojure" "-M:test" :dir federation-dir))

(defn run-tests [postgres-url]
  (println "\n\n[ Running lein tests ]\n")
  (sh "./lein-test" :dir "script")

  (println "\n\n[ Running test1 lein tests (dtlvtest) ]\n")
  (sh "lein" "test" :dir dtlvtest-dir)

  (println "\n\n[ Running Jepsen lein tests ]\n")
  (sh "lein" "test" :dir "jepsen")

  (println "\n\n[ Running JOB tests ]\n")
  (sh "./job-test" :dir "script")

  (println "\n\n[ Running math tests ]\n")
  (sh "./math-test" :dir "script")

  (println "\n\n[ Running LDBC-SNB tests ]\n")
  (sh "./ldbc-snb-test" :dir "script")

  (println "\n\n[ Running idoc tests ]\n")
  (sh "clojure" "-M:test" :dir "benchmarks/idoc-bench")

  (println "\n\n[ Running OpenRuleBench tests ]\n")
  (sh "clojure" "-M:test" :dir "benchmarks/openrulebench")

  (println "\n\n[ Running YCSB benchmark tests ]\n")
  (sh "clojure" "-M:jvm:test" :dir "benchmarks/ycsb-bench")

  (println "\n\n[ Running write benchmark tests ]\n")
  (sh "clojure" "-M:test" :dir "benchmarks/write-bench")

  (run-tpc-and-federation-tests postgres-url)

  (println "\n\n[ Testing jar ]\n")
  (sh "./jar" :dir "script")
  (sh "test-jar/test.sh")

  (println "\n\n[ Testing uberjar ]\n")
  (sh "./uberjar" :dir "script")
  (sh "test-jar/test-uber.sh")

  (println "\n\n[ Testing native jar ]\n")
  (sh "test-jar/test-native.sh")

  (println "\n\n[ Running native tests ]\n")
  (sh "script/compile-native")
  (sh "./dtlv-test0")
  (sh "./script/compile-native-test1" :dir dtlvtest-dir)
  (sh "./dtlv-test1" :dir dtlvtest-dir)
  (run-binding-tests)
  )

(defn- str->json [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")
      (str/replace "\n" "\\n")))

(defn- map->json [m]
  (str "{ "
    (->>
      (map (fn [[k v]] (str "\"" (str->json k) "\": \"" (str->json v) "\"")) m)
      (str/join ",\n"))
    " }"))

(def GITHUB_AUTH (System/getenv "GITHUB_AUTH"))

(defn github-release []
  (let [changelog (->> (slurp "CHANGELOG.md")
                       str/split-lines
                       (drop-while #(not= (str "# " new-v) %))
                       next
                       (take-while #(not (re-matches #"# .+" %)))
                       (remove str/blank?)
                       (str/join "\n"))
        request  { "tag_name" new-v
                   "name"     new-v
                   "target_commitish" "master"
                   "body" changelog}]
    (sh "curl" "-u" GITHUB_AUTH
        "-X" "POST"
        "--data" (map->json request)
        "https://api.github.com/repos/datalevin/datalevin/releases")))

(defn -main []
  (run-tests (postgres-test-url!))
  (update-version)
  (cljdoc-check)
  (make-commit)
  (github-release)
  (sh "./deploy" :dir "script")
  (System/exit 0)
  )

(-main)
