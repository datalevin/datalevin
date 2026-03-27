#!/usr/bin/env clojure

"USAGE: ./release.clj <new-version>"

(def new-v (first *command-line-args*))

(assert (re-matches #"\d+\.\d+\.\d+" (or new-v ""))
        "Use ./release.clj <new-version>")
(println "Releasing version" new-v)

(require '[clojure.string :as str])
(require '[clojure.java.shell :as sh])
(import '[java.time LocalDate])

(defn update-file [f fn]
  (print "Updating" (str f "...")) (flush)
  (spit f (fn (slurp f)))
  (println "OK"))

(defn current-version []
  (second (re-find #"def version \"([0-9\.]+)\"" (slurp "project.clj"))))

(def ^:dynamic *env* {})
(def dtlvtest-dir "../dtlvtest")

(defn sh [& args]
  (apply println "Running" (if (empty? *env*) "" (str :env " " *env*)) args)
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
    (update-file "doc/embedded-release.md" old->new)
    (update-file "doc/dtlv.md" old->new)
    (update-file "examples/java/README.md" old->new)
    (update-file "src/datalevin/constants.clj" old->new)
    (update-file "README.md" old->new)))

(defn make-commit []
  (println "\n\n[ Making a commit ]\n")
  (sh "git" "add"
      "CHANGELOG.md"
      "project.clj"
      "test-jar/deps.edn"
      "test-jar/test-uber.sh"
      "doc/install.md"
      "doc/embedded-release.md"
      "doc/dtlv.md"
      "examples/java/README.md"
      "src/datalevin/constants.clj"
      "README.md")

  (sh "git" "commit" "-m" (str "Version " new-v))
  (sh "git" "tag" "-l" new-v)
  (sh "git" "push" "origin" "master"))

(defn run-tests []
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
  (run-tests)
  (update-version)
  (make-commit)
  (github-release)
  (sh "./deploy" :dir "script")
  (System/exit 0)
  )

(-main)
