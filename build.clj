(ns build
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.tools.build.api :as b])
  (:import [java.io File]))

(def class-dir "target/classes")
(def java-release-dir "target/java-release")
(def java-artifact-dir (str java-release-dir "/classes"))
(def java-source-dir (str java-release-dir "/sources"))
(def javadoc-dir (str java-release-dir "/javadoc"))
(def local-repo (str java-release-dir "/m2"))
(def version (or (some->> (slurp "project.clj")
                          (re-find #"\(def version \"([^\"]+)\"\)")
                          second)
                 "dev"))
(def java-lib 'datalevin/datalevin-java)
(def java-jar-file (format "target/datalevin-java-%s.jar" version))
(def java-pom-file (format "target/datalevin-java-%s.pom" version))
(def java-source-jar-file (format "target/datalevin-java-%s-sources.jar" version))
(def java-javadoc-jar-file (format "target/datalevin-java-%s-javadoc.jar" version))
(def deps-config (edn/read-string (slurp "deps.edn")))
(def runtime-deps (:deps deps-config))
(def basis (b/create-basis {:project "deps.edn"}))
(def scm {:connection          "scm:git:https://github.com/datalevin/datalevin.git"
          :developerConnection "scm:git:git@github.com:datalevin/datalevin.git"
          :tag                 (str "v" version)
          :url                 "https://github.com/datalevin/datalevin"})

(defn- existing-dirs
  [dirs]
  (->> dirs
       (filter #(.exists (File. ^String %)))
       vec))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-java [_]
  (b/delete {:path class-dir})
  (b/javac {:src-dirs   ["src/java"]
            :class-dir  class-dir
            :basis      basis
            :javac-opts ["--release" "21"]}))

(defn clean-java [_]
  (doseq [path [java-release-dir
                java-jar-file
                java-pom-file
                java-source-jar-file
                java-javadoc-jar-file]]
    (b/delete {:path path})))

(defn- java-classpath []
  (->> (cons class-dir (:classpath-roots basis))
       distinct
       (str/join File/pathSeparator)))

(defn- run-process! [command-args]
  (let [{:keys [exit out err]} (b/process {:command-args command-args
                                           :out          :capture
                                           :err          :capture})]
    (when-not (zero? exit)
      (throw (ex-info "External command failed."
                      {:command command-args
                       :exit    exit
                       :out     out
                       :err     err})))
    {:out out :err err}))

(defn javadoc [_]
  (compile-java nil)
  (b/delete {:path javadoc-dir})
  (run-process!
    ["javadoc"
     "--release" "21"
     "-quiet"
     "-notimestamp"
     "-d" javadoc-dir
     "-classpath" (java-classpath)
     "-sourcepath" "src/java"
     "datalevin"])
  (println "Generated Javadoc in" javadoc-dir)
  {:javadoc-dir javadoc-dir})

(defn javadoc-jar [_]
  (javadoc nil)
  (b/delete {:path java-javadoc-jar-file})
  (run-process!
    ["jar"
     "--create"
     "--file" java-javadoc-jar-file
     "-C" javadoc-dir
     "."])
  (println "Generated Javadoc jar at" java-javadoc-jar-file)
  {:javadoc-dir javadoc-dir
   :javadoc-jar java-javadoc-jar-file})

(defn- write-java-poms! []
  (let [pom-dir     (format "%s/META-INF/maven/%s/%s"
                            java-artifact-dir
                            (namespace java-lib)
                            (name java-lib))
        pom-file    (str pom-dir "/pom.xml")
        props-file  (str pom-dir "/pom.properties")
        deps->xml   (fn [[lib dep]]
                      (when-let [dep-version (:mvn/version dep)]
                        (str "    <dependency>\n"
                             "      <groupId>" (namespace lib) "</groupId>\n"
                             "      <artifactId>" (name lib) "</artifactId>\n"
                             "      <version>" dep-version "</version>\n"
                             (when-let [exclusions (:exclusions dep)]
                               (str "      <exclusions>\n"
                                    (apply str
                                           (for [exclusion exclusions]
                                             (str "        <exclusion>\n"
                                                  "          <groupId>" (namespace exclusion) "</groupId>\n"
                                                  "          <artifactId>" (name exclusion) "</artifactId>\n"
                                                  "        </exclusion>\n")))
                                    "      </exclusions>\n"))
                             "    </dependency>\n")))
        pom-xml     (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                         "<project xmlns=\"http://maven.apache.org/POM/4.0.0\"\n"
                         "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                         "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd\">\n"
                         "  <modelVersion>4.0.0</modelVersion>\n"
                         "  <groupId>" (namespace java-lib) "</groupId>\n"
                         "  <artifactId>" (name java-lib) "</artifactId>\n"
                         "  <version>" version "</version>\n"
                         "  <packaging>jar</packaging>\n"
                         "  <name>" (name java-lib) "</name>\n"
                         "  <description>A simple, fast and versatile Datalog database</description>\n"
                         "  <url>https://github.com/datalevin/datalevin</url>\n"
                         "  <licenses>\n"
                         "    <license>\n"
                         "      <name>EPL-1.0</name>\n"
                         "      <url>https://www.eclipse.org/legal/epl-1.0/</url>\n"
                         "    </license>\n"
                         "  </licenses>\n"
                         "  <scm>\n"
                         "    <url>" (:url scm) "</url>\n"
                         "    <connection>" (:connection scm) "</connection>\n"
                         "    <developerConnection>" (:developerConnection scm) "</developerConnection>\n"
                         "    <tag>" (:tag scm) "</tag>\n"
                         "  </scm>\n"
                         "  <repositories>\n"
                         "    <repository>\n"
                         "      <id>clojars</id>\n"
                         "      <url>https://repo.clojars.org/</url>\n"
                         "    </repository>\n"
                         "  </repositories>\n"
                         "  <dependencies>\n"
                         (->> runtime-deps
                              (sort-by (comp str key))
                              (map deps->xml)
                              (apply str))
                         "  </dependencies>\n"
                         "</project>\n")]
    (.mkdirs (File. pom-dir))
    (spit pom-file pom-xml)
    (spit props-file
          (str "groupId=" (namespace java-lib) "\n"
               "artifactId=" (name java-lib) "\n"
               "version=" version "\n"))
    (b/delete {:path java-pom-file})
    (spit java-pom-file pom-xml)))

(defn- prep-java-artifact! []
  (compile-java nil)
  (b/delete {:path java-artifact-dir})
  (b/copy-dir {:src-dirs   (existing-dirs ["src" "resources" class-dir])
               :target-dir java-artifact-dir})
  ;; Keep the release jar free of embedded Java sources; they go in the
  ;; separate sources jar instead.
  (b/delete {:path (str java-artifact-dir "/java")})
  (write-java-poms!))

(defn java-jar [_]
  (prep-java-artifact!)
  (b/jar {:class-dir java-artifact-dir
          :jar-file  java-jar-file
          :manifest  {"Automatic-Module-Name" "datalevin"
                      "Implementation-Title"  "Datalevin Java"
                      "Implementation-Version" version}})
  (println "Generated Java jar at" java-jar-file)
  {:jar-file java-jar-file
   :pom-file java-pom-file})

(defn java-source-jar [_]
  (b/delete {:path java-source-dir})
  (b/copy-dir {:src-dirs   (existing-dirs ["src" "resources"])
               :target-dir java-source-dir})
  (b/delete {:path (str java-source-dir "/java")})
  (b/copy-dir {:src-dirs   (existing-dirs ["src/java"])
               :target-dir java-source-dir})
  (b/jar {:class-dir java-source-dir
          :jar-file  java-source-jar-file})
  (println "Generated Java sources jar at" java-source-jar-file)
  {:source-jar java-source-jar-file})

(defn java-release [_]
  (clean-java nil)
  (java-jar nil)
  (java-source-jar nil)
  (javadoc-jar nil)
  {:jar-file     java-jar-file
   :pom-file     java-pom-file
   :source-jar   java-source-jar-file
   :javadoc-jar  java-javadoc-jar-file})

(defn- repo-path
  [lib-sym]
  (format "%s/%s/%s/%s"
          local-repo
          (str/replace (namespace lib-sym) "." "/")
          (name lib-sym)
          version))

(defn- metadata-path
  [lib-sym]
  (format "%s/%s/%s/maven-metadata-local.xml"
          local-repo
          (str/replace (namespace lib-sym) "." "/")
          (name lib-sym)))

(defn- install-artifact!
  [src filename]
  (b/copy-file {:src src
                :target (str (repo-path java-lib) "/" filename)}))

(defn- write-metadata! []
  (let [artifact-id (name java-lib)
        group-id    (namespace java-lib)
        ts          (.format (java.time.format.DateTimeFormatter/ofPattern
                               "yyyyMMddHHmmss")
                             (java.time.LocalDateTime/now))]
    (spit (metadata-path java-lib)
          (str "<metadata>\n"
               "  <groupId>" group-id "</groupId>\n"
               "  <artifactId>" artifact-id "</artifactId>\n"
               "  <versioning>\n"
               "    <release>" version "</release>\n"
               "    <versions>\n"
               "      <version>" version "</version>\n"
               "    </versions>\n"
               "    <lastUpdated>" ts "</lastUpdated>\n"
               "  </versioning>\n"
               "</metadata>\n"))))

(defn install-java [_]
  (java-release nil)
  (install-artifact! java-jar-file (format "datalevin-java-%s.jar" version))
  (install-artifact! java-pom-file (format "datalevin-java-%s.pom" version))
  (install-artifact! java-source-jar-file
                     (format "datalevin-java-%s-sources.jar" version))
  (install-artifact! java-javadoc-jar-file
                     (format "datalevin-java-%s-javadoc.jar" version))
  (write-metadata!)
  (println "Installed Java release artifacts in" local-repo)
  {:jar-file    java-jar-file
   :pom-file    java-pom-file
   :source-jar  java-source-jar-file
   :javadoc-jar java-javadoc-jar-file
   :local-repo  local-repo})
