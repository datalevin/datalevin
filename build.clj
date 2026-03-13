(ns build
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.build.api :as b])
  (:import [java.io File InputStream]
           [java.security MessageDigest]
           [java.util.jar JarFile]))

(def class-dir "target/classes")
(def java-release-dir "target/java-release")
(def java-artifact-dir (str java-release-dir "/classes"))
(def java-source-dir (str java-release-dir "/sources"))
(def javadoc-dir (str java-release-dir "/javadoc"))
(def java-local-repo (str java-release-dir "/m2"))
(def java-central-dir "target/java-central")
(def java-central-staging-dir (str java-central-dir "/staging"))
(def embedded-release-dir "target/embedded-release")
(def embedded-artifact-dir (str embedded-release-dir "/classes"))
(def embedded-local-repo (str embedded-release-dir "/m2"))
(def python-bindings-dir "bindings/python")
(def javascript-bindings-dir "bindings/javascript")
(def runtime-dir "target/runtime")
(def runtime-class-dir (str runtime-dir "/classes"))
(def runtime-deps-file (str runtime-dir "/deps.edn"))
(def python-jar-dir (str python-bindings-dir "/src/datalevin/jars"))
(def javascript-jar-dir (str javascript-bindings-dir "/jars"))
(def version (or (some->> (slurp "project.clj")
                          (re-find #"\(def version \"([^\"]+)\"\)")
                          second)
                 "dev"))
(def java-lib 'org.datalevin/datalevin-java)
(def embedded-lib 'datalevin/datalevin-embedded)
(def clojure-runtime-lib 'org.clojure/clojure)
(def javacpp-lib 'org.bytedeco/javacpp)
(def java-jar-file (format "target/datalevin-java-%s.jar" version))
(def java-pom-file (format "target/datalevin-java-%s.pom" version))
(def java-source-jar-file (format "target/datalevin-java-%s-sources.jar" version))
(def java-javadoc-jar-file (format "target/datalevin-java-%s-javadoc.jar" version))
(def embedded-jar-file (format "target/datalevin-embedded-%s.jar" version))
(def embedded-pom-file (format "target/datalevin-embedded-%s.pom" version))
(def runtime-jar-file (format "target/datalevin-runtime-%s.jar" version))
(def java-central-bundle-file
  (format "%s/datalevin-java-%s-central-bundle.zip" java-central-dir version))
(def deps-config (edn/read-string (slurp "deps.edn")))
(def runtime-deps (:deps deps-config))
(def basis (b/create-basis {:project "deps.edn"}))
(def release-runtime-excluded-deps
  '#{babashka/babashka.pods
     nrepl/bencode
     org.babashka/sci
     org.clojure/tools.cli
     org.bouncycastle/bcpkix-jdk15on
     org.bouncycastle/bcprov-jdk15on
     com.alipay.sofa/jraft-core})
(def release-runtime-source-excludes
  ["pod"
   "datalevin/ha"
   "datalevin/ha.clj"
   "datalevin/interpret.clj"
   "datalevin/main.clj"
   "datalevin/server.clj"])
(def release-runtime-class-excludes
  ["datalevin/ha"])
(def runtime-excluded-deps
  release-runtime-excluded-deps)
(def runtime-source-excludes
  release-runtime-source-excludes)
(def runtime-class-excludes
  release-runtime-class-excludes)
(def runtime-native-libs
  {"linux-x86_64" 'org.clojars.huahaiy/dtlvnative-linux-x86_64
   "linux-arm64" 'org.clojars.huahaiy/dtlvnative-linux-arm64
   "macosx-arm64" 'org.clojars.huahaiy/dtlvnative-macosx-arm64
   "windows-x86_64" 'org.clojars.huahaiy/dtlvnative-windows-x86_64})
(def runtime-zstd-excludes
  {"linux-x86_64" ["linux/(?!amd64/).*"
                   "win/.*"
                   "darwin/.*"
                   "freebsd/.*"
                   "aix/.*"]
   "linux-arm64"  ["linux/(?!aarch64/).*"
                   "win/.*"
                   "darwin/.*"
                   "freebsd/.*"
                   "aix/.*"]
   "macosx-arm64" ["darwin/(?!aarch64/).*"
                   "linux/.*"
                   "win/.*"
                   "freebsd/.*"
                   "aix/.*"]
   "windows-x86_64" ["win/(?!amd64/).*"
                     "linux/.*"
                     "darwin/.*"
                     "freebsd/.*"
                     "aix/.*"]})
(def runtime-all-zstd-excludes
  ["linux/(?!amd64/|aarch64/).*"
   "win/(?!amd64/).*"
   "darwin/(?!aarch64/).*"
   "freebsd/(?!amd64/).*"
   "aix/.*"])
(def binding-jar-dirs
  [python-jar-dir
   javascript-jar-dir])
(def scm {:connection          "scm:git:https://github.com/datalevin/datalevin.git"
          :developerConnection "scm:git:git@github.com:datalevin/datalevin.git"
          :tag                 (str "v" version)
          :url                 "https://github.com/datalevin/datalevin"})
(def bundled-native-libs
  '#{org.clojars.huahaiy/dtlvnative-macosx-arm64
     org.clojars.huahaiy/dtlvnative-linux-arm64
     org.clojars.huahaiy/dtlvnative-linux-x86_64
     org.clojars.huahaiy/dtlvnative-windows-x86_64})
(defn- dissoc-libs
  [deps libs]
  (reduce dissoc deps libs))
(def release-pom-deps
  (assoc (-> runtime-deps
             (dissoc-libs release-runtime-excluded-deps)
             (dissoc-libs bundled-native-libs))
         javacpp-lib
         {:mvn/version "1.5.13"}))
(def java-pom-deps release-pom-deps)
(def embedded-pom-deps release-pom-deps)
(def developers
  [{:id "huahaiy"
    :name "Huahai Yang"
    :email "huahai.yang@gmail.com"}])
(def checksum-algorithms
  [["MD5" "md5"]
   ["SHA-1" "sha1"]
   ["SHA-256" "sha256"]
   ["SHA-512" "sha512"]])

(defn- existing-dirs
  [dirs]
  (->> dirs
       (filter #(.exists (File. ^String %)))
       vec))

(defn- delete-under-root!
  [root paths]
  (doseq [path paths]
    (b/delete {:path (str root "/" path)})))

(defn- platform-arg->string
  [value]
  (cond
    (nil? value) nil
    (keyword? value) (name value)
    (symbol? value) (name value)
    :else (str value)))

(defn- detect-runtime-native-platform []
  (let [os   (str/lower-case (System/getProperty "os.name" ""))
        arch (str/lower-case (System/getProperty "os.arch" ""))]
    (cond
      (and (str/includes? os "linux")
           (#{"x86_64" "amd64"} arch))
      "linux-x86_64"

      (and (str/includes? os "linux")
           (#{"aarch64" "arm64"} arch))
      "linux-arm64"

      (and (or (str/includes? os "mac")
               (str/includes? os "darwin"))
           (#{"aarch64" "arm64"} arch))
      "macosx-arm64"

      (and (str/includes? os "win")
           (#{"x86_64" "amd64"} arch))
      "windows-x86_64"

      :else
      nil)))

(defn- normalize-runtime-native-platform
  [native-platform]
  (let [platform (or (platform-arg->string native-platform)
                     (detect-runtime-native-platform))]
    (when-not platform
      (throw (ex-info "Unsupported host platform for Datalevin runtime jar."
                      {:os   (System/getProperty "os.name")
                       :arch (System/getProperty "os.arch")})))
    (if (= platform "all")
      :all
      (do
        (when-not (contains? runtime-native-libs platform)
          (throw (ex-info "Unsupported Datalevin runtime native platform."
                          {:native-platform platform
                           :supported       (sort (keys runtime-native-libs))})))
        platform))))

(defn- runtime-deps-for
  [native-platform]
  (let [platform    (normalize-runtime-native-platform native-platform)
        native-libs (if (= platform :all)
                      bundled-native-libs
                      #{(runtime-native-libs platform)})
        deps        (reduce dissoc runtime-deps runtime-excluded-deps)
        deps        (reduce dissoc deps bundled-native-libs)]
    (merge deps (select-keys runtime-deps native-libs))))

(defn- runtime-uber-excludes
  [native-platform]
  (let [platform (normalize-runtime-native-platform native-platform)]
    (if (= platform :all)
      runtime-all-zstd-excludes
      (runtime-zstd-excludes platform))))

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
                embedded-release-dir
                java-central-dir
                java-jar-file
                java-pom-file
                java-source-jar-file
                java-javadoc-jar-file
                embedded-jar-file
                embedded-pom-file
                runtime-dir
                runtime-jar-file
                java-central-bundle-file]]
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

(defn- dependency->xml [[lib dep]]
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

(defn- developer->xml [{:keys [id name email]}]
  (str "    <developer>\n"
       (when id
         (str "      <id>" id "</id>\n"))
       "      <name>" name "</name>\n"
       (when email
         (str "      <email>" email "</email>\n"))
       "    </developer>\n"))

(defn- pom-xml
  [lib-sym {:keys [title description deps]}]
  (str "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
       "<project xmlns=\"http://maven.apache.org/POM/4.0.0\"\n"
       "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
       "         xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd\">\n"
       "  <modelVersion>4.0.0</modelVersion>\n"
       "  <groupId>" (namespace lib-sym) "</groupId>\n"
       "  <artifactId>" (name lib-sym) "</artifactId>\n"
       "  <version>" version "</version>\n"
       "  <packaging>jar</packaging>\n"
       "  <name>" title "</name>\n"
       "  <description>" description "</description>\n"
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
       "  <developers>\n"
       (apply str (map developer->xml developers))
       "  </developers>\n"
       "  <dependencies>\n"
       (->> deps
            (sort-by (comp str key))
            (map dependency->xml)
            (apply str))
       "  </dependencies>\n"
       "</project>\n"))

(defn- write-pom-files!
  [artifact-dir lib-sym pom-file {:keys [title description deps]}]
  (let [pom-dir    (format "%s/META-INF/maven/%s/%s"
                           artifact-dir
                           (namespace lib-sym)
                           (name lib-sym))
        pom-path   (str pom-dir "/pom.xml")
        props-file (str pom-dir "/pom.properties")
        pom-xml    (pom-xml lib-sym {:title       title
                                     :description description
                                     :deps        deps})]
    (.mkdirs (File. pom-dir))
    (spit pom-path pom-xml)
    (spit props-file
          (str "groupId=" (namespace lib-sym) "\n"
               "artifactId=" (name lib-sym) "\n"
               "version=" version "\n"))
    (b/delete {:path pom-file})
    (spit pom-file pom-xml)))

(defn- write-java-poms! []
  (write-pom-files!
    java-artifact-dir
    java-lib
    java-pom-file
    {:title       (name java-lib)
     :description "A simple, fast and versatile Datalog database"
     :deps        java-pom-deps}))

(defn- write-embedded-poms! []
  (write-pom-files!
    embedded-artifact-dir
    embedded-lib
    embedded-pom-file
    {:title       (name embedded-lib)
     :description (str "Embedded-only Datalevin runtime with bundled native "
                       "libraries and remote client support")
     :deps        embedded-pom-deps}))

(defn- native-jar-paths []
  (->> (:classpath-roots basis)
       (filter #(re-find #"/dtlvnative-[^/]+-\d[^/]*\.jar$" %))
       sort))

(defn- copy-jar-prefix!
  [jar-path prefix target-dir]
  (with-open [jar (JarFile. jar-path)]
    (doseq [entry (enumeration-seq (.entries jar))
            :let [entry-name (.getName entry)]
            :when (and (not (.isDirectory entry))
                       (str/starts-with? entry-name prefix))]
      (let [target-file (File. target-dir entry-name)]
        (.mkdirs (.getParentFile target-file))
        (with-open [in (.getInputStream jar entry)
                    out (io/output-stream target-file)]
          (io/copy in out))))))

(defn- copy-bundled-native-payloads!
  [target-dir]
  (doseq [jar-path (native-jar-paths)]
    (copy-jar-prefix! jar-path "datalevin/dtlvnative/" target-dir)))

(defn- prep-trimmed-artifact!
  [target-dir]
  (compile-java nil)
  (b/delete {:path target-dir})
  (b/copy-dir {:src-dirs   (existing-dirs ["src" "resources" class-dir])
               :target-dir target-dir})
  ;; Keep release jars free of embedded Java sources.
  (b/delete {:path (str target-dir "/java")})
  ;; Embedded consumers do not need the CLI, pod entrypoint, or HA/server runtime.
  (delete-under-root! target-dir release-runtime-source-excludes)
  (delete-under-root! target-dir release-runtime-class-excludes))

(defn- prep-java-artifact! []
  (prep-trimmed-artifact! java-artifact-dir)
  (copy-bundled-native-payloads! java-artifact-dir)
  (write-java-poms!))

(defn- prep-embedded-artifact! []
  (prep-trimmed-artifact! embedded-artifact-dir)
  (copy-bundled-native-payloads! embedded-artifact-dir)
  (write-embedded-poms!))

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
  ;; Keep the published sources aligned with the trimmed runtime jar.
  (delete-under-root! java-source-dir release-runtime-source-excludes)
  (delete-under-root! java-source-dir release-runtime-class-excludes)
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
  [local-repo lib-sym]
  (format "%s/%s/%s/%s"
          local-repo
          (str/replace (namespace lib-sym) "." "/")
          (name lib-sym)
          version))

(defn- metadata-path
  [local-repo lib-sym]
  (format "%s/%s/%s/maven-metadata-local.xml"
          local-repo
          (str/replace (namespace lib-sym) "." "/")
          (name lib-sym)))

(defn- install-artifact!
  [local-repo lib-sym src filename]
  (b/copy-file {:src src
                :target (str (repo-path local-repo lib-sym) "/" filename)}))

(defn- write-metadata!
  [local-repo lib-sym]
  (let [artifact-id (name lib-sym)
        group-id    (namespace lib-sym)
        ts          (.format (java.time.format.DateTimeFormatter/ofPattern
                               "yyyyMMddHHmmss")
                             (java.time.LocalDateTime/now))]
    (spit (metadata-path local-repo lib-sym)
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
  (install-artifact! java-local-repo
                     java-lib
                     java-jar-file
                     (format "datalevin-java-%s.jar" version))
  (install-artifact! java-local-repo
                     java-lib
                     java-pom-file
                     (format "datalevin-java-%s.pom" version))
  (install-artifact! java-local-repo
                     java-lib
                     java-source-jar-file
                     (format "datalevin-java-%s-sources.jar" version))
  (install-artifact! java-local-repo
                     java-lib
                     java-javadoc-jar-file
                     (format "datalevin-java-%s-javadoc.jar" version))
  (write-metadata! java-local-repo java-lib)
  (println "Installed Java release artifacts in" java-local-repo)
  {:jar-file    java-jar-file
   :pom-file    java-pom-file
   :source-jar  java-source-jar-file
   :javadoc-jar java-javadoc-jar-file
   :local-repo  java-local-repo})

(defn embedded-jar [_]
  (prep-embedded-artifact!)
  (b/jar {:class-dir embedded-artifact-dir
          :jar-file  embedded-jar-file
          :manifest  {"Implementation-Title"   "Datalevin Embedded"
                      "Implementation-Version" version}})
  (println "Generated Datalevin embedded jar at" embedded-jar-file)
  {:jar-file embedded-jar-file
   :pom-file embedded-pom-file})

(defn install-embedded [_]
  (embedded-jar nil)
  (install-artifact! embedded-local-repo
                     embedded-lib
                     embedded-jar-file
                     (format "datalevin-embedded-%s.jar" version))
  (install-artifact! embedded-local-repo
                     embedded-lib
                     embedded-pom-file
                     (format "datalevin-embedded-%s.pom" version))
  (write-metadata! embedded-local-repo embedded-lib)
  (println "Installed embedded release artifacts in" embedded-local-repo)
  {:jar-file   embedded-jar-file
   :pom-file   embedded-pom-file
   :local-repo embedded-local-repo})

(defn- prep-runtime-artifact! []
  (prep-trimmed-artifact! runtime-class-dir))

(defn- write-runtime-deps!
  [native-platform]
  (.mkdirs (File. runtime-dir))
  (spit runtime-deps-file
        (pr-str {:paths []
                 :deps  (runtime-deps-for native-platform)})))

(defn- runtime-basis
  [native-platform]
  (write-runtime-deps! native-platform)
  (b/create-basis {:project runtime-deps-file}))

(defn runtime-jar
  [{:keys [native-platform] :as _opts}]
  (prep-runtime-artifact!)
  (b/delete {:path runtime-jar-file})
  (b/uber {:class-dir runtime-class-dir
           :uber-file runtime-jar-file
           :basis     (runtime-basis native-platform)
           :exclude   (runtime-uber-excludes native-platform)})
  (println "Generated Datalevin runtime jar at" runtime-jar-file)
  {:jar-file runtime-jar-file})

(defn- delete-vendored-runtime-jars!
  [jar-dir]
  (when (.exists (File. jar-dir))
    (doseq [existing (file-seq (File. jar-dir))
            :when (and (.isFile ^File existing)
                       (or (str/starts-with? (.getName ^File existing) "datalevin-java-")
                           (str/starts-with? (.getName ^File existing) "datalevin-python-runtime-")
                           (str/starts-with? (.getName ^File existing) "datalevin-runtime-"))
                       (str/ends-with? (.getName ^File existing) ".jar"))]
      (b/delete {:path (.getPath ^File existing)}))))

(defn vendor-jar
  [{:keys [native-platform] :as opts}]
  (runtime-jar opts)
  (doseq [jar-dir binding-jar-dirs]
    (.mkdirs (File. jar-dir))
    (delete-vendored-runtime-jars! jar-dir)
    (let [target (format "%s/datalevin-runtime-%s.jar" jar-dir version)]
      (b/copy-file {:src runtime-jar-file
                    :target target})
      (println "Vendored Datalevin runtime jar at" target)))
  {:jar-file        runtime-jar-file
   :native-platform (normalize-runtime-native-platform native-platform)})

(defn python-runtime-jar
  [opts]
  (runtime-jar opts))

(defn vendor-python-jar
  [opts]
  (vendor-jar opts))

(defn- digest-file
  [algorithm path]
  (let [digest (MessageDigest/getInstance algorithm)
        buffer (byte-array 8192)]
    (with-open [^InputStream in (io/input-stream path)]
      (loop []
        (let [read (.read in buffer)]
          (when (pos? read)
            (.update digest buffer 0 read)
            (recur)))))
    (apply str (map #(format "%02x" (bit-and % 0xff)) (.digest digest)))))

(defn- sign-artifact!
  [artifact]
  (let [signature (str artifact ".asc")
        gpg-home  (System/getenv "JAVA_GPG_HOME")
        key-id    (System/getenv "JAVA_GPG_KEY_ID")
        command   (vec (concat ["gpg" "--batch" "--yes" "--armor"]
                               (when (seq gpg-home)
                                 ["--homedir" gpg-home])
                               (when (seq key-id)
                                 ["--local-user" key-id])
                               ["--detach-sign"
                                "--output" signature
                                artifact]))]
    (run-process! command)
    signature))

(defn- write-checksums!
  [artifact]
  (doseq [[algorithm extension] checksum-algorithms]
    (spit (str artifact "." extension)
          (str (digest-file algorithm artifact) "\n"))))

(defn- central-artifact-dir []
  (str java-central-staging-dir
       "/"
       (str/replace (namespace java-lib) "." "/")
       "/"
       (name java-lib)
       "/"
       version))

(defn- bundle-artifacts!
  [{:keys [sign?]}]
  (java-release nil)
  (let [artifact-dir (central-artifact-dir)
        artifacts    [{:src java-jar-file
                       :name (format "datalevin-java-%s.jar" version)}
                      {:src java-pom-file
                       :name (format "datalevin-java-%s.pom" version)}
                      {:src java-source-jar-file
                       :name (format "datalevin-java-%s-sources.jar" version)}
                      {:src java-javadoc-jar-file
                       :name (format "datalevin-java-%s-javadoc.jar" version)}]]
    (b/delete {:path java-central-dir})
    (.mkdirs (File. artifact-dir))
    (doseq [{:keys [src name]} artifacts]
      (b/copy-file {:src src
                    :target (str artifact-dir "/" name)}))
    (let [artifact-paths (->> artifacts
                              (mapv #(str artifact-dir "/" (:name %))))
          files-to-hash  (if sign?
                           (into artifact-paths (map sign-artifact! artifact-paths))
                           artifact-paths)]
      (doseq [artifact files-to-hash]
        (write-checksums! artifact))
      {:artifact-dir artifact-dir
       :artifacts    artifact-paths})))

(defn central-java-bundle
  [{:keys [sign]
    :or   {sign true}}]
  (bundle-artifacts! {:sign? sign})
  (b/delete {:path java-central-bundle-file})
  (run-process!
    ["jar"
     "--create"
     "--file" java-central-bundle-file
     "-C" java-central-staging-dir
     "."])
  (println "Generated Maven Central bundle at" java-central-bundle-file)
  {:bundle-file  java-central-bundle-file
   :artifact-dir (central-artifact-dir)
   :signed?      sign})
