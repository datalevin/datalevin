(def version "0.10.5")

(defproject datalevin version
  :description "A simple, fast and versatile Datalog database"
  :url "https://github.com/datalevin/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :managed-dependencies
  [[babashka/babashka.pods "0.2.0"]
   [com.cognitect/transit-clj "1.0.333"]
   [com.github.clj-easy/graal-build-time "1.0.5"]
   [com.taoensso/nippy "3.6.0"]
   [com.taoensso/timbre "6.5.0"]
   [joda-time/joda-time "2.14.0"]
   [me.lemire.integercompression/JavaFastPFOR "0.3.10"]
   [metosin/jsonista "0.3.13"]
   [io.github.nextjournal/markdown "0.7.222"]
   [nrepl/bencode "1.2.0"]
   [org.babashka/sci "0.11.50"]
   [org.bouncycastle/bcprov-jdk15on "1.70"]
   [org.clojure/clojure "1.12.4"]
   [org.clojure/tools.cli "1.3.250"]
   [org.clojure/test.check "1.1.3"]
   [org.eclipse.collections/eclipse-collections "13.0.0"]
   [org.clojars.huahaiy/dtlvnative-macosx-arm64 "0.16.0"]
   [org.clojars.huahaiy/dtlvnative-linux-arm64 "0.16.0"]
   [org.clojars.huahaiy/dtlvnative-linux-x86_64 "0.16.0"]
   [org.clojars.huahaiy/dtlvnative-windows-x86_64 "0.16.0"]
   [org.roaringbitmap/RoaringBitmap "1.3.0"]
   [com.github.luben/zstd-jni "1.5.7-6"]]
  :dependencies
  [[org.clojure/clojure :scope "provided"]
   [org.clojars.huahaiy/dtlvnative-macosx-arm64]
   [org.clojars.huahaiy/dtlvnative-linux-arm64]
   [org.clojars.huahaiy/dtlvnative-linux-x86_64]
   [org.clojars.huahaiy/dtlvnative-windows-x86_64]
   [com.github.clj-easy/graal-build-time]
   [com.taoensso/nippy]
   [com.taoensso/timbre]
   [org.babashka/sci]
   [nrepl/bencode]
   [org.clojure/tools.cli]
   [org.bouncycastle/bcprov-jdk15on]
   [metosin/jsonista]
   [io.github.nextjournal/markdown]
   [babashka/babashka.pods]
   [org.roaringbitmap/RoaringBitmap]
   [org.eclipse.collections/eclipse-collections]
   [me.lemire.integercompression/JavaFastPFOR]
   [com.cognitect/transit-clj]
   [com.github.luben/zstd-jni]
   ]
  :source-paths ["src" "test"]
  :java-source-paths ["src/java"]
  ;; :aot :all
  :profiles
  {:uberjar        {:main datalevin.main
                    :aot  [datalevin.main]}
   :native-uberjar {:aot          [datalevin.main],
                    :uberjar-name "main.uberjar.jar"}
   :test0-uberjar  {:main         datalevin.test0
                    :aot          [datalevin.test0],
                    :dependencies [[org.clojure/test.check]
                                   [joda-time/joda-time]]
                    :jar-inclusions
                    [#"_test" #"\/test\/" #"test\d" #"\/data\.json" #"all\.json"
                     #"\.csv" #"\.edn" #"\.txt"]
                    :uberjar-name "test0.uberjar.jar"}
   :test1-uberjar  {:main         datalevin.test1
                    :aot          [datalevin.test1],
                    :dependencies [[org.clojure/test.check]
                                   [joda-time/joda-time]]
                    :jar-inclusions
                    [#"_test" #"\/test\/" #"test\d" #"\/data\.json" #"all\.json"
                     #"\.csv" #"\.edn"  #"\.txt"]
                    :uberjar-name "test1.uberjar.jar"}
   :dev            {:main datalevin.test0
                    :dependencies
                    [[org.clojure/test.check]
                     [joda-time/joda-time]]}}
  :global-vars {*print-namespace-maps* false
                *unchecked-math*       :warn-on-boxed
                *warn-on-reflection*   true}
  :jvm-opts ["-XX:+IgnoreUnrecognizedVMOptions"
             "-Xlint:all"
             "-Dclojure.compiler.direct-linking=true"
             "--enable-native-access=ALL-UNNAMED"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
  :javac-options ["--release" "17"]
  :jar-exclusions [#"_test" #"\/test\/" #"test\d" #"\/data\.json" #"all\.json"
                   #"\.csv" #"\.edn" #"\.java"
                   #"\.md" #"\.txt"]
  :uberjar-exclusions [#"pod.huahaiy.datalevin-test"]
  :deploy-repositories [["clojars" {:url           "https://repo.clojars.org"
                                    :username      :env/clojars_username
                                    :password      :env/clojars_password
                                    :sign-releases false}]])
