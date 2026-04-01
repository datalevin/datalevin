(defproject datalevin.jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen tests for Datalevin HA"
  :url "https://github.com/datalevin/datalevin"
  :license {:name "EPL-1.0"
            :url  "https://www.eclipse.org/legal/epl-1.0/"}
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [jepsen "0.3.5"]
                 [com.alipay.sofa/jraft-core "1.4.0"
                  :exclusions [org.rocksdb/rocksdbjni]]
                 [com.cognitect/transit-clj "1.0.333"]
                 [com.github.clj-easy/graal-build-time "1.0.5"]
                 [com.github.luben/zstd-jni "1.5.7-6"]
                 [org.clojars.huahaiy/nippy "3.6.2"]
                 [com.taoensso/timbre "6.5.0"]
                 [io.github.nextjournal/markdown "0.7.222"]
                 [metosin/jsonista "0.3.13"]
                 [nrepl/bencode "1.2.0"]
                 [org.babashka/sci "0.11.50"]
                 [org.bouncycastle/bcprov-jdk15on "1.70"]
                 [org.clojure/tools.cli "1.3.250"]
                 [org.clojars.huahaiy/dtlvnative-macosx-arm64 "0.18.3"]
                 [org.clojars.huahaiy/dtlvnative-linux-arm64 "0.18.3"]
                 [org.clojars.huahaiy/dtlvnative-linux-x86_64 "0.18.3"]
                 [org.clojars.huahaiy/dtlvnative-windows-x86_64 "0.18.3"]
                 [org.eclipse.collections/eclipse-collections "13.0.0"]
                 [org.roaringbitmap/RoaringBitmap "1.3.0"]
                 [me.lemire.integercompression/JavaFastPFOR "0.3.10"]
                 [org.slf4j/slf4j-api "2.0.7"]
                 [ch.qos.logback/logback-classic "1.4.4"]]
  :source-paths ["src" "../src"]
  :java-source-paths ["src/java"]
  :resource-paths ["resources" "../resources" "../target/classes"]
  :test-paths ["test"]
  :main datalevin.jepsen.cli
  :repl-options {:init-ns datalevin.jepsen.cli}
  :global-vars {*print-namespace-maps* false
                *unchecked-math*       :warn-on-boxed
                *warn-on-reflection*   true}
  :jvm-opts ["-Djava.awt.headless=true"
             "-Dclojure.compiler.direct-linking=true"
             "--enable-native-access=ALL-UNNAMED"
             "--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"])
