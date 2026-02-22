#!/usr/bin/env clojure

(require
  '[clojure.java.io :as io]
  '[clojure.java.shell :as sh]
  '[clojure.string :as str])

(defn sh [& cmd]
  (let [res (apply sh/sh cmd)]
    (when (not= 0 (:exit res))
      (throw (ex-info "ERROR" res)))
    (str/trim (:out res))))

(defn copy [^java.io.InputStream input ^java.io.Writer output]
  (let [^"[C" buffer (make-array Character/TYPE 1024)
        in           (java.io.InputStreamReader. input "UTF-8")
        w            (java.io.StringWriter.)]
    (loop []
      (let [size (.read in buffer 0 (alength buffer))]
        (if (pos? size)
          (do (.write output buffer 0 size)
              (.flush output)
              (.write w buffer 0 size)
              (recur))
          (str w))))))

(defn run [& cmd]
  (let [cmd  (remove nil? cmd)
        proc (.exec (Runtime/getRuntime)
                    (into-array String cmd)
                    (@#'sh/as-env-strings sh/*sh-env*)
                    (io/as-file sh/*sh-dir*))
        out  (promise)]
    (with-open [stdout (.getInputStream proc)
                stderr (.getErrorStream proc)]
      (future (deliver out (copy stdout *out*)))
      (future (copy stderr *err*))
      (.close (.getOutputStream proc))
      (let [code (.waitFor proc)]
        (when (not= code 0)
          (throw (ex-info "ERROR" {:cmd cmd :code code})))
        @out))))


(def opts
  (loop [opts {:rebuild    true
               :versions   []
               :benchmarks []}
         args *command-line-args*]
    (if-some [arg (first args)]
      (cond
        (= "rebuild" arg)
        (recur (assoc opts :rebuild true) (next args))

        (re-matches #"(datalevin|datascript|datomic)" arg)
        (recur (update opts :versions conj ["latest" arg]) (next args))

        (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)" arg)
        (recur (update opts :versions conj [arg "datalevin"]) (next args))

        (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)-(datalevin|datascript|datomic)" arg)
        (let [[_ version vm] (re-matches #"(\d+\.\d+\.\d+|[0-9a-fA-F]{40}|latest)-(datalevin|datascript|datomic)" arg)]
          (recur (update opts :versions conj [version vm]) (next args)))

        :else
        (recur (update opts :benchmarks conj arg) (next args)))
      opts)))


(defn run-benchmarks [version vm benchmarks]
  (case vm
    "datalevin"
    (apply run "clojure"
           "-J--add-opens=java.base/java.nio=ALL-UNNAMED"
           "-J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
           "-Sdeps"
           (cond
             (= "latest" version)
             (str "{:paths [\"src\"]"
                  ":deps {datalevin/datalevin {:local/root \"../..\"}
                  org.clojure/clojure   {:mvn/version \"1.12.3\"}
                  com.github.luben/zstd-jni {:mvn/version \"1.5.7-6\"}
                  com.taoensso/nippy    {:mvn/version \"3.6.0\"}
                  com.cognitect/transit-clj {:mvn/version \"1.0.333\"}
                  me.lemire.integercompression/JavaFastPFOR {:mvn/version \"0.3.8\"}
                  org.roaringbitmap/RoaringBitmap {:mvn/version \"1.3.0\"}
                  org.eclipse.collections/eclipse-collections {:mvn/version \"13.0.0\"}
                  org.clojars.huahaiy/dtlvnative-windows-x86_64 {:mvn/version \"0.16.0\"}
                  org.clojars.huahaiy/dtlvnative-linux-x86_64 {:mvn/version \"0.16.0\"}
                  org.clojars.huahaiy/dtlvnative-linux-arm64 {:mvn/version \"0.16.0\"}
                  org.clojars.huahaiy/dtlvnative-macosx-arm64 {:mvn/version \"0.16.0\"}
                  }}"
                  )

             (re-matches #"\d+\.\d+\.\d+" version)
             (str "{:paths [\"src\"]"
                  "    :deps {datalevin/datalevin {:mvn/version \"" version "\"}}}")

             (re-matches #"[0-9a-fA-F]{40}" version)
             (str "{:paths [\"src\"]"
                  "    :deps {datalevin/datalevin {:git/url \"https://github.com/juji-io\" :sha \"" version "\"}}}"))
           "-M" "-m" "datalevin-bench.core"
           benchmarks)

    "datascript"
    (apply run "clojure"
           "-Sdeps"
           (str "{"
                " :paths [\"src\"]"
                " :deps {datascript/datascript {:mvn/version \"" (if (= "latest" version) "1.7.8" version) "\"}}"
                "}")
           "-M" "-m" "datascript-bench.core"
           benchmarks)

    "datomic"
    (apply run "clojure"
           "-Sdeps"
           (str "{"
                " :paths [\"src\"]"
                " :deps {com.datomic/peer {:mvn/version \"" (if (= "latest" version) "1.0.7469" version) "\"}}"
                "}")
           "-M" "-m" "datomic-bench.core"
           benchmarks)
    ))


(def default-benchmarks
  [
   "q1"
   "q2"
   "q3"
   "q4"
   ])


(def default-versions
  [
   ["latest" "datalevin"]
   ["latest" "datomic"]
   ["latest" "datascript"]
   ])


(binding [sh/*sh-env* (merge {} (System/getenv) {})
          sh/*sh-dir* "."]
  (let [{:keys [rebuild benchmarks versions]} opts]
    (when rebuild
      (binding [sh/*sh-dir* "../.."]
        (run "lein" "do" "clean," "javac")))
    (let [benchmarks (if (empty? benchmarks) default-benchmarks benchmarks)
          versions   (if (empty? versions)   default-versions    versions)]
      (print "version   \t")
      (doseq [b benchmarks] (print b "\t"))
      (println)
      (doseq [[version vm] versions]
        (print (str version "-" vm) "\t")
        (flush)
        (run-benchmarks version vm benchmarks))
      (shutdown-agents)
      (System/exit 0))))
