#!/usr/bin/env clojure

;; OpenRuleBench - Benchmark runner matching OpenRuleBench paper specifications
;; See: https://www3.cs.stonybrook.edu/~kifer/TechReports/OpenRuleBench09.pdf
;;
;; Usage:
;;   ./bench.clj                    # Run default benchmarks
;;   ./bench.clj tc:small tc:medium # Run specific benchmarks
;;   ./bench.clj all                # Run all benchmarks
;;   ./bench.clj --systems datalevin # Run only Datalevin
;;   ./bench.clj --systems datalevin,sqlite tc:small
;;
;; Benchmark sizes:
;;   TC: tiny (1K edges, non-standard development scale),
;;       small (50K edges), medium (125K), large (250K), xlarge (500K), xxlarge (1M)
;;   SG: tiny (1K par+sib facts, non-standard development scale),
;;       small (6K facts), medium (24K), large (48K, non-standard extension)
;;   Join1: small (10K tuples), medium (50K), large (250K)
;;   DBLP:  small (2K papers), medium (8K), large (64K)
;;   LUBM:  lubm-1 (1 uni, ~100K triples), lubm-10 (10 unis), lubm-50 (50 unis)

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

;; =============================================================================
;; System Dependencies
;; =============================================================================

(def datalevin-deps
  (str "{:paths [\"src\"]"
       " :deps {datalevin/datalevin {:local/root \"../..\"}"
       "        org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "        com.github.luben/zstd-jni {:mvn/version \"1.5.6-9\"}"
       "        com.taoensso/nippy {:mvn/version \"3.7.0-beta1\"}"
       "        com.cognitect/transit-clj {:mvn/version \"1.0.333\"}"
       "        me.lemire.integercompression/JavaFastPFOR {:mvn/version \"0.2.1\"}"
       "        org.roaringbitmap/RoaringBitmap {:mvn/version \"1.3.0\"}"
       "        org.eclipse.collections/eclipse-collections {:mvn/version \"12.0.0\"}"
       "        org.clojars.huahaiy/dtlvnative-macosx-arm64 {:mvn/version \"0.18.2\"}"
       "        org.clojars.huahaiy/dtlvnative-linux-x86_64 {:mvn/version \"0.18.2\"}"
       "        org.clojars.huahaiy/dtlvnative-linux-arm64 {:mvn/version \"0.18.2\"}"
       "        org.clojars.huahaiy/dtlvnative-windows-x86_64 {:mvn/version \"0.18.2\"}"
       "}}"))

(def clara-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "        com.cerner/clara-rules {:mvn/version \"0.24.0\"}"
       "}}"))

(def odoyle-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "        net.sekao/odoyle-rules {:mvn/version \"1.3.1\"}"
       "}}"))

(def heap-opts
  ["-J-Xmx8g"])

(def jvm-opts
  (into heap-opts
        ["-J--add-opens=java.base/java.nio=ALL-UNNAMED"
         "-J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]))

;; =============================================================================
;; Benchmarks (OpenRuleBench standard instances)
;; =============================================================================

(def default-benchmarks
  "Quick benchmarks for testing."
  ["tc:small" "tc:medium" "sg:small" "join1:small"])

(def all-benchmarks
  "Full OpenRuleBench suite."
  [;; TC benchmarks (uniform random graphs)
   "tc:small" "tc:medium" "tc:large" "tc:xlarge"
   ;; SG benchmarks (random par/sib relations)
   "sg:small" "sg:medium" "sg:large"
   ;; Join1 benchmarks (5-way join)
   "join1:small" "join1:medium" "join1:large"
   ;; DBLP benchmarks (real-world publication data)
   "dblp:small" "dblp:medium" "dblp:large"
   ;; LUBM benchmarks (university domain)
   "lubm:lubm-1" "lubm:lubm-10" "lubm:lubm-50"])

;; Stress test benchmarks (very memory intensive)
(def stress-benchmarks
  "Large benchmarks for stress testing (require >8GB heap)."
  ["tc:xxlarge" "join1:large" "lubm:lubm-50"])

(def default-systems
  "Systems to run when --systems is not provided."
  [:datalevin :sqlite])

;; =============================================================================
;; System Runners
;; =============================================================================

(defn run-datalevin [benchmarks]
  (apply run "clojure"
         (concat jvm-opts
                 ["-Sdeps" datalevin-deps
                  "-M" "-m" "openrulebench.datalevin"]
                 benchmarks)))

(defn run-clara [benchmarks]
  (apply run "clojure"
         (concat heap-opts
                 ["-Sdeps" clara-deps
                  "-M" "-m" "openrulebench.clara"]
                 benchmarks)))

(defn run-odoyle [benchmarks]
  (apply run "clojure"
         (concat heap-opts
                 ["-Sdeps" odoyle-deps
                  "-M" "-m" "openrulebench.odoyle"]
                 benchmarks)))

;; =============================================================================
;; External System Dependencies
;; =============================================================================

(def sqlite-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "        org.xerial/sqlite-jdbc {:mvn/version \"3.47.2.0\"}"
       "}}"))

(def postgresql-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "        org.postgresql/postgresql {:mvn/version \"42.7.4\"}"
       "}}"))

(def xsb-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "}}"))

(def souffle-deps
  (str "{:paths [\"src\"]"
       " :deps {org.clojure/clojure {:mvn/version \"1.12.5\"}"
       "}}"))

;; =============================================================================
;; External System Runners
;; Require: sqlite3, psql, xsb, souffle in PATH
;; =============================================================================

(defn run-sqlite [benchmarks]
  (print "sqlite\t\t")
  (flush)
  (try
    (apply run "clojure"
           (concat ["-Sdeps" sqlite-deps
                    "-M" "-m" "openrulebench.sqlite"]
                   benchmarks))
    (catch Exception e
      (println "ERROR:" (.getMessage e)))))

(defn run-postgresql [benchmarks]
  (print "postgresql\t")
  (flush)
  (try
    (apply run "clojure"
           (concat ["-Sdeps" postgresql-deps
                    "-M" "-m" "openrulebench.postgresql"]
                   benchmarks))
    (catch Exception e
      (println "ERROR:" (.getMessage e)))))

(defn run-xsb [benchmarks]
  (print "xsb\t\t")
  (flush)
  (try
    (apply run "clojure"
           (concat ["-Sdeps" xsb-deps
                    "-M" "-m" "openrulebench.xsb"]
                   benchmarks))
    (catch Exception e
      (println "ERROR:" (.getMessage e)))))

(defn run-souffle [benchmarks]
  (print "souffle\t\t")
  (flush)
  (try
    (apply run "clojure"
           (concat ["-Sdeps" souffle-deps
                    "-M" "-m" "openrulebench.souffle"]
                   benchmarks))
    (catch Exception e
      (println "ERROR:" (.getMessage e)))))

;; =============================================================================
;; CLI
;; =============================================================================

(defn- resolve-benchmarks [args]
  (cond
    (empty? args) default-benchmarks
    (= ["all"] args) all-benchmarks
    (= ["stress"] args) stress-benchmarks
    :else args))

(defn- parse-systems [value]
  (let [systems (->> (str/split value #",")
                     (remove str/blank?)
                     (map keyword)
                     vec)]
    (when (empty? systems)
      (throw (ex-info "No systems provided for --systems" {:value value})))
    systems))

(defn- parse-args
  "Return {:systems [...], :benchmarks [...]}."
  [args]
  (loop [remaining args
         systems nil
         bench-args []]
    (if (empty? remaining)
      {:systems (or systems default-systems)
       :benchmarks (resolve-benchmarks bench-args)}
      (let [arg (first remaining)
            more (rest remaining)]
        (cond
          (or (= "--systems" arg) (= "--system" arg))
          (let [value (first more)]
            (when (nil? value)
              (throw (ex-info "Missing value for --systems" {:args args})))
            (recur (rest more) (parse-systems value) bench-args))

          :else
          (recur more systems (conj bench-args arg)))))))

;; =============================================================================
;; Main
;; =============================================================================

(binding [sh/*sh-env* (merge {} (System/getenv) {})
          sh/*sh-dir* "."]
  (let [{:keys [systems benchmarks]} (parse-args *command-line-args*)
        system-set (set systems)]

    (when (contains? system-set :datalevin)
      ;; Build Datalevin
      (println "Building Datalevin...")
      (binding [sh/*sh-dir* "../.."]
        (run "lein" "do" "clean," "javac")))

    ;; Print header
    (println)
    (print "system\t\t")
    (doseq [b benchmarks]
      (print b "\t"))
    (println)
    (println (apply str (repeat 80 "-")))

    ;; Run selected systems
    (when (contains? system-set :datalevin)
      (print "datalevin\t")
      (flush)
      (try
        (run-datalevin benchmarks)
        (catch Exception e
          (println "ERROR:" (.getMessage e)))))

    (when (contains? system-set :sqlite)
      (run-sqlite benchmarks))

    (when (contains? system-set :postgresql)
      (run-postgresql benchmarks))

    (when (contains? system-set :xsb)
      (run-xsb benchmarks))

    (when (contains? system-set :souffle)
      (run-souffle benchmarks))

    (when (contains? system-set :clara)
      (run-clara benchmarks))

    (when (contains? system-set :odoyle)
      (run-odoyle benchmarks))

    (shutdown-agents)
    (System/exit 0)))
