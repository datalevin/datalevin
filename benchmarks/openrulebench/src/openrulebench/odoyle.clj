(ns openrulebench.odoyle
  "O'Doyle Rules benchmarks for OpenRuleBench."
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [odoyle.rules :as o]))

;; =============================================================================
;; Transitive Closure Rules
;; =============================================================================

(def tc-rules
  (o/ruleset
    {::tc-base
     [:what
      [?a ::edge ?b]
      :then
      (o/insert! ?a ::tc ?b)]

     ::tc-recursive
     [:what
      [?a ::edge ?x]
      [?x ::tc ?b]
      :then
      (o/insert! ?a ::tc ?b)]}))

;; =============================================================================
;; Same Generation Rules (OpenRuleBench spec)
;; sg(X, X) :- par(_, X).              -- reflexive: any node with a parent
;; sg(X, Y) :- par(X, A), sg(A, B), par(Y, B).  -- recursive (propagates UP)
;; =============================================================================

(def sg-rules
  (o/ruleset
    {::sg-reflexive
     [:what
      [?x ::parent ?p]
      :then
      (o/insert! ?x ::sg ?x)]

     ::sg-recursive
     [:what
      [?a ::parent ?x]
      [?b ::parent ?y]
      [?a ::sg ?b]
      :then
      (o/insert! ?x ::sg ?y)]}))

;; =============================================================================
;; Session Creation
;; =============================================================================

(defn create-tc-session [edges]
  (reduce (fn [session [from to]]
            (o/insert session from ::edge to))
          (reduce o/add-rule (o/->session) tc-rules)
          edges))

(defn create-sg-session [edges]
  (reduce (fn [session [parent child]]
            (o/insert session child ::parent parent))
          (reduce o/add-rule (o/->session) sg-rules)
          edges))

(defn fire-session [session]
  (loop [s session
         iterations 0]
    (if (> iterations 100000)
      s  ; safety limit
      (let [s' (o/fire-rules s)]
        (if (= s s')
          s'
          (recur s' (inc iterations)))))))

;; =============================================================================
;; Query Functions
;; =============================================================================

(defn query-tc-all [session]
  (o/query-all session ::tc-base))

(defn query-sg-all [session]
  (o/query-all session ::sg-reflexive))

;; =============================================================================
;; Benchmark Runners with Timeout
;; =============================================================================

(def ^:const timeout-ms 60000)

(defn run-with-timeout [f]
  (let [fut (future (f))]
    (try
      (let [result (deref fut timeout-ms ::timeout)]
        (if (= result ::timeout)
          {:status :timeout}
          {:status :ok :result result}))
      (catch Exception _ {:status :error}))))

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        _ (System/gc)
        start (core/now-ms)
        outcome (run-with-timeout
                  #(let [session (-> (create-tc-session edges) fire-session)]
                     (query-tc-all session)))
        end (core/now-ms)]
    (if (= :ok (:status outcome))
      {:system "odoyle"
       :benchmark (str "tc:" instance-name)
       :time-ms (- end start)
       :result-count (count (:result outcome))
       :status :ok}
      {:system "odoyle"
       :benchmark (str "tc:" instance-name)
       :status (:status outcome)})))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-sg-instance (keyword instance-name))
        _ (System/gc)
        start (core/now-ms)
        outcome (run-with-timeout
                  #(let [session (-> (create-sg-session edges) fire-session)]
                     (query-sg-all session)))
        end (core/now-ms)]
    (if (= :ok (:status outcome))
      {:system "odoyle"
       :benchmark (str "sg:" instance-name)
       :time-ms (- end start)
       :result-count (count (:result outcome))
       :status :ok}
      {:system "odoyle"
       :benchmark (str "sg:" instance-name)
       :status (:status outcome)})))

;; =============================================================================
;; Main Entry Point
;; =============================================================================

(def default-benchmarks
  ["tc:small" "tc:medium" "sg:small"])

(defn parse-benchmark [spec]
  (let [[bench-type instance] (clojure.string/split spec #":")]
    [bench-type instance]))

(defn run-benchmark [spec]
  (let [[bench-type instance] (parse-benchmark spec)]
    (try
      (case bench-type
        "tc" (run-tc-benchmark instance)
        "sg" (run-sg-benchmark instance)
        {:system "odoyle" :benchmark spec :status :error})
      (catch OutOfMemoryError _
        (System/gc)
        {:system "odoyle" :benchmark spec :status :oom})
      (catch StackOverflowError _
        {:system "odoyle" :benchmark spec :status :error})
      (catch Exception _
        {:system "odoyle" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "odoyle" results)
    (shutdown-agents)))
