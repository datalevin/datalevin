(ns openrulebench.clara
  "Clara Rules benchmarks for OpenRuleBench."
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clara.rules :refer [defrule defquery insert fire-rules mk-session query]]))

;; =============================================================================
;; Fact Types
;; =============================================================================

(defrecord Edge [from to])
(defrecord TC [from to])
(defrecord HasParent [child parent])
(defrecord SG [x y])

;; =============================================================================
;; Transitive Closure Rules
;; =============================================================================

(defrule tc-base
  "Base case: direct edge implies transitive closure."
  [Edge (= ?a from) (= ?b to)]
  =>
  (insert! (->TC ?a ?b)))

(defrule tc-recursive
  "Recursive case: edge + tc implies extended tc."
  [Edge (= ?a from) (= ?x to)]
  [TC (= ?x from) (= ?b to)]
  =>
  (insert! (->TC ?a ?b)))

(defquery get-all-tc
  "Query all TC pairs."
  []
  [TC (= ?from from) (= ?to to)])

;; =============================================================================
;; Same Generation Rules (OpenRuleBench spec)
;; sg(X, X) :- par(_, X).              -- reflexive: any node with a parent
;; sg(X, Y) :- par(X, A), sg(A, B), par(Y, B).  -- recursive (propagates UP)
;; =============================================================================

(defrule sg-reflexive
  "Base case: any node with a parent is same-gen with itself."
  [HasParent (= ?x child)]
  =>
  (insert! (->SG ?x ?x)))

(defrule sg-recursive
  "Recursive case: parents of same-gen children are same-gen."
  [HasParent (= ?a child) (= ?x parent)]
  [HasParent (= ?b child) (= ?y parent)]
  [SG (= ?a x) (= ?b y)]
  =>
  (insert! (->SG ?x ?y)))

(defquery get-all-sg
  "Query all SG pairs."
  []
  [SG (= ?x x) (= ?y y)])

;; =============================================================================
;; Session Creation
;; =============================================================================

(defn create-tc-session
  "Create a Clara session with edge facts."
  [edges]
  (let [facts (mapv (fn [[from to]] (->Edge from to)) edges)
        session (mk-session 'openrulebench.clara :cache false)]
    (-> (apply insert session facts)
        (fire-rules))))

(defn create-sg-session
  "Create a Clara session with parent-child facts."
  [edges]
  (let [facts (mapv (fn [[parent child]] (->HasParent child parent)) edges)
        session (mk-session 'openrulebench.clara :cache false)]
    (-> (apply insert session facts)
        (fire-rules))))

;; =============================================================================
;; Benchmark Runners
;; =============================================================================

(defn run-tc-benchmark
  "Run TC benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-tc-instance (keyword instance-name))
        _ (System/gc)
        [result time-ms] (core/time-once
                           (let [session (create-tc-session edges)]
                             (query session get-all-tc)))
        result-count (count result)]
    {:system "clara"
     :benchmark (str "tc:" instance-name)
     :time-ms time-ms
     :result-count result-count
     :status :ok}))

(defn run-sg-benchmark
  "Run SG benchmark on an OpenRuleBench instance. Returns result map."
  [instance-name]
  (let [edges (data/generate-sg-instance (keyword instance-name))
        _ (System/gc)
        [result time-ms] (core/time-once
                           (let [session (create-sg-session edges)]
                             (query session get-all-sg)))
        result-count (count result)]
    {:system "clara"
     :benchmark (str "sg:" instance-name)
     :time-ms time-ms
     :result-count result-count
     :status :ok}))

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
        {:system "clara" :benchmark spec :status :error})
      (catch OutOfMemoryError _
        (System/gc)
        {:system "clara" :benchmark spec :status :oom})
      (catch Exception _
        {:system "clara" :benchmark spec :status :error}))))

(defn run-benchmarks [benchmark-specs]
  (doall (map run-benchmark benchmark-specs)))

(defn -main [& args]
  (let [benchmarks (if (seq args) args default-benchmarks)
        results (run-benchmarks benchmarks)]
    (core/print-row "clara" results)))
