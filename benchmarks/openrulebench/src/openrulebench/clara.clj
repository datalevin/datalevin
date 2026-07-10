(ns openrulebench.clara
  "Clara Rules benchmarks for OpenRuleBench."
  (:require
   [clojure.string :as str]
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [clara.rules :refer [defrule defquery fire-rules insert
                        insert-unconditional! mk-session query]]))

;; =============================================================================
;; Fact Types
;; =============================================================================

(defrecord Edge [from to])
(defrecord TC [from to])
(defrecord Par [from to])
(defrecord Sib [from to])
(defrecord SG [x y])

(def ^:private seen-tc (atom #{}))
(def ^:private seen-sg (atom #{}))

(defn- insert-tc-once! [from to]
  (let [pair [from to]
        [old new] (swap-vals! seen-tc conj pair)]
    (when (not= old new)
      (insert-unconditional! (->TC from to)))))

(defn- insert-sg-once! [x y]
  (let [pair [x y]
        [old new] (swap-vals! seen-sg conj pair)]
    (when (not= old new)
      (insert-unconditional! (->SG x y)))))

;; =============================================================================
;; Transitive Closure Rules
;; =============================================================================

(defrule tc-base
  "Base case: direct edge implies transitive closure."
  [Edge (= ?a from) (= ?b to)]
  [:not [TC (= ?a from) (= ?b to)]]
  =>
  (insert-tc-once! ?a ?b))

(defrule tc-recursive
  "Recursive case: edge + tc implies extended tc."
  [Edge (= ?a from) (= ?x to)]
  [TC (= ?x from) (= ?b to)]
  [:not [TC (= ?a from) (= ?b to)]]
  =>
  (insert-tc-once! ?a ?b))

(defquery get-all-tc
  "Query all TC pairs."
  []
  [TC (= ?from from) (= ?to to)])

;; =============================================================================
;; Same Generation Rules (OpenRuleBench spec)
;; sg(X, Y) :- sib(X, Y).
;; sg(X, Y) :- par(X, Z), sg(Z, Z1), par(Y, Z1).
;; =============================================================================

(defrule sg-base
  "Base case: siblings are same-generation."
  [Sib (= ?x from) (= ?y to)]
  [:not [SG (= ?x x) (= ?y y)]]
  =>
  (insert-sg-once! ?x ?y))

(defrule sg-recursive
  "Recursive case: nodes with same-generation par-successors are same-generation."
  [Par (= ?x from) (= ?z to)]
  [SG (= ?z x) (= ?z1 y)]
  [Par (= ?y from) (= ?z1 to)]
  [:not [SG (= ?x x) (= ?y y)]]
  =>
  (insert-sg-once! ?x ?y))

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
  (reset! seen-tc #{})
  (let [facts (mapv (fn [[from to]] (->Edge from to)) edges)
        session (mk-session 'openrulebench.clara :cache false)]
    (-> (apply insert session facts)
        (fire-rules))))

(defn create-sg-session
  "Create a Clara session with par/sib facts."
  [{:keys [par sib]}]
  (reset! seen-sg #{})
  (let [facts (into (mapv (fn [[from to]] (->Par from to)) par)
                    (map (fn [[from to]] (->Sib from to)) sib))
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
  (let [relations (data/generate-sg-instance (keyword instance-name))
        _ (System/gc)
        [result time-ms] (core/time-once
                           (let [session (create-sg-session relations)]
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
  (let [[bench-type instance] (str/split spec #":")]
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
