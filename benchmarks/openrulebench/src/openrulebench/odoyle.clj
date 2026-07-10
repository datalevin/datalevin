(ns openrulebench.odoyle
  "O'Doyle Rules benchmarks for OpenRuleBench."
  (:require
   [openrulebench.core :as core]
   [openrulebench.data :as data]
   [odoyle.rules :as o]))

(defn insert-pair!
  "Insert a binary relation tuple using the tuple itself as fact id.
   O'Doyle values are single-valued per id+attr, so [x rel y] is not enough
   to represent a many-valued binary relation."
  [rel from to]
  (let [id [rel from to]]
    (o/insert! id rel true)
    (o/insert! id ::from from)
    (o/insert! id ::to to)))

(defn insert-pair
  [session rel from to]
  (let [id [rel from to]]
    (-> session
        (o/insert id rel true)
        (o/insert id ::from from)
        (o/insert id ::to to))))

;; =============================================================================
;; Transitive Closure Rules
;; =============================================================================

(def tc-rules
  (o/ruleset
    {::tc-base
     [:what
      [?edge ::edge true]
      [?edge ::from ?a]
      [?edge ::to ?b]
      :then
      (insert-pair! ::tc ?a ?b)]

     ::tc-recursive
     [:what
      [?edge ::edge true]
      [?edge ::from ?a]
      [?edge ::to ?x]
      [?tc ::tc true]
      [?tc ::from ?x]
      [?tc ::to ?b]
      :then
      (insert-pair! ::tc ?a ?b)]

     ::tc-query
     [:what
      [?tc ::tc true]
      [?tc ::from ?a]
      [?tc ::to ?b]]}))

;; =============================================================================
;; Same Generation Rules (OpenRuleBench spec)
;; sg(X, Y) :- sib(X, Y).
;; sg(X, Y) :- par(X, Z), sg(Z, Z1), par(Y, Z1).
;; =============================================================================

(def sg-rules
  (o/ruleset
    {::sg-base
     [:what
      [?sib ::sib true]
      [?sib ::from ?x]
      [?sib ::to ?y]
      :then
      (insert-pair! ::sg ?x ?y)]

     ::sg-recursive
     [:what
      [?par1 ::par true]
      [?par1 ::from ?x]
      [?par1 ::to ?z]
      [?sg ::sg true]
      [?sg ::from ?z]
      [?sg ::to ?z1]
      [?par2 ::par true]
      [?par2 ::from ?y]
      [?par2 ::to ?z1]
      :then
      (insert-pair! ::sg ?x ?y)]

     ::sg-query
     [:what
      [?sg ::sg true]
      [?sg ::from ?x]
      [?sg ::to ?y]]}))

;; =============================================================================
;; Session Creation
;; =============================================================================

(defn create-tc-session [edges]
  (reduce (fn [session [from to]]
            (insert-pair session ::edge from to))
          (reduce o/add-rule (o/->session) tc-rules)
          edges))

(defn create-sg-session [{:keys [par sib]}]
  (let [session (reduce o/add-rule (o/->session) sg-rules)
        session (reduce (fn [s [from to]]
                          (insert-pair s ::par from to))
                        session
                        par)]
    (reduce (fn [s [from to]]
              (insert-pair s ::sib from to))
            session
            sib)))

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
  (o/query-all session ::tc-query))

(defn query-sg-all [session]
  (o/query-all session ::sg-query))

;; =============================================================================
;; Benchmark Runners with Timeout
;; =============================================================================

(def ^:const timeout-ms 60000)

(defn run-with-timeout [f]
  (let [fut (future (f))]
    (try
      (let [result (deref fut timeout-ms ::timeout)]
        (if (= result ::timeout)
          (do
            (future-cancel fut)
            {:status :timeout})
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
  (let [relations (data/generate-sg-instance (keyword instance-name))
        _ (System/gc)
        start (core/now-ms)
        outcome (run-with-timeout
                  #(let [session (-> (create-sg-session relations) fire-session)]
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
