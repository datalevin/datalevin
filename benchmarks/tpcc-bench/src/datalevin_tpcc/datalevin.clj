(ns datalevin-tpcc.datalevin
  "Load the TPC-C-derived data set into Datalevin."
  (:require
   [clojure.java.io :as io]
   [datalevin-bench.host :as host]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin-tpcc.check :as check]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.generate :as g]
   [datalevin-tpcc.schema :as schema]
   [datalevin-tpcc.txns :as t])
  (:import
   [java.util Random]))

(def schema schema/schema)

(defn- load-table!
  [db table rows eid]
  (let [{:keys [types]} (c/table-specs table)
        as   (c/attrs table)
        cnt  (atom 0)
        t0   (System/nanoTime)]
    (d/fill-db
     db
     (eduction
      (mapcat
       (fn [row]
         (let [e (swap! eid inc)]
           (swap! cnt inc)
           (into []
                 (comp (map (fn [[a t v]]
                              (when (some? v)
                                (d/datom e a (c/parse-value t v)))))
                       (remove nil?))
                 (map vector as types row)))))
      rows))
    (println (format "  %-10s %9d rows  %6.2fs" table @cnt
                     (/ (- (System/nanoTime) t0) 1.0e9)))))

(defn db
  "Load a fresh TPC-C-derived Datalevin database.

  Options:
    :dir         database directory (default \"db\")
    :warehouses  number of warehouses (default 1)
    :seed        population seed (default 42)"
  [{:keys [dir warehouses seed] :or {dir "db" warehouses 1 seed 42}}]
  (println (format "Generating TPC-C data: %d warehouse(s), seed %d"
                   warehouses seed))
  (let [db-dir (io/file c/base-dir dir)
        rows   (g/population-rows seed warehouses)]
    (when (.exists db-dir)
      (println "Removing existing database at" (.getPath db-dir))
      (u/delete-files db-dir))
    (let [t0  (System/nanoTime)
          db  (d/empty-db (.getPath db-dir) schema {:closed-schema? true})
          eid (atom 0)]
      (try
        (doseq [table c/table-order]
          (load-table! db table (get rows table) eid))
        (println (format "Load time: %.2fs"
                         (/ (- (System/nanoTime) t0) 1.0e9)))
        (finally
          (d/close-db db)))))
  (println "Done. Datalevin database created.")
  (shutdown-agents)
  (System/exit 0))

;; ---------------------------------------------------------------------------
;; Driver

(defn- rint ^long [^Random r ^long lo ^long hi]
  (+ lo (.nextInt r (inc (- hi lo)))))

(defn- pick-type ^clojure.lang.Keyword [^Random r]
  (let [x (rint r 1 100)]
    (cond
      (<= x 45)  :new-order
      (<= x 88)  :payment
      (<= x 92)  :order-status
      (<= x 96)  :delivery
      :else      :stock-level)))

(defn- gen-input [r opts type]
  (g/gen-input r opts type))

(defn- do-txn [conn ^Random r opts type]
  (let [input (gen-input r opts type)
        t0    (System/nanoTime)
        res   (case type
                :new-order    (t/new-order! conn input)
                :payment      (t/payment! conn input)
                :order-status (t/order-status! conn input)
                :delivery     (t/delivery! conn input)
                :stock-level  (t/stock-level! conn input))]
    [type input res (/ (- (System/nanoTime) t0) 1.0e6)]))

(defn- percentile [sorted p]
  (when (seq sorted)
    (nth sorted (min (dec (count sorted))
                     (int (Math/floor (* (double p) (count sorted))))))))

(defn- open-bench-conn [path]
  (let [conn (d/get-conn path)
        actual (d/schema conn)
        required (concat [:warehouse/id :item/id]
                         (keys schema/primary-keys) (keys schema/secondary-keys))]
    (if (every? (fn [attr]
                  (let [expected (get schema attr)]
                    (= expected (select-keys (get actual attr) (keys expected)))))
                required)
      conn
      (do (d/close conn)
          (throw (ex-info "TPC-C schema is outdated; reload with :datalevin-db before running"
                          {:path path}))))))

(defn bench
  "Run the TPC-C-derived transaction mix against a loaded Datalevin database.

  Options:
    :dir        database directory (default \"db-w1\")
    :warehouses number of warehouses (default 1)
    :txns       measured transactions (default 10000)
    :threads    terminals (default 1)
    :warmup     warmup transactions (default 1000)
    :seed       input seed (default 42)
    :load-seed  seed used to populate the database (default 42)"
  ([opts] (bench opts {}))
  ([{:keys [dir warehouses txns threads warmup seed load-seed]
     :or   {dir "db-w1" warehouses 1 txns 10000 threads 1 warmup 1000
            seed 42 load-seed 42}}
    {:keys [do-txn pick-type district-next-o-id order-count
            measure! start-terminal open-conn close-conn payment-state payment-errors]
     :or {do-txn do-txn
          pick-type pick-type
          district-next-o-id t/district-next-o-id
          order-count t/order-count
          measure! (fn [f] (host/with-paused-media (f)))
          start-terminal future-call
          open-conn open-bench-conn
          close-conn d/close
          payment-state check/payment-state
          payment-errors check/payment-errors}}]
   (let [conn      (open-conn (.getPath (io/file c/base-dir dir)))
         opts      (assoc (g/run-constants seed load-seed) :warehouses warehouses)
         committed (atom {})
         payments  (atom {})
         customer-payments (atom {})
         lat       (atom [])
         run       (fn [^long ti ^long n record?]
                     (let [r (Random. (+ seed ti 1))
                           opts (g/terminal-options opts ti)]
                       (dotimes [_ n]
                         (let [[type input res ms] (do-txn conn r opts (pick-type r))]
                           (when record?
                             (when (= :ok (:status res))
                               (let [k [(:w input) (:d input)]]
                                 (case type
                                   :new-order
                                   (swap! committed update k (fnil inc 0))
                                   :payment
                                   (doseq [[totals location]
                                           [[payments k]
                                            [customer-payments [(:c-w res) (:c-d res)]]]]
                                     (swap! totals
                                            (fn [m]
                                              (-> m
                                                  (update-in [location :amount] (fnil + 0.0) (:amount res))
                                                  (update-in [location :count] (fnil inc 0))))))
                                   nil)))
                             (swap! lat conj [type ms (:status res)]))))))]
     (try
       (println (format "TPC-C-derived: %d warehouse(s), %d terminal(s), %d txns"
                        warehouses threads txns))
       (run 0 warmup false)
       (measure!
        (fn []
          (let [dists    (for [w (range 1 (inc warehouses)) d (range 1 11)] [w d])
                baseline (into {} (map (fn [[w d]] [[w d] (district-next-o-id conn w d)])
                                       dists))
                base-ord (into {} (map (fn [[w d]] [[w d] (order-count conn w d)])
                                       dists))
                base-pay (payment-state conn)
                t0       (System/nanoTime)
                per      (quot txns threads)
                futs     (mapv (fn [ti]
                                 (start-terminal #(run ti (+ per (if (= ti (dec threads))
                                                                   (mod txns threads)
                                                                   0))
                                                       true)))
                               (range threads))
            ;; Drain every terminal before checking totals or closing the store,
            ;; including when one terminal has failed.
                results  (mapv (fn [f] (try @f (catch Throwable e e))) futs)
                elapsed  (/ (- (System/nanoTime) t0) 1.0e9)]
            (when-let [error (some #(when (instance? Throwable %) %) results)]
              (throw error))
            (let [order-errors
                  (vec (for [[w d :as k] dists
                             [invariant base actual]
                             [[:district-next-o-id baseline (district-next-o-id conn w d)]
                              [:order-count base-ord (order-count conn w d)]]
                             :let [expected (+ (get base k) (get @committed k 0))]
                             :when (not= actual expected)]
                         {:invariant invariant :key k :expected expected :actual actual}))
                  payment-errors (payment-errors base-pay (payment-state conn)
                                                 @payments @customer-payments)
                  errors (into order-errors payment-errors)]
              (when (seq errors)
                (throw (ex-info "TPC-C accounting invariant failure" {:errors errors}))))
            (let [{:keys [new-orders committed-new-orders rolled-back-new-orders tpmc]
                   :as metrics} (c/new-order-metrics @lat elapsed)
                  by-type    (group-by first @lat)
                  stats      (into {}
                                   (for [[ty xs] by-type]
                                     (let [ms (vec (sort (map second xs)))]
                                       [ty {:count (count ms)
                                            :mean  (/ (reduce + 0.0 ms) (count ms))
                                            :p50   (percentile ms 0.50)
                                            :p95   (percentile ms 0.95)
                                            :p99   (percentile ms 0.99)}])))]
              (println "district next_o_id invariant: OK")
              (println "order count invariant: OK")
              (println "Payment accounting invariants: OK")
              (println (format "Elapsed: %.2fs  New-Orders: %d (%d committed, %d rolled back)  tpmC: %.1f"
                               elapsed new-orders committed-new-orders rolled-back-new-orders tpmc))
              (doseq [[ty {:keys [count mean p50 p95 p99]}] (sort-by key stats)]
                (println (format "  %-13s n=%-6d mean=%7.3fms p50=%7.3f p95=%7.3f p99=%7.3f"
                                 (name ty) count mean p50 p95 p99)))
              (assoc metrics :elapsed elapsed :stats stats
                     :payments @payments :customer-payments @customer-payments :invariants :ok)))))
       (finally
         (close-conn conn))))))

(defn -main [& _args]
  (bench {})
  (shutdown-agents)
  (System/exit 0))
