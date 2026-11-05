(ns datalevin-tpcc.datalevin
  "Load the TPC-C-derived data set into Datalevin."
  (:require
   [clojure.java.io :as io]
   [datalevin-tpcc.runner :as runner]
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
    :seed        population seed (default 42)
    :mapsize     initial map size in MiB (default 100000, as in JOB)"
  [{:keys [dir warehouses seed mapsize]
    :or {dir "db" warehouses 1 seed 42 mapsize 100000}}]
  (when-not (pos-int? mapsize)
    (throw (ex-info ":mapsize must be a positive number of MiB" {:mapsize mapsize})))
  (println (format "Generating TPC-C data: %d warehouse(s), seed %d"
                   warehouses seed))
  (let [db-dir (io/file (c/local-path dir))
        rows   (g/population-rows seed warehouses)]
    (when (.exists db-dir)
      (println "Removing existing database at" (.getPath db-dir))
      (u/delete-files db-dir))
    (let [t0  (System/nanoTime)
          db  (d/empty-db (.getPath db-dir) schema
                         {:closed-schema? true :wal? true
                          :kv-opts {:mapsize mapsize}
                          :wal-durability-profile :strict :wal-sync-mode :fsync})
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

(defn- open-bench-conn [path]
  (let [conn (d/create-conn path nil
                            {:wal? true :wal-durability-profile :strict
                             :wal-sync-mode :fsync
                             :client-opts {:pool-size 1 :read-cache-limit 0}})
        actual (d/schema conn)
        required (keys schema)]
    (if (every? (fn [attr]
                  (let [expected (get schema attr)]
                    (= expected (select-keys (get actual attr) (keys expected)))))
                required)
      (let [expected {:wal? true :wal-durability-profile :strict :wal-sync-mode :fsync}
            actual (select-keys (d/opts conn) (keys expected))]
        (if (= expected actual)
          conn
          (do (d/close conn)
              (throw (ex-info "TPC-C requires strict WAL with fsync"
                              {:expected expected :actual actual})))))
      (do (d/close conn)
          (throw (ex-info "TPC-C schema is outdated; reload with :datalevin-db before running"
                          {:path path}))))))

(defn bench
  "Run embedded or remote Datalevin WAL with one warmed connection per terminal.
  :dir is a local directory; :uri selects client/server mode. :warmup is per
  terminal and :txns is the total measured count. See runner/bench."
  ([opts] (bench opts {}))
  ([{:keys [dir uri] :or {dir "db-w1"} :as opts} dependencies]
   (let [target (or uri (.getPath (io/file (c/local-path dir))))
         open (or (:open-conn dependencies) open-bench-conn)]
     (runner/bench opts
       (merge {:do-txn do-txn :pick-type pick-type
               :district-next-o-id t/district-next-o-id :order-count t/order-count
               :payment-state check/payment-state :payment-errors check/payment-errors
               :close-conn d/close}
              dependencies {:open-conn #(open target)})))))

(defn -main [& _args]
  (bench {})
  (shutdown-agents)
  (System/exit 0))
