(ns datalevin-tpcc.postgres
  "TPC-C-derived benchmark against PostgreSQL.

  Loads the same generated data set as the Datalevin and SQLite drivers into a
  PostgreSQL database and runs the same transaction mix and invariants. Results
  include client/server transport and the connection lifecycle described in
  the README.

  PostgreSQL is optional: it requires a running server reachable through a JDBC
  URL. Connection settings come from the options map or JVM properties
  (`pg.url`, `pg.user`, `pg.pass`)."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin-tpcc.runner :as runner]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.check :as check]
   [datalevin-tpcc.generate :as g])
  (:import
   [java.sql Connection DriverManager PreparedStatement ResultSet Types]
   [java.time Instant]
   [java.util Random]))

;; ---------------------------------------------------------------------------
;; Connection

(def default-url
  (System/getProperty "pg.url" "jdbc:postgresql://localhost:5432/postgres"))

(defn- conn-opts
  "Return [url user pass], taking each from the options map first, then from the
  JVM properties, then from the environment/defaults."
  [{:keys [url user pass] :or {url default-url}}]
  [url
   (or user (System/getProperty "pg.user" (System/getenv "USER")))
   (or pass (System/getProperty "pg.pass" ""))])

(defn- get-connection ^Connection [opts]
  (DriverManager/getConnection (first opts) (second opts) (nth opts 2)))

;; ---------------------------------------------------------------------------
;; JDBC helpers

(defn- bind!
  "Bind a single value to a JDBC parameter, using the value's runtime type."
  [^PreparedStatement ps idx v]
  (let [i (int idx)]
    (cond
      (nil? v)              (.setNull ps i Types/NULL)
      (instance? String v)  (.setString ps i ^String v)
      (instance? Double v)  (.setDouble ps i (double v))
      (instance? Long v)    (.setLong ps i (long v))
      (instance? Integer v) (.setLong ps i (long v))
      (number? v)           (.setDouble ps i (double v))
      :else                 (.setString ps i (str v)))))

(defn- row->vec
  [^ResultSet rs]
  (let [n (.getColumnCount (.getMetaData rs))]
    (mapv (fn [i] (.getObject rs (int i))) (range 1 (inc n)))))

(defn- q1
  "Run a query and return the first row as a vector, or nil."
  [^Connection conn ^String sql & params]
  (with-open [ps (.prepareStatement conn sql)]
    (doseq [[i p] (map-indexed vector params)]
      (bind! ps (inc i) p))
    (with-open [rs (.executeQuery ps)]
      (when (.next rs)
        (row->vec rs)))))

(defn- qall
  "Run a query and return every row as a vector."
  [^Connection conn ^String sql & params]
  (with-open [ps (.prepareStatement conn sql)]
    (doseq [[i p] (map-indexed vector params)]
      (bind! ps (inc i) p))
    (with-open [rs (.executeQuery ps)]
      (loop [acc (transient [])]
        (if (.next rs)
          (recur (conj! acc (row->vec rs)))
          (persistent! acc))))))

(defn- exec!
  [^Connection conn ^String sql & params]
  (with-open [ps (.prepareStatement conn sql)]
    (doseq [[i p] (map-indexed vector params)]
      (bind! ps (inc i) p))
    (.executeUpdate ps)))

;; ---------------------------------------------------------------------------
;; Loader

(defn- insert-sql
  [table columns]
  (str "INSERT INTO " table " (" (s/join ", " columns) ") VALUES ("
       (s/join ", " (repeat (count columns) "?")) ")"))

(defn- set-param!
  [^PreparedStatement ps idx t v]
  (let [i (int idx)]
    (if (nil? v)
      (.setNull ps i (int (case t :long Types/BIGINT :double Types/DOUBLE
                                Types/VARCHAR)))
      (case t
        :long   (.setLong ps i (long v))
        :double (.setDouble ps i (double v))
        :string (.setString ps i (str v))))))

(defn- load-table!
  [^Connection conn table rows]
  (let [{:keys [columns types]} (c/table-specs table)
        cnt (atom 0)
        t0  (System/nanoTime)]
    (with-open [ps (.prepareStatement conn (insert-sql table columns))]
      (doseq [row rows]
        (dotimes [i (count columns)]
          (set-param! ps (inc i) (nth types i) (nth row i)))
        (.addBatch ps)
        (swap! cnt inc)
        (when (zero? (mod @cnt 10000))
          (.executeBatch ps)))
      (.executeBatch ps))
    (println (format "  %-10s %9d rows  %6.2fs" table @cnt
                     (/ (- (System/nanoTime) t0) 1.0e9)))))

(defn- exec-schema!
  [^Connection conn]
  (let [sql (-> (slurp (io/file (c/data-dir) "schema-postgres.sql"))
                (s/replace #"(?m)--[^\n]*" "")
                (s/split #";"))]
    (with-open [st (.createStatement conn)]
      (doseq [stmt sql
              :let [stmt (s/trim stmt)]
              :when (seq stmt)]
        (.execute st ^String stmt)))))

(defn- drop-tables!
  [^Connection conn]
  (with-open [st (.createStatement conn)]
    (doseq [t (reverse c/table-order)]
      (.executeUpdate st (str "DROP TABLE IF EXISTS " t " CASCADE")))))

(defn db
  "Load a fresh TPC-C-derived PostgreSQL database.

  Options:
    :url         JDBC URL (default jdbc:postgresql://localhost:5432/postgres)
    :user        PostgreSQL user (default $USER)
    :pass        PostgreSQL password
    :warehouses  number of warehouses (default 1)
    :seed        population seed (default 42)"
  [{:keys [warehouses seed] :or {warehouses 1 seed 42} :as opts}]
  (println (format "Generating TPC-C data: %d warehouse(s), seed %d"
                   warehouses seed))
  (let [c-opts (conn-opts opts)
        rows   (g/population-rows seed warehouses)
        t0     (System/nanoTime)]
    (with-open [conn (get-connection c-opts)]
      (.setAutoCommit conn false)
      (drop-tables! conn)
      (exec-schema! conn)
      (doseq [table c/table-order]
        (load-table! conn table (get rows table)))
      (.commit conn)
      ;; ANALYZE must see the committed rows; run it outside the load
      ;; transaction so the planner statistics are guaranteed to persist.
      (.setAutoCommit conn true)
      (with-open [st (.createStatement conn)]
        (.execute st "ANALYZE")))
    (println (format "Load time: %.2fs"
                     (/ (- (System/nanoTime) t0) 1.0e9))))
  (println "Done. PostgreSQL database created.")
  (shutdown-agents)
  (System/exit 0))

;; ---------------------------------------------------------------------------
;; Transactions

(defn- now-str [] (str (Instant/now)))

(defn new-order!
  [^Connection conn {:keys [w d c ol]}]
  (.setAutoCommit conn false)
  (try
    (let [w-tax   (double (first (q1 conn
                                     "SELECT w_tax FROM warehouse WHERE w_id = ?"
                                     w)))
          drow    (q1 conn
                      "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ? FOR UPDATE"
                      w d)
          _       (when (nil? drow)
                    (throw (ex-info "No such district" {:w w :d d})))
          d-tax   (double (nth drow 0))
          o-id    (long (nth drow 1))
          [c-disc c-last c-credit]
          (q1 conn "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
              w d c)
          entry (now-str)
          all-local (long (if (every? #(= w (:supply-w %)) ol) 1 0))]
      (exec! conn
             "UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?"
             (inc o-id) w d)
      (exec! conn
             "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
             o-id d w c entry nil (count ol) all-local)
      (exec! conn
             "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)"
             o-id d w)
      ;; Process and write each valid line before looking up the next item,
      ;; including when the final item will require a rollback (TPC-C 2.4.2.3).
      (loop [lines (seq ol) number 1 results []]
        (if-let [{:keys [i-id supply-w qty]} (first lines)]
          (if-let [[price item-name item-data]
                   (q1 conn "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?" i-id)]
            (let [[dist-info stock-data & srow]
                  (q1 conn
                      (str "SELECT " (c/stock-dist-column d)
                           ", s_data, s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_w_id = ? AND s_i_id = ? FOR UPDATE")
                      supply-w i-id)
                  [quantity ytd ocnt rcnt]
                  (c/stock-after-lines srow [qty] (not= supply-w w))
                  result (c/new-order-line-result
                          number i-id supply-w qty item-name (first srow) price item-data stock-data)]
              (exec! conn
                     "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?"
                     quantity ytd ocnt rcnt supply-w i-id)
              (exec! conn
                     "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                     o-id d w number i-id supply-w nil qty
                     (:amount result) dist-info)
              (recur (next lines) (inc number) (conj results result)))
            (do
              (.rollback conn)
              (c/new-order-rollback-result w d c o-id i-id c-last c-credit)))
          (let [result (c/new-order-result w d c o-id entry c-last c-credit c-disc w-tax d-tax results)]
            (.commit conn)
            result))))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))
    (finally
      (.setAutoCommit conn true))))

(defn- customer-by-name
  [^Connection conn w d last-name]
  (first (c/middle-customer
          (qall conn
                "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first, c_id"
                w d last-name))))

(defn payment!
  [^Connection conn {:keys [w d c c-w c-d last-name amount by-name?]}]
  (.setAutoCommit conn false)
  (try
    (let [c-w (or c-w w)
          c-d (or c-d d)
          [w-ytd & warehouse]
          (q1 conn "SELECT w_ytd, w_name, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = ? FOR UPDATE" w)
          [d-ytd & district]
          (q1 conn "SELECT d_ytd, d_name, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = ? AND d_id = ? FOR UPDATE" w d)
          c-id (if by-name? (customer-by-name conn c-w c-d last-name) c)
          row (when c-id
                (q1 conn "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
                    c-w c-d c-id))]
      (if-not row
        (do (.rollback conn)
            {:type :payment :status :no-customer :w w :d d})
        (let [customer (zipmap c/payment-customer-fields row)
              c-id (long (:id customer))
              date (now-str)
              new-data (when (= "BC" (:credit customer))
                         (c/bad-credit-data
                          c-id c-d c-w d w amount
                          (first (q1 conn "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                                     c-w c-d c-id))))]
          (exec! conn "UPDATE warehouse SET w_ytd = ? WHERE w_id = ?"
                 (+ (double w-ytd) (double amount)) w)
          (exec! conn "UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?"
                 (+ (double d-ytd) (double amount)) w d)
          (exec! conn
                 "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                 (- (double (:balance customer)) (double amount))
                 (+ (double (:ytd-payment customer)) (double amount))
                 (inc (long (:payment-cnt customer))) c-w c-d c-id)
          (when new-data
            (exec! conn "UPDATE customer SET c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                   new-data c-w c-d c-id))
          (exec! conn
                 "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                 c-id c-d c-w d w date (double amount)
                 (c/payment-history-data (first warehouse) (first district)))
          (.commit conn)
          (c/payment-result w d c-w c-d amount date
                            (zipmap c/payment-location-fields warehouse)
                            (zipmap c/payment-location-fields district)
                            customer new-data))))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))
    (finally (.setAutoCommit conn true))))

(defn- order-status-tx!
  [^Connection conn {:keys [w d c last-name by-name?]}]
  (let [cust (if by-name?
               (c/middle-customer
                (qall conn
                      "SELECT c_id, c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first, c_id"
                      w d last-name))
               (q1 conn
                   "SELECT c_id, c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                   w d c))]
    (if (nil? cust)
      {:type :order-status :status :no-customer}
      (let [orow (q1 conn
                     "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1"
                     w d (first cust))]
        (if orow
          (let [o-id  (long (first orow))
                lines (qall conn
                            "SELECT ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? ORDER BY ol_number"
                            w d o-id)]
            (c/order-status-result cust orow lines))
          {:type :order-status :status :no-order})))))

(defn order-status! [^Connection conn input]
  (let [isolation (.getTransactionIsolation conn)]
    (.setTransactionIsolation conn Connection/TRANSACTION_SERIALIZABLE)
    (.setAutoCommit conn false)
    (try
      (let [result (order-status-tx! conn input)]
        (.commit conn)
        result)
      (catch Exception e
        (try (.rollback conn) (catch Exception _))
        (throw e))
      (finally
        (.setAutoCommit conn true)
        (.setTransactionIsolation conn isolation)))))

(defn delivery!
  [^Connection conn {:keys [w carrier]}]
  (.setAutoCommit conn false)
  (try
    (let [dids  (mapv (fn [row] (long (first row)))
                      (qall conn
                            "SELECT d_id FROM district WHERE d_w_id = ? ORDER BY d_id"
                            w))
          plans (vec (for [did dids
                           :let [no (first (q1 conn
                                               "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id LIMIT 1 FOR UPDATE"
                                               w did))]
                           :when no]
                       [did (long no)]))]
      (if (empty? plans)
        (do
          (.commit conn)
          {:type :delivery :status :ok :delivered 0})
        (let [now (now-str)]
          (doseq [[did no] plans]
            (exec! conn
                   "DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?"
                   w did no)
            (exec! conn
                   "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
                   carrier w did no)
            (exec! conn
                   "UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
                   now w did no)
            (let [total (double (first (q1 conn
                                           "SELECT COALESCE(SUM(ol_amount), 0) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
                                           w did no)))
                  c-id  (long (first (q1 conn
                                         "SELECT o_c_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ? FOR UPDATE"
                                         w did no)))
                  crow  (q1 conn
                            "SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
                            w did c-id)
                  c-bal (double (nth crow 0))
                  c-dc  (long (nth crow 1))]
              (exec! conn
                     "UPDATE customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                     (+ c-bal total) (inc c-dc) w did c-id)))
          (.commit conn)
          {:type :delivery :status :ok :delivered (count plans)})))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))
    (finally
      (.setAutoCommit conn true))))

(defn stock-level!
  [^Connection conn {:keys [w d threshold]}]
  (let [next-id (long (first (q1 conn
                                 "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?"
                                 w d)))
        ;; TPC-C 2.8: examine the last 20 orders, [d_next_o_id - 20,
        ;; d_next_o_id), including orders already delivered.
        low     (long (first
                       (q1 conn
                           "SELECT COUNT(DISTINCT s.s_i_id) FROM stock s WHERE s.s_w_id = ? AND s.s_quantity < ? AND s.s_i_id IN (SELECT ol.ol_i_id FROM order_line ol WHERE ol.ol_w_id = ? AND ol.ol_d_id = ? AND ol.ol_o_id >= ? AND ol.ol_o_id < ?)"
                           w threshold w d (- next-id 20) next-id)))]
    {:type :stock-level :status :ok :low-stock low}))

;; ---------------------------------------------------------------------------
;; Verification helpers

(defn- district-next-o-id [^Connection conn w d]
  (long (first (q1 conn
                   "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?"
                   w d))))

(defn- order-count [^Connection conn w d]
  (long (first (q1 conn
                   "SELECT COUNT(*) FROM orders WHERE o_w_id = ? AND o_d_id = ?"
                   w d))))

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

(defn- retryable? [^java.sql.SQLException e]
  (let [s (.getSQLState e)]
    (or (= "40001" s) (= "40P01" s))))

(defn- run-with-retry
  "Run `f` until it completes or a non-retryable failure occurs, returning
  `[result elapsed-ms]`. Timing spans every attempt and the backoff between
  them, so a retried transaction does not report only its final attempt."
  [label f]
  (let [t0 (System/nanoTime)]
    (loop [attempt 0]
      (let [result (try
                     [:ok (f)]
                     (catch java.sql.SQLException e
                       (if (retryable? e) :retry (throw e))))]
        (if (= result :retry)
          (if (< attempt 50)
            (do (Thread/sleep 2) (recur (inc attempt)))
            (throw (ex-info "TPC-C transaction retry budget exhausted"
                            {:type label})))
          (let [[_ res] result]
            [res (/ (- (System/nanoTime) t0) 1.0e6)]))))))

(defn- do-txn [conn ^Random r opts type]
  (let [input   (gen-input r opts type)
        [res ms] (run-with-retry
                  type
                  #(case type
                     :new-order    (new-order! conn input)
                     :payment      (payment! conn input)
                     :order-status (order-status! conn input)
                     :delivery     (delivery! conn input)
                     :stock-level  (stock-level! conn input)))]
    [type input res ms]))

(defn- open-bench-connection [opts]
  (let [conn (get-connection opts)]
    (try
      (check/sql-schema! conn :postgres qall)
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_COMMITTED)
      (with-open [statement (.createStatement conn)]
        (.execute statement "SET synchronous_commit=on"))
      (let [settings (into {} (qall conn "SELECT name,setting FROM pg_settings WHERE name IN ('fsync','synchronous_commit','wal_sync_method','full_page_writes')"))]
        (when-not (= {"fsync" "on" "synchronous_commit" "on"
                      "wal_sync_method" "fsync" "full_page_writes" "on"} settings)
          (throw (ex-info "TPC-C requires synchronous WAL with wal_sync_method=fsync"
                          {:settings settings}))))
      conn
      (catch Throwable error (.close conn) (throw error)))))

(defn bench
  "Run PostgreSQL with one warmed connection per terminal. :warmup is per
  terminal and :txns is the total measured count. See runner/bench."
  ([opts] (bench opts {}))
  ([opts dependencies]
   (let [open (or (:get-connection dependencies) open-bench-connection)]
     (runner/bench opts
       (merge {:do-txn do-txn :pick-type pick-type
               :close-conn #(.close ^Connection %)
               :district-next-o-id district-next-o-id :order-count order-count
               :payment-state #(check/sql-payment-state % qall)
               :payment-errors check/payment-errors}
              dependencies {:open-conn #(open (conn-opts opts))})))))

(defn -main [& _args]
  (bench {})
  (shutdown-agents)
  (System/exit 0))
