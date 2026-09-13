(ns datalevin-tpcc.postgres
  "TPC-C-derived benchmark against PostgreSQL.

  Loads the same generated data set as the Datalevin and SQLite drivers into a
  PostgreSQL database and runs the same transaction mix and invariants, so the
  systems can be compared directly.

  PostgreSQL is optional: it requires a running server reachable through a JDBC
  URL. Connection settings come from the options map or JVM properties
  (`pg.url`, `pg.user`, `pg.pass`)."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin-bench.host :as host]
   [datalevin-tpcc.common :as c]
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
                                     "SELECT w_tax FROM warehouse WHERE w_id = ? FOR UPDATE"
                                     w)))
          drow    (q1 conn
                      "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ? FOR UPDATE"
                      w d)
          _       (when (nil? drow)
                    (throw (ex-info "No such district" {:w w :d d})))
          d-tax   (double (nth drow 0))
          o-id    (long (nth drow 1))
          c-disc  (double (first (q1 conn
                                     "SELECT c_discount FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                                     w d c)))
          all-local (long (if (every? #(= w (:supply-w %)) ol) 1 0))]
      (exec! conn
             "UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?"
             (inc o-id) w d)
      (exec! conn
             "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
             o-id d w c (now-str) nil (count ol) all-local)
      (exec! conn
             "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)"
             o-id d w)
      ;; Process and write each valid line before looking up the next item,
      ;; including when the final item will require a rollback (TPC-C 2.4.2.3).
      (loop [lines (seq ol) number 1 subtotal 0.0]
        (if-let [{:keys [i-id supply-w qty]} (first lines)]
          (if-let [[price] (q1 conn "SELECT i_price FROM item WHERE i_id = ?" i-id)]
            (let [[dist-info & srow]
                  (q1 conn
                      (str "SELECT " (c/stock-dist-column d)
                           ", s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_w_id = ? AND s_i_id = ? FOR UPDATE")
                      supply-w i-id)
                  [quantity ytd ocnt rcnt]
                  (c/stock-after-lines srow [qty] (not= supply-w w))
                  amount (* (double qty) (double price))]
              (exec! conn
                     "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?"
                     quantity ytd ocnt rcnt supply-w i-id)
              (exec! conn
                     "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                     o-id d w number i-id supply-w nil qty
                     amount dist-info)
              (recur (next lines) (inc number) (+ subtotal amount)))
            (do
              (.rollback conn)
              {:type :new-order :status :invalid-item :w w :d d
               :o-id o-id :i-id i-id}))
          (let [amount (c/new-order-total subtotal c-disc w-tax d-tax)]
            (.commit conn)
            {:type :new-order :status :ok :w w :d d :o-id o-id
             :amount amount}))))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))
    (finally
      (.setAutoCommit conn true))))

(defn- customer-by-name
  "The middle customer by c_first with the given last name in the district."
  [^Connection conn w d last-name]
  (let [rows (qall conn
                   "SELECT c_id, rtrim(c_credit), c_data, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first, c_id FOR UPDATE"
                   w d last-name)]
    (c/middle-customer rows)))

(defn- customer-by-id
  [^Connection conn w d c]
  (q1 conn
      "SELECT c_id, rtrim(c_credit), c_data, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
      w d c))

(defn payment!
  [^Connection conn {:keys [w d c last-name amount by-name?]}]
  (.setAutoCommit conn false)
  (try
    (let [[w-ytd w-name] (q1 conn
                            "SELECT w_ytd, w_name FROM warehouse WHERE w_id = ? FOR UPDATE"
                            w)
          [d-ytd d-name] (q1 conn
                            "SELECT d_ytd, d_name FROM district WHERE d_w_id = ? AND d_id = ? FOR UPDATE"
                            w d)
          cust  (if by-name?
                  (customer-by-name conn w d last-name)
                  (customer-by-id conn w d c))]
      (if (nil? cust)
        (do
          (.rollback conn)
          {:type :payment :status :no-customer :w w :d d})
        (let [c-id     (long (nth cust 0))
              c-credit (str (nth cust 1))
              c-data   (nth cust 2)
              cb       (double (nth cust 3))
              cyp      (double (nth cust 4))
              cpc      (long (nth cust 5))
              new-data (when (= "BC" c-credit)
                         (c/bad-credit-data c-id d w d w amount c-data))]
          (exec! conn "UPDATE warehouse SET w_ytd = ? WHERE w_id = ?"
                 (+ (double w-ytd) (double amount)) w)
          (exec! conn "UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?"
                 (+ (double d-ytd) (double amount)) w d)
          (exec! conn
                 "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                 (- cb (double amount)) (+ cyp (double amount)) (inc cpc)
                 w d c-id)
          (when new-data
            (exec! conn
                   "UPDATE customer SET c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                   new-data w d c-id))
          (exec! conn
                 "INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                 c-id d w d w (now-str) (double amount)
                 (c/payment-history-data w-name d-name))
          (.commit conn)
          {:type :payment :status :ok :w w :d d :c c-id :amount amount
           :credit c-credit})))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))
    (finally
      (.setAutoCommit conn true))))

(defn order-status!
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

(defn- nurand
  "NURand(A, x, y) as defined by TPC-C, with constant `c` chosen per run."
  [r a x y c]
  (+ x (mod (+ (bit-or (.nextInt r (inc a)) (rint r x y)) c)
            (inc (- y x)))))

(defn- pick-type ^clojure.lang.Keyword [^Random r]
  (let [x (rint r 1 100)]
    (cond
      (<= x 45)  :new-order
      (<= x 88)  :payment
      (<= x 92)  :order-status
      (<= x 96)  :delivery
      :else      :stock-level)))

(defn- gen-input [^Random r {:keys [warehouses c-item c-cust c-last]} type]
  (let [w (rint r 1 warehouses)]
    (case type
      :new-order
      {:w w :d (rint r 1 10) :c (nurand r 1023 1 3000 c-cust)
       :ol (g/new-order-lines r w #(nurand r 8191 1 c/item-count c-item))}

      :payment
      (let [by-name? (<= (rint r 1 100) 60)]
        {:w w :d (rint r 1 10) :c (nurand r 1023 1 3000 c-cust)
         :by-name? by-name?
         :last-name (when by-name? (nth g/last-names (nurand r 255 0 999 c-last)))
         :amount (/ (rint r 100 500000) 100.0)})

      :order-status
      (let [by-name? (<= (rint r 1 100) 60)]
        {:w w :d (rint r 1 10) :c (nurand r 1023 1 3000 c-cust)
         :by-name? by-name?
         :last-name (when by-name? (nth g/last-names (nurand r 255 0 999 c-last)))})

      :delivery
      {:w w :carrier (rint r 1 10)}

      :stock-level
      {:w w :d (rint r 1 10) :threshold (rint r 10 20)})))

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

(defn- percentile [sorted p]
  (when (seq sorted)
    (nth sorted (min (dec (count sorted))
                     (int (Math/floor (* (double p) (count sorted))))))))

(defn bench
  "Run the TPC-C-derived transaction mix against a loaded PostgreSQL database.

  Options:
    :url         JDBC URL (default jdbc:postgresql://localhost:5432/postgres)
    :user        PostgreSQL user (default $USER)
    :pass        PostgreSQL password
    :warehouses  number of warehouses (default 1)
    :txns        measured transactions (default 10000)
    :threads     terminals (default 1)
    :warmup      warmup transactions (default 1000)
    :seed        input seed (default 42)
    :load-seed   seed used to populate the database (default 42)

  Throws with accounting errors before reporting metrics if any district's
  next order id or order count disagrees with committed New-Orders."
  [{:keys [warehouses txns threads warmup seed load-seed]
    :or   {warehouses 1 txns 10000 threads 1 warmup 1000 seed 42 load-seed 42}
    :as   opts}]
  (with-open [conn (get-connection (conn-opts opts))]
    (let [txn-opts (assoc (g/run-constants seed load-seed) :warehouses warehouses)
          committed (atom {})
          lat       (atom [])
          run       (fn [^Connection cnn ^long ti ^long n record?]
                      (let [r (Random. (+ seed ti 1))]
                        (dotimes [_ n]
                          (let [[type input res ms] (do-txn cnn r txn-opts (pick-type r))]
                            (when (and (= :new-order type) (= :ok (:status res)))
                              (swap! committed update [(:w input) (:d input)]
                                     (fnil inc 0)))
                            (when record?
                              (swap! lat conj [type ms (:status res)]))))))]
      (println (format "TPC-C-derived: %d warehouse(s), %d terminal(s), %d txns"
                       warehouses threads txns))
      ;; Each terminal uses its own connection so transactions are isolated.
      ;; A run owns its RNG, so execute the whole warmup as one batch.
      (let [warm-conn (get-connection (conn-opts opts))]
        (try (run warm-conn 0 warmup false)
             (finally (.close warm-conn))))
      (host/with-paused-media
        (let [dists    (for [w (range 1 (inc warehouses)) d (range 1 11)] [w d])
              baseline (into {} (map (fn [[w d]] [[w d] (district-next-o-id conn w d)])
                                     dists))
              base-ord (into {} (map (fn [[w d]] [[w d] (order-count conn w d)])
                                     dists))]
          (reset! committed {})
          (reset! lat [])
          (let [t0       (System/nanoTime)
                per      (quot txns threads)
                futs     (mapv (fn [ti]
                                 (future
                                   (let [c (get-connection (conn-opts opts))]
                                     (try
                                       (run c ti (+ per (if (= ti (dec threads))
                                                           (mod txns threads)
                                                           0))
                                            true)
                                       (finally (.close c))))))
                               (range threads))
                ;; Drain every terminal before leaving the run, including
                ;; when one has failed, so all terminal connections close.
                results  (mapv (fn [f] (try @f (catch Throwable e e))) futs)]
            (when-let [error (some #(when (instance? Throwable %) %) results)]
              (throw error))
            (let [elapsed    (/ (- (System/nanoTime) t0) 1.0e9)
                  errors
                  (vec (for [[w d :as k] dists
                             [invariant base actual]
                             [[:district-next-o-id baseline (district-next-o-id conn w d)]
                              [:order-count base-ord (order-count conn w d)]]
                             :let [expected (+ (get base k) (get @committed k 0))]
                             :when (not= actual expected)]
                         {:invariant invariant :key k :expected expected :actual actual}))
                  {:keys [new-orders committed-new-orders rolled-back-new-orders tpmc]
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
              (when (seq errors)
                (throw (ex-info "TPC-C accounting invariant failure" {:errors errors})))
              (println "district next_o_id invariant: OK")
              (println "order count invariant: OK")
              (println (format "Elapsed: %.2fs  New-Orders: %d (%d committed, %d rolled back)  tpmC: %.1f"
                               elapsed new-orders committed-new-orders rolled-back-new-orders tpmc))
              (doseq [[ty {:keys [count mean p50 p95 p99]}] (sort-by key stats)]
                (println (format "  %-13s n=%-6d mean=%7.3fms p50=%7.3f p95=%7.3f p99=%7.3f"
                                 (name ty) count mean p50 p95 p99)))
              (assoc metrics :elapsed elapsed :stats stats
                     :invariants :ok))))))))

(defn -main [& _args]
  (bench {})
  (shutdown-agents)
  (System/exit 0))
