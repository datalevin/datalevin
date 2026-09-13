(ns datalevin-tpcc.sqlite
  "TPC-C-derived benchmark against SQLite.

  Loads the same generated data set as the Datalevin driver into a SQLite file
  and runs the same transaction mix and invariants, so the two can be compared
  directly."
  (:require
   [clojure.java.io :as io]
   [clojure.string :as s]
   [datalevin-tpcc.common :as c]
   [datalevin-tpcc.generate :as g])
  (:import
   [java.sql Connection DriverManager PreparedStatement ResultSet Types]
   [java.time Instant]
   [java.util Random]))

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
      (.setNull ps i (int (case t :long Types/INTEGER :double Types/REAL
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
  (let [sql (-> (slurp (io/file (c/data-dir) "schema-sqlite.sql"))
                (s/replace #"(?m)--[^\n]*" "")
                (s/split #";"))]
    (with-open [st (.createStatement conn)]
      (doseq [stmt sql
              :let [stmt (s/trim stmt)]
              :when (seq stmt)]
        (.execute st ^String stmt)))))

(defn db
  "Load a fresh TPC-C-derived SQLite database.

  Options:
    :path        database file (default \"sqlite.db\")
    :warehouses  number of warehouses (default 1)
    :seed        population seed (default 42)"
  [{:keys [path warehouses seed] :or {path "sqlite.db" warehouses 1 seed 42}}]
  (println (format "Generating TPC-C data: %d warehouse(s), seed %d"
                   warehouses seed))
  (let [db-file (io/file c/base-dir path)
        rows    (g/population-rows seed warehouses)]
    (when (.exists db-file)
      (println "Removing existing database at" (.getPath db-file))
      (.delete db-file))
    (let [t0 (System/nanoTime)]
      (with-open [conn (DriverManager/getConnection
                        (str "jdbc:sqlite:" (.getPath db-file)))]
        (.setAutoCommit conn false)
        (exec-schema! conn)
        (doseq [table c/table-order]
          (load-table! conn table (get rows table)))
        (.commit conn))
      (println (format "Load time: %.2fs"
                       (/ (- (System/nanoTime) t0) 1.0e9)))))
  (println "Done. SQLite database created.")
  (shutdown-agents)
  (System/exit 0))

;; ---------------------------------------------------------------------------
;; Transactions

(defn- now-str [] (str (Instant/now)))

(defn new-order!
  [^Connection conn {:keys [w d c ol]}]
  (.setAutoCommit conn false)
  (try
    (let [_w-tax  (double (first (q1 conn
                                     "SELECT w_tax FROM warehouse WHERE w_id = ?"
                                     w)))
          drow    (q1 conn
                      "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?"
                      w d)
          _       (when (nil? drow)
                    (throw (ex-info "No such district" {:w w :d d})))
          _d-tax  (double (nth drow 0))
          d-next  (long (nth drow 1))
          _c-disc (double (first (q1 conn
                                     "SELECT c_discount FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                                     w d c)))
          line-data (mapv
                     (fn [{:keys [i-id supply-w qty]}]
                       (let [prow (q1 conn "SELECT i_price FROM item WHERE i_id = ?"
                                      i-id)]
                         {:i-id i-id :supply-w supply-w :qty qty
                          :price (when prow (double (first prow)))}))
                     ol)
          missing (some #(when (nil? (:price %)) %) line-data)]
      (if missing
        ;; A single invalid item rolls the whole New-Order back.
        (do
          (.rollback conn)
          {:type :new-order :status :invalid-item :w w :d d
           :i-id (:i-id missing)})
        (let [o-id      d-next
              all-local (long (if (every? #(= w (:supply-w %)) line-data) 1 0))
              ;; A TPC-C order may repeat an item id. Aggregate by stock row so
              ;; each row receives exactly one quantity update per order.
              stock-aggs (reduce
                          (fn [m {:keys [i-id supply-w qty]}]
                            (let [k [supply-w i-id]]
                              (-> m
                                  (update-in [k :qty] (fnil + 0) qty)
                                  (update-in [k :lines] (fnil inc 0))
                                  (assoc-in [k :remote?] (not= supply-w w)))))
                          {} line-data)]
          (exec! conn
                 "UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?"
                 (inc d-next) w d)
          (exec! conn
                 "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                 o-id d w c (now-str) nil (count line-data) all-local)
          (exec! conn
                 "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (?, ?, ?)"
                 o-id d w)
          (doseq [[n {:keys [i-id supply-w qty price]}] (map-indexed vector line-data)]
            (exec! conn
                   "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                   o-id d w (inc n) i-id supply-w nil qty
                   (* (double qty) (double price))
                   "distinfo-distinfo-distinfo"))
          (doseq [[[supply-w i-id] {:keys [qty lines remote?]}] stock-aggs]
            (let [srow  (q1 conn
                            "SELECT s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_w_id = ? AND s_i_id = ?"
                            supply-w i-id)
                  s-qty (long (nth srow 0))
                  s-ytd (long (nth srow 1))
                  s-ocnt (long (nth srow 2))
                  s-rcnt (long (nth srow 3))
                  qty   (long qty)
                  new-q (if (>= s-qty qty)
                          (- s-qty qty)
                          (+ (- s-qty qty) 91))]
              (exec! conn
                     "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?"
                     new-q (+ s-ytd qty) (+ s-ocnt (long lines))
                     (if remote? (inc s-rcnt) s-rcnt) supply-w i-id)))
          (.commit conn)
          {:type :new-order :status :ok :w w :d d :o-id o-id})))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))))

(defn- customer-by-name
  "The middle customer by c_first with the given last name in the district."
  [^Connection conn w d last-name]
  (let [rows (qall conn
                   "SELECT c_id, c_credit, c_data, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first, c_id"
                   w d last-name)]
    (when (seq rows)
      (nth rows (quot (count rows) 2)))))

(defn- customer-by-id
  [^Connection conn w d c]
  (q1 conn
      "SELECT c_id, c_credit, c_data, c_balance, c_ytd_payment, c_payment_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
      w d c))

(defn payment!
  [^Connection conn {:keys [w d c last-name amount by-name?]}]
  (.setAutoCommit conn false)
  (try
    (let [w-ytd (double (first (q1 conn
                                  "SELECT w_ytd FROM warehouse WHERE w_id = ?"
                                  w)))
          d-ytd (double (first (q1 conn
                                   "SELECT d_ytd FROM district WHERE d_w_id = ? AND d_id = ?"
                                   w d)))
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
                         (let [s (str (format "|%d %d %d %d %s" w d c-id w (now-str))
                                      c-data)]
                           (subs s 0 (min 500 (count s)))))]
          (exec! conn "UPDATE warehouse SET w_ytd = ? WHERE w_id = ?"
                 (+ w-ytd (double amount)) w)
          (exec! conn "UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?"
                 (+ d-ytd (double amount)) w d)
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
                 "hdata-hdata-hdata-hdata")
          (.commit conn)
          {:type :payment :status :ok :w w :d d :c c-id :amount amount
           :credit c-credit})))
    (catch Exception e
      (try (.rollback conn) (catch Exception _))
      (throw e))))

(defn order-status!
  [^Connection conn {:keys [w d c last-name by-name?]}]
  (let [c-id (if by-name?
               (let [rows (qall conn
                                "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first, c_id"
                                w d last-name)]
                 (when (seq rows)
                   (long (first (nth rows (quot (count rows) 2))))))
               (let [row (q1 conn
                             "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                             w d c)]
                 (when row (long (first row)))))]
    (if (nil? c-id)
      {:type :order-status :status :no-customer}
      (let [orow (q1 conn
                     "SELECT o_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1"
                     w d c-id)]
        (if orow
          (let [o-id  (long (first orow))
                lines (long (first (q1 conn
                                       "SELECT COUNT(*) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
                                       w d o-id)))]
            {:type :order-status :status :ok :lines lines})
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
                                               "SELECT MIN(no_o_id) FROM new_order WHERE no_w_id = ? AND no_d_id = ?"
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
                                         "SELECT o_c_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
                                         w did no)))
                  crow  (q1 conn
                            "SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
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
      (throw e))))

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
       :ol (mapv (fn [_]
                   {:i-id (if (zero? (mod (rint r 1 100) 100))
                            100001
                            (nurand r 8191 1 c/item-count c-item))
                    :supply-w w
                    :qty (rint r 1 10)})
                 (range (rint r 1 15)))}

      :payment
      (let [by-name? (<= (rint r 1 100) 40)]
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

(defn- do-txn [conn ^Random r opts type]
  (let [input (gen-input r opts type)
        t0    (System/nanoTime)
        res   (case type
                :new-order    (new-order! conn input)
                :payment      (payment! conn input)
                :order-status (order-status! conn input)
                :delivery     (delivery! conn input)
                :stock-level  (stock-level! conn input))]
    [type input res (/ (- (System/nanoTime) t0) 1.0e6)]))

(defn- percentile [sorted p]
  (when (seq sorted)
    (nth sorted (min (dec (count sorted))
                     (int (Math/floor (* (double p) (count sorted))))))))

(defn- open-conn
  "A fresh connection for one terminal. Each terminal must own its connection:
  sharing one connection lets a failed transaction roll back another terminal's
  work."
  ^java.sql.Connection [path]
  (let [cfg  (doto (org.sqlite.SQLiteConfig.)
               (.setBusyTimeout 30000)
               ;; Take the write lock when the transaction starts, before any
               ;; read, so a later write cannot fail with BUSY_SNAPSHOT.
               (.setTransactionMode
                org.sqlite.SQLiteConfig$TransactionMode/IMMEDIATE))
        conn (DriverManager/getConnection
              (str "jdbc:sqlite:" (.getPath (io/file c/base-dir path)))
              (.toProperties cfg))]
    conn))

(defn- enable-wal!
  "WAL is a persistent database property and must be set before other
  connections open the file."
  [path]
  (with-open [conn (DriverManager/getConnection
                    (str "jdbc:sqlite:" (.getPath (io/file c/base-dir path))))]
    (with-open [stmt (.createStatement conn)]
      (.execute stmt "PRAGMA journal_mode=WAL")
      (.execute stmt "PRAGMA busy_timeout=30000"))))

(defn bench
  "Run the TPC-C-derived transaction mix against a loaded SQLite database.

  Options:
    :path       database file (default \"sqlite.db\")
    :warehouses number of warehouses (default 1)
    :txns       measured transactions (default 10000)
    :threads    terminals (default 1)
    :warmup     warmup transactions (default 1000)
    :seed       input seed (default 42)"
  [{:keys [path warehouses txns threads warmup seed]
    :or   {path "sqlite.db" warehouses 1 txns 10000 threads 1 warmup 1000
           seed 42}}]
  (enable-wal! path)
  (let [conn      (open-conn path)
        c-r       (Random. seed)
        opts      {:warehouses warehouses
                   :c-item     (rint c-r 0 8191)
                   :c-cust     (rint c-r 0 1023)
                   :c-last     (rint c-r 0 255)}
        committed (atom {})
        lat       (atom [])
        ;; SQLite has a single writer, so write transactions serialize on one
        ;; lock. Reads run concurrently.
        write-lock (java.util.concurrent.locks.ReentrantLock.)
        run       (fn [^java.sql.Connection cnn ^long ti ^long n record?]
                    (let [r (Random. (+ seed ti 1))]
                      (dotimes [_ n]
                        (let [type (pick-type r)]
                          ;; SQLite has a single writer. Serializing every
                          ;; transaction on one lock makes the read-modify-write
                          ;; sequences atomic and avoids snapshot conflicts;
                          ;; terminals still use independent connections.
                          (.lock write-lock)
                          (let [[ty input res ms]
                                (try (do-txn cnn r opts type)
                                     (finally (.unlock write-lock)))]
                            (when (and (= :new-order ty) (= :ok (:status res)))
                              (swap! committed update [(:w input) (:d input)]
                                     (fnil inc 0)))
                            (when record?
                              (swap! lat conj [ty ms])))))))]
    (println (format "TPC-C-derived: %d warehouse(s), %d terminal(s), %d txns"
                     warehouses threads txns))
    (let [warm-conn (open-conn path)]
      (try (dotimes [_ warmup] (run warm-conn 0 1 false))
           (finally (.close warm-conn))))
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
                               (let [c (open-conn path)]
                                 (try
                                   (run c ti (+ per (if (= ti (dec threads))
                                                       (mod txns threads)
                                                       0))
                                        true)
                                   (finally (.close c))))))
                           (range threads))]
        (doseq [f futs] @f)
        (let [elapsed    (/ (- (System/nanoTime) t0) 1.0e9)
              new-orders (reduce + 0 (vals @committed))
              by-type    (group-by first @lat)
              stats      (into {}
                               (for [[ty xs] by-type]
                                 (let [ms (vec (sort (map second xs)))]
                                   [ty {:count (count ms)
                                        :mean  (/ (reduce + 0.0 ms) (count ms))
                                        :p50   (percentile ms 0.50)
                                        :p95   (percentile ms 0.95)
                                        :p99   (percentile ms 0.99)}])))
              tpmc       (/ (* 60.0 new-orders) elapsed)]
          (println (format "Elapsed: %.2fs  New-Orders: %d  tpmC: %.1f"
                           elapsed new-orders tpmc))
          (doseq [[ty {:keys [count mean p50 p95 p99]}] (sort-by key stats)]
            (println (format "  %-13s n=%-6d mean=%7.3fms p50=%7.3f p95=%7.3f p99=%7.3f"
                             (name ty) count mean p50 p95 p99)))
          ;; Invariant: every district's next_o_id advanced by exactly the number
          ;; of New-Order transactions it committed in the measured interval.
          (let [bad (for [[[w d] n] @committed
                          :let [actual (district-next-o-id conn w d)]
                          :when (not= actual (+ (get baseline [w d]) n))]
                      [w d :expected (+ (get baseline [w d]) n) :actual actual])]
            (println (if (seq bad)
                       (str "INVARIANT FAILURE: " (pr-str bad))
                       "district next_o_id invariant: OK")))
          (let [bad (for [[[w d] n] @committed
                          :let [actual (order-count conn w d)]
                          :when (not= actual (+ (get base-ord [w d]) n))]
                      [w d :expected (+ (get base-ord [w d]) n) :actual actual])]
            (println (if (seq bad)
                       (str "ORDER COUNT FAILURE: " (pr-str bad))
                       "order count invariant: OK")))
          (.close conn)
          {:tpmc tpmc :new-orders new-orders :elapsed elapsed :stats stats})))))

(defn -main [& _args]
  (bench {})
  (shutdown-agents)
  (System/exit 0))
