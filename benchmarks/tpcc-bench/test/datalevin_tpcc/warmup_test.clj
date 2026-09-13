(ns datalevin-tpcc.warmup-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin-tpcc.datalevin]
   [datalevin-tpcc.postgres]
   [datalevin-tpcc.sqlite]
   [datalevin-tpcc.txns :as t]))

(def ^:private drivers
  '[datalevin-tpcc.datalevin datalevin-tpcc.sqlite datalevin-tpcc.postgres])

(use-fixtures :once
  (fn [f]
    ;; Keep the real benchmark loops and input generators, but allow their
    ;; database boundaries to be stubbed even under the direct-linking profile.
    (try
      (binding [*compiler-options* (assoc *compiler-options* :direct-linking false)]
        (doseq [driver drivers] (require driver :reload)))
      (f)
      (finally
        (doseq [driver drivers] (require driver :reload))))))

(defn- warmup-stream [driver seed n]
  (let [calls    (atom [])
        done     (ex-info "Warmup complete" {})
        conn     (reify java.sql.Connection (close [_]))
        generate (ns-resolve driver 'gen-input)
        capture  (fn [_ r opts type]
                   (let [input (generate r opts type)]
                     (swap! calls conj {:rng r :type type :input input})
                     [type input {:status :probe} 0.0]))
        stop     (fn [& _] (throw done))
        db-stubs (case driver
                   datalevin-tpcc.datalevin
                   {#'d/get-conn (constantly conn)
                    #'d/close (constantly nil)
                    #'t/district-next-o-id stop}

                   datalevin-tpcc.sqlite
                   {(ns-resolve driver 'enable-wal!) (constantly nil)
                    (ns-resolve driver 'open-conn) (constantly conn)
                    (ns-resolve driver 'district-next-o-id) stop}

                   datalevin-tpcc.postgres
                   {(ns-resolve driver 'get-connection) (constantly conn)
                    (ns-resolve driver 'district-next-o-id) stop})]
    (with-redefs-fn (assoc db-stubs (ns-resolve driver 'do-txn) capture)
      (fn []
        ;; Stop at the first accounting baseline read, after warmup and before
        ;; measured transactions. No database is opened or modified.
        (with-out-str
          (is (identical? done
                          (try
                            ((ns-resolve driver 'bench)
                             {:seed seed :warehouses 1 :warmup n
                              :threads 1 :txns 1})
                            nil
                            (catch clojure.lang.ExceptionInfo e e)))))))
    @calls))

(defn- inputs [calls]
  (mapv #(select-keys % [:type :input]) calls))

(deftest warmup-advances-one-random-stream-through-the-full-mix
  (doseq [driver drivers]
    (testing (str driver)
      (let [calls (warmup-stream driver 42 1000)]
        (is (= 1000 (count calls)))
        (is (= #{:new-order :payment :order-status :delivery :stock-level}
               (set (map :type calls))))
        (is (> (count (distinct (inputs calls))) 100))
        (is (= 1 (count (distinct (map :rng calls))))
            "every warmup transaction consumes the same RNG instance")
        (is (= (inputs calls) (inputs (warmup-stream driver 42 1000))))
        (is (not= (inputs calls) (inputs (warmup-stream driver 43 1000))))
        (is (= [] (warmup-stream driver 42 0)))
        (is (= [(first (inputs calls))]
               (inputs (warmup-stream driver 42 1))))))))
