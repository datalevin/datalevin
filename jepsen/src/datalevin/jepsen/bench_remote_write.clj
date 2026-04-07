(ns datalevin.jepsen.bench-remote-write
  (:require
   [clojure.string :as str]
   [datalevin.core :as d]
   [datalevin.jepsen.core :as core]
   [datalevin.jepsen.local :as local]
   [datalevin.jepsen.workload.register :as register]
   [datalevin.server.ha :as server-ha]
   [jepsen.db :as jdb]
   [taoensso.timbre :as log])
  (:import
   [java.util UUID]))

(def ^:private default-config
  {:rounds                 3
   :warmup-txs             50
   :measured-txs           250
   :entity-count           128
   :leader-timeout-ms      15000
   :replication-timeout-ms 15000})

(def ^:private register-id-query
  '[:find ?key ?e
    :where
    [?e :register/key ?key]])

(defn- uuid-suffix
  []
  (subs (str (UUID/randomUUID)) 0 8))

(defn- parse-long-arg
  [s]
  (when (and s (not (str/blank? s)))
    (Long/parseLong s)))

(defn- parse-args
  [args]
  (let [[rounds measured-txs warmup-txs entity-count]
        (map parse-long-arg args)]
    (cond-> default-config
      rounds       (assoc :rounds rounds)
      measured-txs (assoc :measured-txs measured-txs)
      warmup-txs   (assoc :warmup-txs warmup-txs)
      entity-count (assoc :entity-count entity-count))))

(defn- percentile
  [sorted-values p]
  (let [n (count sorted-values)]
    (when (pos? n)
      (nth sorted-values
           (-> (Math/ceil (* (double p) n))
               long
               dec
               (max 0)
               (min (dec n)))))))

(defn- summarize-latencies
  [latencies-ms elapsed-ms]
  (let [n      (count latencies-ms)
        sorted (vec (sort latencies-ms))
        total  (reduce + 0.0 latencies-ms)]
    {:count       n
     :elapsed-ms  elapsed-ms
     :ops-per-sec (when (pos? elapsed-ms)
                    (/ (* 1000.0 n) elapsed-ms))
     :mean-ms     (when (pos? n) (/ total n))
     :median-ms   (percentile sorted 0.50)
     :p95-ms      (percentile sorted 0.95)
     :min-ms      (first sorted)
     :max-ms      (last sorted)}))

(defn- pct-change
  [baseline value]
  (when (and baseline (not (zero? (double baseline))))
    (* 100.0 (/ (- (double value) (double baseline))
                (double baseline)))))

(defn- summarize-mode
  [rounds]
  (let [latencies-ms (vec (mapcat :latencies-ms rounds))
        elapsed-ms   (reduce + 0.0 (keep :elapsed-ms rounds))]
    (assoc (summarize-latencies latencies-ms elapsed-ms)
           :rounds (count rounds))))

(defn- mode-order
  [round]
  (if (odd? round)
    [:disabled :enabled]
    [:enabled :disabled]))

(defn- make-test-map
  [mode round]
  (core/datalevin-test
   {:workload        :register
    :control-backend :sofa-jraft
    :db-name         (str "bench-remote-write-"
                          (name mode)
                          "-"
                          round
                          "-"
                          (uuid-suffix))
    :rate            1
    :time-limit      1
    :nemesis         []}))

(defn- start-cluster!
  [test-map]
  (doseq [node (:nodes test-map)]
    (jdb/setup! (:db test-map) test-map node))
  test-map)

(defn- stop-cluster!
  [test-map]
  (doseq [node (:nodes test-map)]
    (try
      (jdb/teardown! (:db test-map) test-map node)
      (catch Throwable _
        nil))))

(defn- wait-for-replication!
  [cluster-id leader timeout-ms]
  (let [target-lsn (local/effective-local-lsn cluster-id leader)]
    (local/wait-for-live-nodes-at-least-lsn! cluster-id target-lsn timeout-ms)))

(defn- seed-registers!
  [conn entity-count]
  (d/transact!
   conn
   (mapv (fn [k]
           {:db/id          (str "bench-register-" k)
            :register/key   (long k)
            :register/value (long k)})
         (range entity-count))))

(defn- entity-ids
  [conn entity-count]
  (let [rows (d/q register-id-query @conn)
        by-k (into {} (map (fn [[k e]] [(long k) e])) rows)]
    (mapv by-k (range entity-count))))

(defn- transact-once!
  [conn entity-ids i]
  (let [entity-count (count entity-ids)
        entid        (nth entity-ids (mod i entity-count))]
    (d/transact! conn [{:db/id entid
                        :register/value (long i)}])))

(defn- run-write-loop!
  [conn entity-ids tx-count capture-latencies?]
  (let [latencies (when capture-latencies? (transient []))
        started   (System/nanoTime)]
    (dotimes [i tx-count]
      (let [tx-started (System/nanoTime)]
        (transact-once! conn entity-ids i)
        (when capture-latencies?
          (conj! latencies
                 (/ (double (- (System/nanoTime) tx-started)) 1000000.0)))))
    {:elapsed-ms   (/ (double (- (System/nanoTime) started)) 1000000.0)
     :latencies-ms (when capture-latencies? (persistent! latencies))}))

(defn- run-round*
  [mode round {:keys [entity-count warmup-txs measured-txs
                      leader-timeout-ms replication-timeout-ms]}]
  (let [test-map    (make-test-map mode round)
        cluster-id  (:datalevin/cluster-id test-map)]
    (try
      (start-cluster! test-map)
      (let [leader-info (local/wait-for-single-leader! cluster-id leader-timeout-ms)
            leader      (:leader leader-info)
            conn        (local/open-leader-conn! test-map register/schema)]
        (try
          (seed-registers! conn entity-count)
          (wait-for-replication! cluster-id leader replication-timeout-ms)
          (let [entity-ids            (entity-ids conn entity-count)
                _                     (run-write-loop! conn entity-ids warmup-txs false)
                _                     (wait-for-replication! cluster-id leader replication-timeout-ms)
                {:keys [elapsed-ms latencies-ms]}
                (run-write-loop! conn entity-ids measured-txs true)]
            (assoc (summarize-latencies latencies-ms elapsed-ms)
                   :mode mode
                   :round round
                   :leader leader
                   :latencies-ms latencies-ms))
          (finally
            (d/close conn))))
      (finally
        (stop-cluster! test-map)))))

(defn- run-round!
  [mode round config]
  (if (= mode :disabled)
    (with-redefs [server-ha/ha-write-commit-publish-fn
                  (fn [_deps _server _message]
                    nil)]
      (run-round* mode round config))
    (run-round* mode round config)))

(defn- benchmark!
  [config]
  (let [rounds (mapcat (fn [round]
                         (map (fn [mode]
                                (run-round! mode round config))
                              (mode-order round)))
                       (range 1 (inc (:rounds config))))
        grouped (group-by :mode rounds)
        disabled (summarize-mode (get grouped :disabled))
        enabled  (summarize-mode (get grouped :enabled))]
    {:config config
     :modes  {:disabled disabled
              :enabled enabled}
     :delta  {:mean-ms {:absolute (- (double (:mean-ms enabled))
                                     (double (:mean-ms disabled)))
                        :percent  (pct-change (:mean-ms disabled)
                                              (:mean-ms enabled))}
              :median-ms {:absolute (- (double (:median-ms enabled))
                                       (double (:median-ms disabled)))
                          :percent  (pct-change (:median-ms disabled)
                                                (:median-ms enabled))}
              :p95-ms {:absolute (- (double (:p95-ms enabled))
                                    (double (:p95-ms disabled)))
                       :percent  (pct-change (:p95-ms disabled)
                                             (:p95-ms enabled))}
              :ops-per-sec {:absolute (- (double (:ops-per-sec enabled))
                                         (double (:ops-per-sec disabled)))
                            :percent  (pct-change (:ops-per-sec disabled)
                                                  (:ops-per-sec enabled))}}
     :rounds (mapv #(dissoc % :latencies-ms) rounds)}))

(defn -main
  [& args]
  (log/set-min-level! :warn)
  (println (pr-str (benchmark! (parse-args args)))))
