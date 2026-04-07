(ns datalevin.bench-ha-commit
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [datalevin.ha.control :as ctrl]
   [datalevin.test.core :as test-core]
   [datalevin.util :as u]
   [taoensso.timbre :as log])
  (:import
   [java.util UUID]))

(def ^:private default-config
  {:rounds            3
   :warmup-renews     50
   :measured-renews   300
   :leader-timeout-ms 8000})

(def ^:private leader-node-id 1)
(def ^:private leader-endpoint "bench-node-1")
(def ^:private lease-renew-ms 1000)
(def ^:private lease-timeout-ms 3000)
(def ^:private rpc-timeout-ms 1000)
(def ^:private operation-timeout-ms 5000)
(def ^:private election-timeout-ms 1000)

(defn- uuid-suffix
  []
  (subs (str (UUID/randomUUID)) 0 8))

(defn- parse-long-arg
  [s]
  (when (and s (not (str/blank? s)))
    (Long/parseLong s)))

(defn- parse-args
  [args]
  (let [[rounds measured-renews warmup-renews]
        (map parse-long-arg args)]
    (cond-> default-config
      rounds          (assoc :rounds rounds)
      measured-renews (assoc :measured-renews measured-renews)
      warmup-renews   (assoc :warmup-renews warmup-renews))))

(defn- control-peer-id
  []
  (str "127.0.0.1:" (test-core/allocate-port)))

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

(defn- voter
  [node-id peer-id]
  {:peer-id     peer-id
   :promotable? true
   :ha-node-id  node-id})

(defn- authority-entry
  [group-id voters root-dir node-id peer-id]
  (let [raft-dir (str root-dir u/+separator+ "raft-" node-id)]
    (.mkdirs (io/file raft-dir))
    {:node-id   node-id
     :peer-id   peer-id
     :raft-dir  raft-dir
     :authority (ctrl/new-authority
                 {:backend              :sofa-jraft
                  :group-id             group-id
                  :local-peer-id        peer-id
                  :voters               voters
                  :rpc-timeout-ms       rpc-timeout-ms
                  :operation-timeout-ms operation-timeout-ms
                  :election-timeout-ms  election-timeout-ms
                  :raft-dir             raft-dir})}))

(defn- create-cluster
  [root-dir]
  (let [group-id (str "bench-ha-commit-" (uuid-suffix))
        peer-ids (repeatedly 3 control-peer-id)
        voters   (mapv voter (range 1 4) peer-ids)]
    {:group-id group-id
     :voters   voters
     :entries  (mapv (fn [[node-id peer-id]]
                       (authority-entry group-id voters root-dir node-id peer-id))
                     (map vector (range 1 4) peer-ids))}))

(defn- start-cluster!
  [entries]
  (doseq [{:keys [authority]} entries]
    (ctrl/start-authority! authority)))

(defn- stop-cluster!
  [entries]
  (doseq [{:keys [authority]} (reverse entries)]
    (try
      (ctrl/stop-authority! authority)
      (catch Throwable _
        nil))))

(defn- wait-for-leader!
  [entries timeout-ms]
  (let [deadline-ms (+ (System/currentTimeMillis) (long timeout-ms))]
    (loop [last-diags nil]
      (let [diags (mapv (fn [{:keys [peer-id authority]}]
                          {:peer-id peer-id
                           :diag    (ctrl/authority-diagnostics authority)})
                        entries)
            leader (some (fn [{:keys [peer-id diag]}]
                           (when (true? (:node-leader? diag))
                             (assoc (first (filter #(= peer-id (:peer-id %))
                                                   entries))
                                    :diag diag)))
                         diags)]
        (if leader
          leader
          (if (< (System/currentTimeMillis) deadline-ms)
            (do
              (Thread/sleep 25)
              (recur diags))
            (u/raise "Timed out waiting for HA control leader in benchmark"
                     {:timeout-ms timeout-ms
                      :diags      (or diags last-diags)})))))))

(defn- acquire-lease!
  [authority db-identity]
  (let [{:keys [lease version]}
        (ctrl/read-state authority db-identity rpc-timeout-ms)
        result
        (ctrl/try-acquire-lease
         authority
         {:db-identity             db-identity
          :leader-node-id          leader-node-id
          :leader-endpoint         leader-endpoint
          :lease-renew-ms          lease-renew-ms
          :lease-timeout-ms        lease-timeout-ms
          :leader-last-applied-lsn 0
          :now-ms                  (System/currentTimeMillis)
          :observed-version        version
          :observed-lease          lease})]
    (when-not (:ok? result)
      (u/raise "HA control benchmark could not acquire lease"
               {:db-identity db-identity
                :result result}))
    result))

(defn- renew-once!
  [authority db-identity term leader-last-applied-lsn]
  (let [result
        (ctrl/renew-lease
         authority
         {:db-identity             db-identity
          :leader-node-id          leader-node-id
          :leader-endpoint         leader-endpoint
          :term                    term
          :lease-renew-ms          lease-renew-ms
          :lease-timeout-ms        lease-timeout-ms
          :leader-last-applied-lsn leader-last-applied-lsn
          :now-ms                  (System/currentTimeMillis)
          :timeout-ms              operation-timeout-ms})]
    (when-not (:ok? result)
      (u/raise "HA control benchmark renew failed"
               {:db-identity db-identity
                :result result}))
    result))

(defn- run-renew-loop!
  [authority db-identity term start-lsn renew-count capture-latencies?]
  (let [latencies (when capture-latencies? (transient []))
        started   (System/nanoTime)]
    (dotimes [i renew-count]
      (let [lsn        (+ start-lsn i)
            renew-beg  (System/nanoTime)]
        (renew-once! authority db-identity term lsn)
        (when capture-latencies?
          (conj! latencies
                 (/ (double (- (System/nanoTime) renew-beg)) 1000000.0)))))
    {:elapsed-ms   (/ (double (- (System/nanoTime) started)) 1000000.0)
     :latencies-ms (when capture-latencies? (persistent! latencies))}))

(defn- run-round!
  [round {:keys [warmup-renews measured-renews leader-timeout-ms]}]
  (let [root-dir    (u/tmp-dir (str "ha-commit-bench-round-"
                                    round "-"
                                    (UUID/randomUUID)))
        db-identity (str "bench-db-" round "-" (uuid-suffix))
        cluster     (create-cluster root-dir)
        entries     (:entries cluster)]
    (try
      (start-cluster! entries)
      (let [{:keys [authority peer-id]}
            (wait-for-leader! entries leader-timeout-ms)
            _                 (ctrl/init-membership-hash!
                               authority
                               (str "bench-membership-" (uuid-suffix)))
            acquire           (acquire-lease! authority db-identity)
            term              (:term acquire)
            warmup-end        (+ 1 warmup-renews)
            _                 (run-renew-loop! authority
                                               db-identity
                                               term
                                               1
                                               warmup-renews
                                               false)
            {:keys [elapsed-ms latencies-ms]}
            (run-renew-loop! authority
                             db-identity
                             term
                             warmup-end
                             measured-renews
                             true)]
        (assoc (summarize-latencies latencies-ms elapsed-ms)
               :round round
               :leader-peer-id peer-id
               :latencies-ms latencies-ms))
      (finally
        (stop-cluster! entries)
        (u/delete-files root-dir)))))

(defn- summarize-rounds
  [rounds]
  (let [latencies-ms (vec (mapcat :latencies-ms rounds))
        elapsed-ms   (reduce + 0.0 (map :elapsed-ms rounds))]
    (assoc (summarize-latencies latencies-ms elapsed-ms)
           :rounds (count rounds))))

(defn- benchmark!
  [config]
  (let [rounds (mapv #(run-round! % config)
                     (range 1 (inc (:rounds config))))]
    {:config  config
     :summary (summarize-rounds rounds)
     :rounds  (mapv #(dissoc % :latencies-ms) rounds)}))

(defn -main
  [& args]
  (log/set-min-level! :warn)
  (println (pr-str (benchmark! (parse-args args)))))
