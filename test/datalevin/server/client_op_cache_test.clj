(ns datalevin.server.client-op-cache-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.server.client-op-cache :as cache])
  (:import [java.util.concurrent ConcurrentHashMap]))

(defn- pending! [c id]
  (let [entry {:request {:client-op-id id} :result-promise (promise)}]
    (.put ^ConcurrentHashMap (:pending c) id entry)
    entry))

(deftest completed-requests-expire-without-expiring-in-flight-requests-test
  (let [clock (atom 0)
        c (cache/create)
        pending (pending! c "pending")
        done (pending! c "done")
        failed (pending! c "failed")
        failure (ex-info "Rolled back" {})]
    (with-redefs [cache/now-nanos (fn ^long [] (long @clock))]
      (cache/complete! c done {:status :ok :response :saved})
      (reset! clock 1000000000)
      (cache/complete! c failed {:status :error :exception failure})
      (reset! clock 59999999999)
      (cache/prune! c)
      (is (= #{"pending" "done" "failed"} (set (keys (:pending c)))))
      (is (= :saved (:response @(:result-promise done))))
      (is (identical? failure (:exception @(:result-promise failed))))
      (reset! clock 60000000000)
      (cache/prune! c)
      (is (= #{"pending" "failed"} (set (keys (:pending c)))))
      (reset! clock 61000000000)
      (cache/prune! c)
      (is (= #{"pending"} (set (keys (:pending c)))))
      (is (not (realized? (:result-promise pending)))))))

(deftest expiration-preserves-a-replacement-for-the-same-request-id-test
  (let [clock (atom 0)
        c (cache/create)
        old (pending! c "same-id")]
    (with-redefs [cache/now-nanos (fn ^long [] (long @clock))]
      (cache/complete! c old {:status :ok})
      (let [replacement (pending! c "same-id")]
        (reset! clock 60000000000)
        (cache/prune! c)
        (is (identical? replacement (get (:pending c) "same-id")))))))

(deftest expiration-does-not-traverse-the-pending-map-test
  (let [clock (atom 0)
        scans (atom 0)
        c (assoc (cache/create) :pending
                 (proxy [ConcurrentHashMap] []
                   (entrySet [] (swap! scans inc) (proxy-super entrySet))))]
    (with-redefs [cache/now-nanos (fn ^long [] (long @clock))]
      (dotimes [n 1000]
        (cache/complete! c (pending! c (str n)) {:status :ok}))
      (dotimes [_ 100] (cache/prune! c))
      (is (= 1000 (.size ^ConcurrentHashMap (:pending c))))
      (reset! clock 60000000000)
      (cache/prune! c)
      (is (.isEmpty ^ConcurrentHashMap (:pending c)))
      (is (zero? @scans) "expiration must not scan all retained requests"))))

(deftest expiration-handles-nanotime-wraparound-test
  (let [clock (atom (- Long/MAX_VALUE 1000000000))
        c (cache/create)
        entry (pending! c "wrapped")]
    (with-redefs [cache/now-nanos (fn ^long [] (long @clock))]
      (cache/complete! c entry {:status :ok})
      (swap! clock #(unchecked-add (long %) 60000000000))
      (cache/prune! c)
      (is (.isEmpty ^ConcurrentHashMap (:pending c))))))
