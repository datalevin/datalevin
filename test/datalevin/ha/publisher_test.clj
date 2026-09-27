(ns datalevin.ha.publisher-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.ha.control :as ctrl]
   [datalevin.ha.publisher :as publisher]))

(defn- state [renew]
  {:ha-authority (reify ctrl/ILeaseAuthority (renew-lease [_ req] (renew req)))
   :ha-renewal-publisher (publisher/create)
   :ha-role :leader :ha-db-identity "db" :ha-node-id 1 :ha-leader-term 1
   :ha-local-endpoint "127.0.0.1:8898" :ha-authority-version 0
   :ha-lease-renew-ms 1000 :ha-lease-timeout-ms 10000})

(defn- success [req version]
  {:ok? true :version version :authority-now-ms (:now-ms req)
   :lease (-> (select-keys req [:db-identity :leader-node-id :term
                                :leader-last-applied-lsn])
              (assoc :lease-until-ms (+ (long (:now-ms req)) 10000)))})

(deftest failure-is-shared-and-does-not-poison-the-next-publication
  (doseq [failure [{:ok? false :reason :timeout} (ex-info "network failed" {})]]
    (testing (str failure)
      (let [entered (promise) release (promise) joined (promise)
            calls (atom 0)
            m (state (fn [req]
                       (let [n (swap! calls inc)]
                         (if (= n 1)
                           (do (deliver entered true)
                               (assert (deref release 5000 false))
                               (if (instance? Throwable failure) (throw failure) failure))
                           (success req n)))))
            publication (:ha-renewal-publisher m)
            run #(try (publisher/renew! m 42 5000) (catch Throwable t t))
            jobs (atom [(future (run))])]
        (try
          (is (deref entered 5000 false))
          (add-watch publication ::joined
                     (fn [_ _ old new]
                       (when (and (:pending old)
                                  (identical? (:pending old) (:pending new)))
                         (deliver joined true))))
          (swap! jobs conj (future (run)))
          (is (deref joined 5000 false))
          (deliver release true)
          (let [[a b] (mapv #(deref % 5000 ::timeout) @jobs)]
            (is (identical? a b))
            (if (instance? Throwable failure)
              (is (identical? failure a))
              (is (false? (get-in a [:result :ok?])))))
          (is (= 1 @calls))
          (is (true? (get-in (publisher/renew! m 42 5000 {:reuse-ms 1000})
                             [:result :ok?])))
          (is (= 2 @calls))
          (finally
            (remove-watch publication ::joined)
            (deliver release true)
            (doseq [job @jobs] (deref job 5000 nil))))))))

(deftest old-runtime-completion-cannot-replace-new-runtime-proof
  (let [entered (promise) release (promise)
        terms (atom [])
        m (state (fn [req]
                   (swap! terms conj (:term req))
                   (when (= 1 (:term req))
                     (deliver entered true)
                     (assert (deref release 5000 false)))
                   (success req (:term req))))
        old (future (publisher/renew! m 42 5000))]
    (try
      (is (deref entered 5000 false))
      (let [next (assoc m :ha-leader-term 2 :ha-renew-loop-running? (Object.))
            result (publisher/renew! next 42 5000)]
        (is (= 2 (get-in result [:result :lease :term])))
        (deliver release true)
        (is (= 1 (get-in (deref old 5000 nil) [:result :lease :term])))
        (is (identical? result (publisher/renew! next 42 5000 {:reuse-ms 1000})))
        (is (= [1 2] @terms)))
      (finally (deliver release true) (deref old 5000 nil)))))

(deftest idle-renewal-refreshes-after-the-reuse-interval
  (let [calls (atom 0)
        m (state #(success % (swap! calls inc)))
        first-result (publisher/renew! m 0 5000)]
    (is (identical? first-result (publisher/renew! m 0 5000 {:reuse-ms 1000})))
    (is (= 1 @calls))
    (swap! (:ha-renewal-publisher m) assoc-in [:completed :local-start-nanos]
           (- (System/nanoTime) 2000000000))
    (is (= 2 (get-in (publisher/renew! m 0 5000 {:reuse-ms 1000}) [:result :version])))
    (is (= 2 @calls))))
