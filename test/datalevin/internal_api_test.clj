(ns datalevin.internal-api-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is testing]]
   [datalevin.client :as client]
   [datalevin.remote]
   [datalevin.server]
   [datalevin.storage.options :as options])
  (:import [java.io PushbackReader]))

(defn- boundary-vars
  [caller source target-namespaces]
  (binding [*ns* (the-ns caller)
            *read-eval* false]
    (with-open [in (PushbackReader. (io/reader (io/resource source)))]
      (into #{}
            (comp
             (mapcat #(tree-seq coll? seq %))
             (filter symbol?)
             (keep #(when-let [alias (some-> % namespace symbol)]
                      (when (contains? target-namespaces
                                       (some-> (get (ns-aliases *ns*) alias)
                                               ns-name))
                        (ns-resolve *ns* %)))))
            (take-while #(not= ::eof %)
                        (repeatedly #(read {:eof ::eof} in)))))))

(deftest subsystem-boundaries-use-public-vars-test
  (doseq [[caller source targets]
          [['datalevin.remote "datalevin/remote.clj" #{'datalevin.client}]
           ['datalevin.server "datalevin/server.clj"
            #{'datalevin.storage 'datalevin.storage.options}]]]
    (let [vars (boundary-vars caller source targets)]
      (is (seq vars))
      (doseq [v vars]
        (is (not (:private (meta v)))
            (str caller " must use an explicit internal API instead of " v))))))

(deftest internal-apis-are-public-and-documented-test
  (doseq [v [#'client/retryable-ha-write-reject?
             #'client/retry-ha-write-request
             #'client/retry-ha-transport-failure
             #'client/request-ha-open
             #'client/clear-preferred-ha-endpoint!
             #'client/active-ha-request-client
             #'client/sync-ha-routing!
             #'client/disable-ha-write-retry!
             #'client/enable-ha-write-retry!
             #'options/transact-opts]]
    (testing (str v)
      (is (not (:private (meta v))))
      (is (:no-doc (meta v)) "internal APIs stay out of end-user API docs")
      (is (seq (:doc (meta v)))))))

(defn- routing-client []
  (client/->Client "user" "password" "127.0.0.1" 19001 1 1000 nil nil))

(def ^:private rejected
  {:error :ha/write-rejected :retryable? true
   :ha-retry-endpoints ["127.0.0.1:19002"]})

(deftest retry-api-replays-custom-requests-and-remembers-endpoint-test
  (let [base    (routing-client)
        target  {:endpoint "127.0.0.1:19002"}
        req     {:type :transact-kv :mode :copy-in :args ["db"]}
        calls   (atom [])
        request (fn [c r]
                  (swap! calls conj [c r])
                  {:type :command-complete :result :transacted})]
    (with-redefs [client/new-client-for-endpoint (fn [_ _ _] target)]
      (is (= :transacted
             (client/retry-ha-write-request base req "not leader"
                                           rejected request)))
      (is (= [[target req]] @calls))
      (is (identical? target (client/active-ha-request-client base)))
      (client/clear-preferred-ha-endpoint! base)
      (is (identical? base (client/active-ha-request-client base))))))

(deftest retry-api-preserves-disabled-and-nonretryable-errors-test
  (let [base (routing-client)
        req  {:type :transact-kv :args ["db"]}
        call (fn [& _] (is false "must not replay") nil)]
    (doseq [err [(assoc rejected :retryable? false)
                 {:error :replica/read-only}]]
      (let [e (try (client/retry-ha-write-request base req "rejected" err call)
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= err (:err-data (ex-data e))))
        (is (= "rejected" (:server-message (ex-data e))))))
    (client/disable-ha-write-retry! base)
    (try
      (let [e (try (client/retry-ha-write-request base req "pinned" rejected call)
                   (catch clojure.lang.ExceptionInfo e e))]
        (is (= rejected (:err-data (ex-data e))))
        (is (= "pinned" (:server-message (ex-data e)))))
      (finally (client/enable-ha-write-retry! base)))))

(deftest transport-retry-api-preserves-metadata-and-decoding-errors-test
  (let [base   (routing-client)
        target {:endpoint "127.0.0.1:19002"}
        result {:tx-data [] :db-info {:max-tx 9} :new-attributes [:name]}
        req    {:type :tx-data+db-info :args ["db"]}
        calls  (atom [])
        send   (fn [c r]
                 (swap! calls conj [c r])
                 {:type :command-complete :result result})
        failed (ex-info "socket closed" {})]
    (with-redefs [client/new-client-for-endpoint (fn [_ _ _] target)]
      (is (nil? (client/retry-ha-transport-failure
                 base req send ["127.0.0.1:19001"] failed)))
      (is (empty? @calls))
      (is (= {:type :command-complete :result result
              :db-info {:max-tx 9} :new-attributes [:name]}
             (client/retry-ha-transport-failure
              base req send ["127.0.0.1:19001" "127.0.0.1:19002"] failed)))
      (is (= [[target req]] @calls))
      (let [decode-error (ex-info "invalid native value"
                                  {:error :native-value/decode})
            wrapped      (ex-info "wire read failed" {} decode-error)
            actual       (try
                           (client/retry-ha-transport-failure
                            base req send ["127.0.0.1:19002"] wrapped)
                           (catch Exception e e))]
        (is (identical? wrapped actual))
        (is (= 1 (count @calls)))))))
