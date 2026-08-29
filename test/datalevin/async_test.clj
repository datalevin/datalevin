(ns datalevin.async-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.async :as async]))

(deftype FailingWork [callback]
  async/IAsyncWork
  (work-key [_] :async-test/failure)
  (do-work [_] (throw (ex-info "expected async failure" {:type :test})))
  (combine [_] nil)
  (callback [_] callback))

(deftype FailingCombinedWork [callback]
  async/IAsyncWork
  (work-key [_] :async-test/combined-failure)
  (do-work [_] (throw (ex-info "expected combined async failure"
                               {:type :test})))
  (combine [this] (fn [_] this))
  (callback [_] callback))

(deftype SuccessfulWork [key value gate callback]
  async/IAsyncWork
  (work-key [_] key)
  (do-work [_]
    (when gate @gate)
    value)
  (combine [_] nil)
  (callback [_] callback))

(declare ->CombiningWork)

(deftype CombiningWork [values callback]
  async/IAsyncWork
  (work-key [_] :async-test/combined-success)
  (do-work [_] values)
  (combine [_]
    (fn [works]
      (->CombiningWork
        (mapv #(.-values ^CombiningWork %) works)
        nil)))
  (callback [_] callback))

(defn- thrown-value
  [f]
  (try
    (f)
    nil
    (catch Throwable t t)))

(deftest callbacks-receive-async-errors
  (let [callback-result (promise)
        result          (async/exec
                          (async/get-executor)
                          (->FailingWork #(deliver callback-result %)))]
    (try
      (let [future-error   (thrown-value #(deref result))
            callback-error (deref callback-result 1000 ::timeout)]
        (is (instance? clojure.lang.ExceptionInfo future-error))
        (is (= "expected async failure" (ex-message future-error)))
        (is (identical? future-error callback-error)))
      (finally
        (async/shutdown-executor)))))

(deftest combined-callbacks-receive-async-errors
  (let [callback-result (promise)
        result          (async/exec
                          (async/get-executor)
                          (->FailingCombinedWork
                            #(deliver callback-result %)))]
    (try
      (let [future-error   (thrown-value #(deref result))
            callback-error (deref callback-result 1000 ::timeout)]
        (is (instance? clojure.lang.ExceptionInfo future-error))
        (is (= "expected combined async failure" (ex-message future-error)))
        (is (identical? future-error callback-error)))
      (finally
        (async/shutdown-executor)))))

(deftest futures-are-realized-before-success-callbacks
  (let [gate            (promise)
        result-holder   (atom nil)
        callback-result (promise)
        result          (async/exec
                          (async/get-executor)
                          (->SuccessfulWork
                            :async-test/realization
                            :done
                            gate
                            #(deliver callback-result
                                      {:payload %
                                       :realized?
                                       (realized? @result-holder)})))]
    (try
      (reset! result-holder result)
      (deliver gate true)
      (is (= :done @result))
      (is (= {:payload :done :realized? true}
             (deref callback-result 1000 ::timeout)))
      (finally
        (async/shutdown-executor)))))

(deftest blocking-deref-times-out-without-cancelling-work
  (let [gate   (promise)
        result (async/exec
                 (async/get-executor)
                 (->SuccessfulWork
                   :async-test/timeout :eventual gate nil))]
    (try
      (is (= ::timeout (deref result 10 ::timeout)))
      (deliver gate true)
      (is (= :eventual (deref result 1000 ::timeout)))
      (finally
        (deliver gate true)
        (async/shutdown-executor)))))

(deftest callback-failures-do-not-poison-the-executor
  (let [second-callback (promise)]
    (try
      (let [first-result
            (async/exec
              (async/get-executor)
              (->SuccessfulWork
                :async-test/callback-failure
                :first
                nil
                #(throw (ex-info "callback failed" {:payload %}))))]
        (is (= :first @first-result)))
      (let [second-result
            (async/exec
              (async/get-executor)
              (->SuccessfulWork
                :async-test/callback-failure
                :second
                nil
                #(deliver second-callback %)))]
        (is (= :second @second-result))
        (is (= :second (deref second-callback 1000 ::timeout))))
      (finally
        (async/shutdown-executor)))))

(deftest combined-work-realizes-every-future-and-callback
  (let [new-executor (ns-resolve 'datalevin.async 'new-async-executor)
        executor     (new-executor)
        callbacks    (repeatedly 3 promise)
        values       [:a :b :c]
        results      (mapv (fn [value callback-result]
                             (async/exec
                               executor
                               (->CombiningWork
                                 value
                                 #(deliver callback-result %))))
                           values callbacks)]
    (try
      (async/start executor)
      (doseq [result results]
        (is (= values (deref result 1000 ::timeout))))
      (doseq [callback-result callbacks]
        (is (= values (deref callback-result 1000 ::timeout))))
      (finally
        (async/stop executor)))))
