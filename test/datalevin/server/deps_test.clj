(ns datalevin.server.deps-test
  "Guards the server dependency wiring: every entry is Var-backed so
  redefinition is observed consistently, and every map is validated
  against its consumer's contract at construction time."
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.server :as server]
   [datalevin.server.deps :as deps]))

(def ^:private sample-value 42)

(def ^:private contract
  {:callbacks #{:a :b}
   :values    {:c map?}})

(defn- validation-error
  [deps & [opts]]
  (try
    (deps/validate ::contract contract deps (or opts {}))
    nil
    (catch clojure.lang.ExceptionInfo e e)))

(deftest validate-accepts-a-complete-map-test
  (let [deps {:a (fn []) :b (fn []) :c {}}]
    (is (identical? deps (deps/validate ::contract contract deps)))))

(deftest validate-rejects-missing-keys-test
  (let [ex (validation-error {})]
    (is (instance? clojure.lang.ExceptionInfo ex))
    (is (= :server/deps-invalid (:error (ex-data ex))))
    (is (= #{:a :b :c} (set (:missing (ex-data ex)))))))

(deftest validate-rejects-non-callable-test
  (let [ex (validation-error {:a 1 :b (fn []) :c {}})]
    (is (= [:a] (:not-fn (ex-data ex))))))

(deftest validate-rejects-wrong-value-type-test
  (let [ex (validation-error {:a (fn []) :b (fn []) :c []})]
    (is (= [:c] (mapv first (:invalid (ex-data ex)))))))

(deftest validate-strict-mode-rejects-unexpected-keys-test
  (let [ex (validation-error {:a (fn []) :b (fn []) :c {} :extra 1}
                             {:strict? true})]
    (is (= [:extra] (:unexpected (ex-data ex))))))

(deftest value-resolves-vars-and-passes-plain-values-test
  (is (= sample-value (deps/value {:k #'sample-value} :k)))
  (is (= 7 (deps/value {:k 7} :k)))
  (is (nil? (deps/value {} :k))))

(deftest server-deps-are-var-backed-test
  (testing "every dependency entry in every server map is a Var"
    (doseq [deps-var [#'server/handler-deps
                      #'server/session-deps
                      #'server/copy-deps
                      #'server/dispatch-deps
                      #'server/ha-deps]
            [k v]      @deps-var]
      (is (var? v) (str (:name (meta deps-var)) " " k " is not Var-backed")))))

(deftest callback-redefinition-is-observed-through-deps-test
  (let [handler-deps @#'server/handler-deps]
    (is (identical? (:write-message handler-deps) #'server/write-message))
    (with-redefs [server/write-message (fn [& _] :replaced)]
      (is (= :replaced ((:write-message handler-deps) nil {:type :test}))))
    (is (identical? (:write-message handler-deps) #'server/write-message))))

(deftest data-dep-redefinition-is-observed-through-deps-test
  (testing "dispatch reads the current handler table"
    (let [dispatch-deps @#'server/dispatch-deps]
      (is (map? (deps/value dispatch-deps :message-handler-map)))
      (with-redefs [server/message-handler-map {:only :this}]
        (is (= {:only :this}
               (deps/value dispatch-deps :message-handler-map))))))
  (testing "HA reads the current exempt-write-type set"
    (let [ha-deps @#'server/ha-deps]
      (is (set? (deps/value ha-deps :udf-admission-exempt-write-types)))
      (with-redefs [server/udf-admission-exempt-write-types #{:only-this}]
        (is (= #{:only-this}
               (deps/value ha-deps
                           :udf-admission-exempt-write-types)))))))
