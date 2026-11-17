(ns datalevin.server.preparation-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.server.handlers :as handlers]
            [datalevin.test.core :refer [db-fixture]]
            [taoensso.timbre :as log]))

(use-fixtures :each db-fixture)

(defn- with-db [options f]
  (with-open [cn (d/create-conn nil {:name {:db/valueType :db.type/string}
                                   :value {:db/valueType :db.type/long}}
                  (merge {:wal? false :background-sampling? false
                          :snapshot-scheduler? false} options))]
    (d/transact! cn [{:db/id 1 :name "one" :value 0}])
    (f cn)))

(defn- with-preparation-log [entries f]
  (log/with-config
    (assoc log/*config* :min-level :trace
           :appenders {::capture {:enabled? true :async? false
                                  :fn #(swap! entries conj %)}})
    (f)))

(defn- caught-exception [f]
  (try (f) nil (catch Exception e e)))

(deftest pure-compiler-exceptions-reach-the-caller
  (with-db {}
    (fn [cn]
      (doseq [failure [(NullPointerException. "compiler defect")
                       (ex-info "compiler defect" {:source :compiler})
                       (ex-info "compiler defect" {:input 7})]]
        (let [entries (atom [])
              actual (with-preparation-log entries
                       #(binding [db/*tx-compiler-observer*
                                  (fn [_ _] (throw failure))]
                          (caught-exception
                            (fn [] (#'handlers/prepare-server-tx
                                     @cn [[:db/add 1 :value 7]])))))]
          (is (identical? failure actual))
          (is (empty? @entries))
          (is (= 0 (:value (d/entity @cn 1)))))))))

(deftest binding-retries-report-the-original-exception
  (with-db {}
    (fn [cn]
      (doseq [forms-fn [#(vector [:db/add 1 :name %]) #(vector {:name %})]
              failure [(NullPointerException. "binding defect")
                       (ex-info "binding defect" {:source :binding})]]
        (let [value (proxy [Object] [] (toString [] (throw failure)))
              forms (forms-fn value)
              entries (atom []) events (atom [])
              prepared (with-preparation-log entries
                         #(binding [db/*tx-compiler-observer*
                                    (fn [event details] (swap! events conj [event details]))]
                            (#'handlers/prepare-server-tx @cn forms)))]
          (is (nil? prepared))
          (is (= [[:compiler-exception {:phase :server-binding :exception failure}]] @events))
          (is (= [:warn] (mapv :level @entries)))
          (is (identical? failure (:?err (first @entries)))))))))

(deftest observer-can-make-binding-retries-fail-loudly
  (with-db {}
    (fn [cn]
      (let [failure (IllegalStateException. "binding defect")
            value (proxy [Object] [] (toString [] (throw failure)))
            actual (binding [db/*tx-compiler-observer*
                             (fn [event details]
                               (when (= :compiler-exception event)
                                 (throw (:exception details))))]
                     (caught-exception
                       #(#'handlers/prepare-server-tx @cn [[:db/add 1 :name value]])))]
        (is (identical? failure actual))))))

(deftest validation-failures-retain-the-writer-retry
  (with-db {:validate-data? true}
    (fn [cn]
      (let [forms [[:db/add 1 :name 42]]
            entries (atom []) events (atom [])
            prepared (with-preparation-log entries
                       #(binding [db/*tx-compiler-observer*
                                  (fn [event details] (swap! events conj [event details]))]
                          (#'handlers/prepare-server-tx @cn forms)))
            failure (:exception (second (first @events)))]
        (is (nil? prepared))
        (is (= [:compiler-exception] (mapv first @events)))
        (is (re-find #"Invalid data" (ex-message failure)))
        (is (identical? failure (:?err (first @entries))))
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid data"
              (d/with-transaction [tx cn]
                (d/transact! tx forms))))
        (is (= "one" (:name (d/entity @cn 1))))))))

(deftest successful-preparation-and-declines-have-no-compiler-exceptions
  (with-db {}
    (fn [cn]
      (let [events (atom []) entries (atom []) callbacks (atom 0)
            host-value (proxy [Object] [] (toString [] "bound"))
            dynamic-forms [[:db.fn/call (fn [_] (swap! callbacks inc)
                                         [[:db/add 1 :value 7]])]]]
        (with-preparation-log entries
          #(binding [db/*tx-compiler-observer*
                     (fn [event _] (swap! events conj event))]
             (doseq [forms [[[:db/add 1 :value 7]]
                            [[:db/add 1 :name host-value]]
                            [{:name host-value}]]]
               (is (= :compiled (first (#'handlers/prepare-server-tx @cn forms)))))
             (is (nil? (#'handlers/prepare-server-tx @cn dynamic-forms)))))
        (is (zero? (count (filter #{:compiler-exception} @events))))
        (is (empty? @entries))
        (is (zero? @callbacks))))))
