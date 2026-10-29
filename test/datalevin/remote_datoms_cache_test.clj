(ns datalevin.remote-datoms-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.db :as db]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :once server-fixture)

(defn- with-connections [f]
  (let [db-name (str "datoms-cache-" (random-uuid))
        uri #(str "dtlv://datalevin:datalevin@" % "/" db-name)
        opts {:background-sampling? false
              :client-opts {:pool-size 1 :time-out 5000}}]
    ;; Separate URIs prevent the writer from invalidating the reader's cache
    ;; in this JVM, as when the writer runs in another process.
    (with-open [^java.io.Closeable reader
                (d/create-conn (uri "localhost")
                               {:name {:db/valueType :db.type/string}} opts)
                ^java.io.Closeable writer
                (d/create-conn (uri "127.0.0.1") nil opts)]
      (is (not= (i/dir (:store @reader)) (i/dir (:store @writer))))
      (f reader writer))))

(def ^:private index-reads
  [[:eav-all #(d/datoms % :eav) ["old"] ["new"]]
   [:eav-entity #(d/datoms % :eav 1) ["old"] ["new"]]
   [:eav-attribute #(d/datoms % :eav 1 :name) ["old"] ["new"]]
   [:eav-value #(d/datoms % :eav 1 :name "old") ["old"] []]
   [:eav-missing #(d/datoms % :eav 1 :name "new") [] ["new"]]
   [:ave-all #(d/datoms % :ave) ["old"] ["new"]]
   [:ave-attribute #(d/datoms % :ave :name) ["old"] ["new"]]
   [:ave-value #(d/datoms % :ave :name "old") ["old"] []]
   [:ave-entity #(d/datoms % :ave :name "old" 1) ["old"] []]
   [:ave-missing #(d/datoms % :ave :name "new" 1) [] ["new"]]
   [:seek #(d/seek-datoms % :eav 1 :name) ["old"] ["new"]]
   [:rseek #(d/rseek-datoms % :eav 1 :name) ["old"] ["new"]]
   [:range #(d/index-range % :name nil nil) ["old"] ["new"]]
   [:search #(d/search-datoms % 1 :name nil) ["old"] ["new"]]])

(deftest cached-index-reads-observe-external-commits
  (with-connections
    (fn [reader writer]
      ;; Keep one DB view throughout; dereferencing a new view or calling db?
      ;; must not be necessary to validate a direct index read.
      (let [view @reader]
        (doseq [[label read before after] index-reads]
          (testing (name label)
            (d/transact! writer [{:db/id 1 :name "old"}])
            (dotimes [_ 2] (is (= before (mapv :v (read view)))))
            (d/transact! writer [[:db/add 1 :name "new"]])
            (dotimes [_ 2] (is (= after (mapv :v (read view)))))))))))

(deftest cached-datoms-honor-the-freshness-interval
  (with-connections
    (fn [reader writer]
      (d/transact! writer [{:db/id 1 :name "old"}])
      (let [view @reader
            read #(d/datoms view :eav 1 :name)
            old (read)]
        (binding [c/*remote-db-last-modified-check-interval-ms* 60000]
          (is (identical? old (read)))
          (d/transact! writer [[:db/add 1 :name "new"]])
          (is (identical? old (read)))
          (is (= ["old"] (mapv :v (read)))))
        (binding [c/*remote-db-last-modified-check-interval-ms* 0]
          (is (= ["new"] (mapv :v (read)))))))))

(deftest local-writes-do-not-hide-unobserved-external-commits
  (doseq [interval [0 60000]
          tx [[{:db/id 2 :name "unrelated"}] [] [[:db/add 1 :name "new"]]]]
    (testing (str "interval=" interval ", local transaction=" tx)
      (with-connections
        (fn [reader writer]
          (d/transact! reader [{:db/id 1 :name "old"}])
          (binding [c/*remote-db-last-modified-check-interval-ms* interval]
            (let [view @reader
                  query '[:find ?name . :where [1 :name ?name]]
                  prepared (d/prepare-q view query)
                  reads [[#(mapv :v (d/datoms view :eav 1 :name)) ["old"] ["new"]]
                         [#(d/pull view [:name] 1) {:name "old"} {:name "new"}]
                         [#(d/q query view) "old" "new"]
                         [#(prepared []) "old" "new"]]]
              (doseq [[read before] reads]
                (is (= before (read))))
              ;; A reader that already captured the old generation must also
              ;; be prevented from publishing after a no-op write completes.
              (let [token (db/cache-token (:store view))]
                (d/transact! writer [[:db/add 1 :name "new"]])
                (d/transact! reader tx)
                (is (false? (db/cache-put-if-current
                              (:store view) token ::old-read ["old"]))))
              (is (= {:name "new"} (i/pull (:store view) [:name] 1 {})))
              (dotimes [_ 2]
                (doseq [[read _ after] reads]
                  (is (= after (read))))))))))))
