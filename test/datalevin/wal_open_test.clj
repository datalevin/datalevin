(ns datalevin.wal-open-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.server :as server]
            [datalevin.test.core :refer [allocate-port db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(defn- open-store [api path opts]
  (let [opts (merge {:mapsize 16 :wal-segment-prealloc? false
                    :snapshot-bootstrap-force? false} opts)]
    (case api
      :kv (d/open-kv path opts)
      :datalog (d/create-conn
                 path {:counter {:db/valueType :db.type/long}}
                 (assoc (dissoc opts :mapsize :flags)
                        :kv-opts (select-keys opts [:mapsize :flags]))))))

(defn- kv-handle [api store]
  (if (= api :kv) store (d/datalog-kv store)))

(defn- close-store [api store]
  (if (= api :kv) (d/close-kv store) (d/close store)))

(defn- write-counter! [api store value]
  (case api
    :kv (do (d/open-dbi store "counter")
            (d/transact-kv store [[:put "counter" 1 value :id :long]]))
    :datalog (d/transact! store [{:db/id 1 :counter value}])))

(defn- read-counter [api store]
  (case api
    :kv (d/get-value store "counter" 1 :id :long)
    :datalog (:counter (d/pull @store [:counter] 1))))

(deftest wal-writemap-default-and-explicit-overrides
  (let [path (u/tmp-dir (str "wal-open-flags-" (random-uuid)))]
    (try
      (doseq [api [:kv :datalog]
              [label opts expected?]
              [[:ordinary {} false]
               [:disabled {:wal? false} false]
               [:wal {:wal? true} true]
               [:override {:wal? true :flags c/default-env-flags} false]
               [:explicit {:wal? false :flags (conj c/default-env-flags :writemap)} true]]]
        (testing (str api " " label)
          (let [dir (str path "/" (name api) "-" (name label))
                store (open-store api dir opts)]
            (try
              (let [kv (kv-handle api store)
                    flags (d/get-env-flags kv)]
                (is (= expected? (contains? flags :writemap)))
                (when (:wal? opts) (is (contains? flags :nosync))))
              (write-counter! api store 1)
              (is (= 1 (read-counter api store)))
              (finally (close-store api store))))))
      (finally (u/delete-files path)))))

(deftest wal-writemap-default-on-implicit-reopen
  (let [path (u/tmp-dir (str "wal-reopen-flags-" (random-uuid)))]
    (try
      (doseq [api [:kv :datalog]]
        (let [dir (str path "/" (name api))
              store (open-store api dir {:wal? true})]
          (try (write-counter! api store 1)
               (finally (close-store api store)))
          ;; WAL comes from kv-info, before Datalog loads its store options.
          (let [reopened (open-store api dir {})]
            (try
              (let [kv (kv-handle api reopened)]
                (is (contains? (d/get-env-flags kv) :writemap))
                (is (:wal? (d/txlog-watermarks kv))))
              (is (= 1 (read-counter api reopened)))
              (write-counter! api reopened 2)
              (is (= 2 (read-counter api reopened)))
              (finally (close-store api reopened))))
          (let [overridden (open-store api dir {:flags c/default-env-flags})]
            (try
              (let [kv (kv-handle api overridden)]
                (is (not (contains? (d/get-env-flags kv) :writemap)))
                (is (:wal? (d/txlog-watermarks kv))))
              (is (= 2 (read-counter api overridden)))
              (finally (close-store api overridden))))))
      (finally (u/delete-files path)))))

(deftest remote-wal-writemap-default-and-override
  (let [path (u/tmp-dir (str "remote-wal-open-flags-" (random-uuid)))
        port (allocate-port)
        srv (server/create {:root path :port port})]
    (try
      (server/start srv)
      (doseq [api [:kv :datalog] override? [false true]]
        (let [uri (str "dtlv://datalevin:datalevin@localhost:" port "/"
                       (name api) (when override? "-override"))
              opts (cond-> {:wal? true}
                     override? (assoc :flags c/default-env-flags))
              store (open-store api uri opts)]
          (try
            (let [flags (d/get-env-flags (kv-handle api store))]
              (is (= (not override?) (contains? flags :writemap)))
              (is (contains? flags :nosync)))
            (write-counter! api store 1)
            (is (= 1 (read-counter api store)))
            (finally (close-store api store)))))
      (finally (server/stop srv) (u/delete-files path)))))

(deftest persisted-wal-key-encodings-and-disabled-override
  (let [path (u/tmp-dir (str "wal-open-metadata-" (random-uuid)))]
    (try
      (doseq [[label legacy typed expected?]
              [[:legacy true nil true]
               [:enabled false true true]
               [:disabled true false false]]]
        (let [dir (str path "/" (name label))
              db (open-store :kv dir {:wal? false})]
          (try
            (d/transact-kv db [[:put c/kv-info :wal? legacy :data :data]
                               (if (some? typed)
                                 [:put c/kv-info :wal? typed :keyword :boolean]
                                 [:del c/kv-info :wal? :keyword])])
            (finally (d/close-kv db)))
          (let [reopened (open-store :kv dir {})]
            (try
              (is (= expected? (contains? (d/get-env-flags reopened) :writemap)))
              (is (= expected? (:wal? (d/txlog-watermarks reopened))))
              (finally (d/close-kv reopened))))
          (let [disabled (open-store :kv dir {:wal? false})]
            (try
              (is (not (contains? (d/get-env-flags disabled) :writemap)))
              (is (false? (:wal? (d/txlog-watermarks disabled))))
              (finally (d/close-kv disabled))))))
      (finally (u/delete-files path)))))
