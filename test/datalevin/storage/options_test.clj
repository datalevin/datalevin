(ns datalevin.storage.options-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.storage.options :as options]
   [datalevin.util :as u])
  (:import [java.util UUID]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "storage-option-api-" (UUID/randomUUID)))
              c/*db-background-sampling?* false]
      (try (f) (finally (u/delete-files *dir*))))))

(deftest persist-options-for-rollback-and-reopen-test
  (doseq [wal? [false true]]
    (let [dir   (str *dir* "/" wal?)
          conn  (d/create-conn dir nil {:wal? wal? :validate-data? true})
          store (:store @conn)
          lmdb  (d/datalog-kv conn)
          old   (i/opts store)]
      (try
        (i/assoc-opt store :validate-data? false)
        (is (false? (:validate-data? (options/load-opts lmdb))))
        (options/transact-opts lmdb old)
        (is (true? (:validate-data? (options/load-opts lmdb))))
        (is (false? (:validate-data? (i/opts store)))
            "persistence leaves cache replacement to the rollback caller")
        (i/transact-kv lmdb [[:put c/meta :last-modified 17 :attr :long]])
        (options/transact-opts lmdb (assoc old :runtime-opts {:test true}
                                              :ha-node-id 99))
        (is (= 17 (i/get-value lmdb c/meta :last-modified :attr :long))
            "runtime-only differences do not rewrite persistent options")
        (is (not (contains? (options/load-opts lmdb) :runtime-opts)))
        (is (not (contains? (options/load-opts lmdb) :ha-node-id)))
        (finally (d/close conn)))
      (let [reopened (d/create-conn dir)]
        (try
          (is (true? (:validate-data? (i/opts (:store @reopened)))))
          (finally (d/close reopened)))))))
