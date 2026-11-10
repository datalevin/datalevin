(ns datalevin.admin-backup-cleanup-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.admin :as admin]
            [datalevin.constants :as c]
            [datalevin.util :as u])
  (:import [java.io IOException]
           [java.nio.file AccessDeniedException Files Path]
           [java.nio.file.attribute FileAttribute]))

(defn- with-root [f]
  (let [root (u/tmp-dir (str "backup-cleanup-" (random-uuid)))]
    (u/create-dirs root)
    (try (f root) (finally (u/delete-files root)))))

(defn- run-backup [root names overrides]
  (let [results (atom []) requests (atom []) copies (atom []) closed (atom 0)
        exit
        (admin/run! {:all-databases true} ["backup" (str root "/backup")]
          (merge
            {:getenv (constantly "dtlv://user:password@localhost")
             :connect (fn [& _] :client)
             :disconnect (fn [_] (swap! closed inc))
             :request (fn [_ op args]
                        (swap! requests conj op)
                        (case op
                          :list-databases names
                          :list-databases-in-use []
                          :query-system (mapv #(vector % :key-value 1) names)
                          :admin-status {:db-name (first args) :db-id 1 :db-type :key-value
                                         :admin-capabilities #{:guarded-maintenance :standalone-copy}}))
             :backup (fn [_ target ^Path file]
                       (swap! copies conj (:db-name target))
                       (spit (str file) "verified bytes")
                       (spit (str (.resolve (.getParent file) "VERSION")) c/version)
                       {:verified? true :copy-format :binary-chunks})
             :emit #(swap! results conj %)
             :diagnostic (fn [_])}
            overrides))]
    {:exit exit :results @results :requests @requests :copies @copies :closed @closed}))

(deftest empty-discovery-does-not-create-a-destination
  (with-root
    (fn [root]
      (let [result (run-backup root []
                     {:create-directory! (fn [_] (throw (AssertionError. "No directory should be created")))})]
        (is (= 0 (:exit result)))
        (is (empty? (:results result)))
        (is (empty? (:copies result)))
        (is (= [:list-databases :list-databases-in-use :query-system] (:requests result)))
        (is (= 1 (:closed result)))
        (is (not (u/file-exists (str root "/backup"))))))))

(deftest destination-setup-failures-clean-up-and-allow-the-same-path-to-be-retried
  (doseq [after-create? [false true]]
    (testing (str {:after-create? after-create?})
      (with-root
        (fn [root]
          (let [calls (atom 0)
                create! (fn [^Path path]
                          (if (= 2 (swap! calls inc))
                            (do
                              (when after-create? (Files/createDirectory path (make-array FileAttribute 0)))
                              (throw (AccessDeniedException. (str path))))
                            (Files/createDirectory path (make-array FileAttribute 0))))
                failed (run-backup root ["one"] {:create-directory! create!})]
            (is (= 2 (:exit failed)))
            (is (not-any? #{:admin-status} (:requests failed)))
            (is (empty? (:copies failed)))
            (is (not (u/file-exists (str root "/backup"))))
            (let [retried (run-backup root ["one"] {:create-directory! create!})]
              (is (= 0 (:exit retried)))
              (is (= ["one"] (:copies retried)))
              (is (= "verified bytes" (slurp (str root "/backup/one/data.mdb"))))
              (is (not (u/file-exists (str root "/backup/.incomplete")))))))))))

(deftest successful-sweeps-remove-the-empty-staging-directory
  (with-root
    (fn [root]
      (let [result (run-backup root ["one" "two"] {})]
        (is (= 0 (:exit result)))
        (is (= ["one" "two"] (:copies result)))
        (doseq [name ["one" "two"]]
          (is (= "verified bytes" (slurp (str root "/backup/" name "/data.mdb"))))
          (is (= c/version (slurp (str root "/backup/" name "/VERSION")))))
        (is (not (u/file-exists (str root "/backup/.incomplete"))))))))

(deftest target-discovery-errors-remove-an-unused-destination
  (with-root
    (fn [root]
      (let [failed (run-backup root ["one"]
                     {:request (fn [_ op _]
                                 (case op
                                   :list-databases ["one"]
                                   :list-databases-in-use []
                                   :query-system [["one" :key-value 1]]
                                   :admin-status (throw (ex-info "Database disappeared"
                                                                {:error :admin/database-not-found}))))})]
        (is (= 1 (:exit failed)))
        (is (empty? (:copies failed)))
        (is (not (u/file-exists (str root "/backup"))))
        (is (= 0 (:exit (run-backup root ["one"] {}))))))))

(deftest setup-cleanup-preserves-nonempty-and-preexisting-directories
  (with-root
    (fn [root]
      (let [calls (atom 0)
            create! (fn [^Path path]
                      (Files/createDirectory path (make-array FileAttribute 0))
                      (when (= 2 (swap! calls inc))
                        (spit (str (.resolve path "retained")) "keep these bytes")
                        (throw (IOException. "Setup failed after staging bytes"))))
            failed (run-backup root ["one"] {:create-directory! create!})]
        (is (= 2 (:exit failed)))
        (is (empty? (:copies failed)))
        (is (= "keep these bytes" (slurp (str root "/backup/.incomplete/retained"))))
        (is (= 2 (:exit (run-backup root ["one"] {}))))
        (is (= "keep these bytes" (slurp (str root "/backup/.incomplete/retained"))))))))

(deftest sweep-cleanup-preserves-failed-payloads
  (with-root
    (fn [root]
      (let [result (run-backup root ["one"]
                     {:backup (fn [_ _ file]
                                (spit (str file) "partial bytes")
                                (throw (IOException. "Connection closed")))})]
        (is (= 1 (:exit result)))
        (is (= "partial bytes" (slurp (str root "/backup/.incomplete/one/data.mdb"))))))))
