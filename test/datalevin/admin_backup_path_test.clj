(ns datalevin.admin-backup-path-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [datalevin.admin :as admin]
            [datalevin.constants :as c]
            [datalevin.util :as u])
  (:import [java.nio.file Path]))

(defn- backup-name [db-name all? allowed?]
  (let [root (u/tmp-dir (str "backup-path-" (random-uuid)))
        destination (str root "/backup")
        requests (atom [])
        copies (atom [])
        closed (atom 0)]
    (u/create-dirs root)
    (try
      (let [exit
            (admin/run! (if all? {:all-databases true} {:database [db-name]})
              ["backup" destination]
              {:getenv (constantly "dtlv://user:password@localhost")
               :connect (fn [& _] :client)
               :disconnect (fn [_] (swap! closed inc))
               :request
               (fn [_ op args]
                 (case op
                   :list-databases [db-name]
                   :list-databases-in-use []
                   :query-system [[db-name :key-value 1]]
                   :admin-status
                   (let [name (first args)]
                     (swap! requests conj name)
                     {:db-name name :db-id 1 :db-type :key-value
                      :admin-capabilities #{:guarded-maintenance :standalone-copy}})))
               :backup
               (fn [_ target ^Path file]
                 (swap! copies conj (:db-name target))
                 (when-not allowed?
                   (throw (ex-info "Invalid name reached the copy callback" {})))
                 (spit (str file) "verified bytes")
                 (spit (str (.resolve (.getParent file) "VERSION")) c/version)
                 {:verified? true :copy-format :binary-chunks})
               :emit (fn [_]) :diagnostic (fn [_])})]
        {:exit exit :requests @requests :copies @copies :closed @closed
         :destination? (u/file-exists destination)
         :published? (u/file-exists (str destination "/" db-name "/data.mdb"))})
      (finally (u/delete-files root)))))

(deftest backup-rejects-windows-device-names-before-creating-destination
  (let [devices (concat ["con" "prn" "aux" "nul" "conin$" "conout$"]
                        (for [prefix ["com" "lpt"] digit (concat (range 1 10) ["¹" "²" "³"])]
                          (str prefix digit)))]
    (doseq [device devices
            name [device (str/upper-case device) (str/capitalize device)
                  (str device ".backup") (str/upper-case (str device ".tar.gz"))]]
      (testing name
        (let [result (backup-name name true false)]
          (is (= 2 (:exit result)))
          (is (empty? (:requests result)))
          (is (empty? (:copies result)))
          (is (not (:destination? result)))
          (is (= 1 (:closed result))))))
    ;; Named targets go through normal option parsing and normalization too.
    (doseq [name ["CON" "Prn" "aux" "NUL" "com1" "com9" "lpt1" "lpt9"]]
      (testing name
        (let [result (backup-name name false false)]
          (is (= 2 (:exit result)))
          (is (empty? (:requests result)))
          (is (not (:destination? result))))))))

(deftest backup-reserves-incomplete-marker-without-regard-to-case
  (doseq [name [".incomplete" ".INCOMPLETE" ".Incomplete" ".iNcOmPlEtE"]]
    (let [result (backup-name name true false)]
      (is (= 2 (:exit result)))
      (is (empty? (:requests result)))
      (is (not (:destination? result))))))

(deftest backup-allows-names-that-only-resemble-reserved-components
  (doseq [name ["console" "auxiliary" "null" "prn-backup" "con-backup"
                "com" "com0" "com10" "lpt" "lpt0" "lpt10" "conin" "conout"
                ".incomplete-copy" "incomplete"]]
    (testing name
      (let [result (backup-name name true true)]
        (is (= 0 (:exit result)))
        (is (= [name] (:requests result) (:copies result)))
        (is (:published? result))))))
