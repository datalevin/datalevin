(ns datalevin.admin-backup-io-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.admin :as admin]
            [datalevin.client.copy :as copy]
            [datalevin.constants :as c]
            [datalevin.util :as u])
  (:import [java.io EOFException IOException]
           [java.net SocketTimeoutException]
           [java.nio.channels ClosedByInterruptException]
           [java.nio.file AccessDeniedException Path]))

(defn- backup-sweep [fail-first & [block-stage?]]
  (let [root (u/tmp-dir (str "backup-local-io-" (random-uuid)))
        destination (str root "/backup")
        results (atom []) copies (atom []) requests (atom []) closed (atom 0)]
    (u/create-dirs root)
    (try
      (let [exit
            (admin/run! {:database ["first" "later"]} ["backup" destination]
              {:getenv (constantly "dtlv://user:password@localhost")
               :connect (fn [& _] :client) :disconnect (fn [_] (swap! closed inc))
               :request
               (fn [_ _ [name]]
                 (swap! requests conj name)
                 (when (and block-stage? (= name "first"))
                   (u/create-dirs (str destination "/.incomplete/first")))
                 {:db-name name :db-id (if (= name "first") 1 2) :db-type :key-value
                  :admin-capabilities #{:guarded-maintenance :standalone-copy}})
               :backup
               (fn [_ target ^Path file]
                 (swap! copies conj (:db-name target))
                 (if (= "first" (:db-name target))
                   (fail-first file)
                   (do (spit (str file) "verified bytes")
                       (spit (str (.resolve (.getParent file) "VERSION")) c/version)
                       {:verified? true :copy-format :binary-chunks})))
               :emit #(swap! results conj %) :diagnostic (fn [_])})]
        {:exit exit :results @results :copies @copies :requests @requests :closed @closed
         :incomplete? (u/file-exists (str destination "/.incomplete/first"))
         :published? (u/file-exists (str destination "/later/data.mdb"))})
      (finally (u/delete-files root)))))

(deftest local-filesystem-and-sink-errors-continue-to-later-targets
  (doseq [[label fail-first]
          [[:access-denied (fn [file] (throw (AccessDeniedException. (str file))))]
           [:wrapped-access-denied
            (fn [file]
              (throw (ex-info "Cannot stage backup" {:admin/maintenance-sent? false}
                              (AccessDeniedException. (str file)))))]
           [:local-io
            (fn [file]
              (copy/receiver file {:sink-factory (fn [_] (throw (IOException. "Disk full")))}))]
           [:local-auth-message
            (fn [file]
              (copy/receiver file
                {:sink-factory (fn [_] (throw (IOException. "Authentication failure writing file")))}))]
           [:wrapped-local-io
            (fn [file]
              (copy/receiver file
                {:sink-factory (fn [_]
                                 (throw (ex-info "Disk full" {:error ::disk-full}
                                                 (IOException. "No space left"))))}))]]]
    (testing (name label)
      (let [result (backup-sweep fail-first)]
        (is (= 1 (:exit result)))
        (is (= ["first" "later"] (:requests result)))
        (is (= ["first" "later"] (:copies result)))
        (is (= ["first" "later"] (mapv :db-name (:results result))))
        (is (get-in result [:results 0 :error]))
        (when (= :wrapped-local-io label)
          (is (= ::disk-full (get-in result [:results 0 :error :data :error]))))
        (is (true? (get-in result [:results 1 :result :verified?])))
        (is (:incomplete? result))
        (is (:published? result))
        (is (= 1 (:closed result)))))))

(deftest local-staging-directory-errors-continue-before-a-copy-is-sent
  (let [result (backup-sweep (fn [_] (throw (AssertionError. "First copy must not run"))) true)]
    (is (= 1 (:exit result)))
    (is (= ["first" "later"] (:requests result)))
    (is (= ["later"] (:copies result)))
    (is (= 2 (count (:results result))))
    (is (:published? result))))

(deftest transport-uncertainty-and-interruption-still-stop-backup
  (doseq [failure [(IOException. "Connection closed")
                   (EOFException. "Reply lost")
                   (SocketTimeoutException. "Reply timed out")]]
    (let [result (backup-sweep (fn [_] (throw failure)))]
      (is (= 1 (:exit result)))
      (is (= ["first"] (:requests result)))
      (is (= ["first"] (:copies result)))
      (is (= 1 (count (:results result))))
      (is (not (:published? result)))))
  (let [result (backup-sweep
                 (fn [file]
                   (copy/receiver file
                     {:sink-factory (fn [_] (throw (ClosedByInterruptException.)))})))]
    (is (= 1 (:exit result)))
    (is (= ["first"] (:requests result)))
    (is (not (:published? result)))))
