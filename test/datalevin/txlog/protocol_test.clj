(ns datalevin.txlog.protocol-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [datalevin.tx-state.lifetime :as lifetime]
            [datalevin.tx-state.protocol :as protocol]
            [datalevin.util :as u])
  (:import [java.lang AutoCloseable ProcessBuilder]
           [java.util.concurrent TimeUnit]))

(defn- caught [f] (try (f) (catch Throwable e e)))
(defn- child ^Process [form]
  (let [^java.util.List command
        [(str (System/getProperty "java.home") "/bin/java")
         "-cp" (System/getProperty "java.class.path")
         "clojure.main" "-e" (pr-str form)]]
    (.start (doto (ProcessBuilder. command) (.redirectErrorStream true)))))

(deftest marker-and-canonical-path-exclusion
  (let [dir (u/tmp-dir (str "wal-protocol-" (random-uuid)))
        lease (protocol/acquire-write-protocol-lease! dir :kv-independent-v1 "db-test")
        alias (protocol/acquire-write-protocol-lease! (str dir "/.")
                                                       :kv-independent-v1 "db-test")]
    (try
      (is (identical? (:holder lease) (:holder alias)))
      (is (= {:version 1 :mode :kv-independent-v1 :db-identity "db-test"}
             (dissoc (protocol/read-write-protocol-marker dir) :generation)))
      (is (= :txlog/write-protocol-mismatch
             (:error (ex-data (caught #(protocol/acquire-write-protocol-lease!
                                        dir :legacy-writer-v1 "db-test"))))))
      (is (= :txlog/write-protocol-mismatch
             (:error (ex-data (caught #(protocol/acquire-write-protocol-lease!
                                        dir :kv-independent-v1 "other-db"))))))
      (let [proc (child `(do
                          (require '~'[datalevin.tx-state.protocol :as p])
                          (try
                            (~'p/acquire-write-protocol-lease! ~dir :kv-independent-v1 "db-test")
                            (System/exit 3)
                            (catch clojure.lang.ExceptionInfo ~'e
                              (println (:error (ex-data ~'e)))
                              (System/exit 0)))))]
        (try
          (is (.waitFor proc 15 TimeUnit/SECONDS))
          (is (zero? (.exitValue proc)))
          (is (.contains ^String (slurp (.getInputStream proc))
                         ":txlog/write-protocol-in-use"))
          (finally
            (when (.isAlive proc) (.destroyForcibly proc) (.waitFor proc)))))
      (finally
        (protocol/release! alias)
        (protocol/release! lease)
        (u/delete-files dir)))))

(deftest fenced-native-owner-retains-registry-and-file-lease
  (let [dir (u/tmp-dir (str "wal-protocol-fenced-" (random-uuid)))
        lease (protocol/acquire-write-protocol-lease! dir :kv-independent-v1 "db-test")
        guard (lifetime/create)
        native (lifetime/enter! guard)]
    (protocol/bind-lifetime! lease guard)
    (try
      (lifetime/fence! guard)
      (is (= :txlog/native-not-quiescent
             (:error (ex-data (caught #(protocol/release! lease))))))
      (is (= :txlog/native-fenced
             (:error (ex-data (caught #(protocol/acquire-write-protocol-lease!
                                        (str dir "/.") :kv-independent-v1 "db-test"))))))
      (.close ^AutoCloseable native)
      (is (= :txlog/native-not-quiescent
             (:error (ex-data (caught #(protocol/release! lease))))))
      (lifetime/close! guard 1000 (fn []))
      (protocol/release! lease)
      (let [new-lease (protocol/acquire-write-protocol-lease! dir :kv-independent-v1 "db-test")]
        (protocol/release! new-lease))
      (finally
        (.close ^AutoCloseable native)
        (lifetime/close! guard 1000 (fn []))
        (protocol/release! lease)
        (u/delete-files dir)))))

(deftest process-exit-is-required-before-reopening-an-unquiescent-owner
  (let [dir (u/tmp-dir (str "wal-protocol-process-" (random-uuid)))
        ready (str (io/file dir "owner-ready"))
        proc (child `(do
                       (require '~'[datalevin.tx-state.protocol :as p])
                       (require '~'[datalevin.tx-state.lifetime :as l])
                       (let [~'lease (~'p/acquire-write-protocol-lease!
                                     ~dir :kv-independent-v1 "db-test")
                             ~'guard (~'l/create)]
                         (~'p/bind-lifetime! ~'lease ~'guard)
                         (~'l/enter! ~'guard)
                         (~'l/fence! ~'guard)
                         (spit ~ready "fenced")
                         (loop []
                           (try (Thread/sleep 1000) (catch InterruptedException ~'_))
                           (recur)))))]
    (try
      (is (loop [n 0]
            (cond (.exists (io/file ready)) true
                  (or (>= n 1500) (not (.isAlive proc))) false
                  :else (do (Thread/sleep 10) (recur (inc n))))))
      (is (= :txlog/write-protocol-in-use
             (:error (ex-data (caught #(protocol/acquire-write-protocol-lease!
                                        dir :kv-independent-v1 "db-test"))))))
      (.destroyForcibly proc)
      (is (.waitFor proc 10 TimeUnit/SECONDS))
      (is (not (.isAlive proc)))
      (let [lease (protocol/acquire-write-protocol-lease! dir :kv-independent-v1 "db-test")]
        (protocol/release! lease))
      (finally
        (when (.isAlive proc) (.destroyForcibly proc) (.waitFor proc))
        (u/delete-files dir)))))
