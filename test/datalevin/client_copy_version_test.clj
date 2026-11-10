(ns datalevin.client-copy-version-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.client :as client]
            [datalevin.client.copy :as copy]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.protocol :as protocol]
            [datalevin.remote :as remote]
            [datalevin.util :as u])
  (:import [datalevin.client ConnectionPool]
           [java.nio ByteBuffer]
           [java.nio.channels SocketChannel]
           [java.nio.channels.spi SelectorProvider]
           [java.nio.file Files]
           [java.security MessageDigest]
           [java.util Base64]
           [java.util.concurrent.atomic AtomicBoolean]))

(def ^:private payload (byte-array [1 2 3 4]))
(def ^:private target {:db-name "orders" :db-id 1 :db-type :key-value})

(defn- transport [metadata binary?]
  (let [frames (atom [])
        requests (atom [])
        header (cond-> {:type :copy-out-response :copy-meta metadata}
                 binary? (assoc :copy-format :binary-chunks :checksum-algo :sha-256))
        done (cond-> {:type :copy-done}
               binary? (assoc :bytes 4 :chunks 1 :checksum-algo :sha-256
                              :checksum (u/hexify (.digest (MessageDigest/getInstance "SHA-256") payload))))
        channel (proxy [SocketChannel] [(SelectorProvider/provider)]
                  (write [^ByteBuffer src]
                    (let [buffer (.duplicate src)
                          format (.get buffer)
                          _ (.getInt buffer)
                          message (protocol/read-value format buffer)
                          responses (if (= :admin-status (:type message))
                                      [{:type :command-complete
                                        :result (assoc target :admin-capabilities
                                                       #{:guarded-maintenance :standalone-copy})}]
                                      [header [(if binary? payload
                                                   (.encodeToString (Base64/getEncoder) payload))] done])
                          size (.remaining src)]
                      (swap! requests conj (:type message))
                      (reset! frames
                              (mapv #(doto (ByteBuffer/allocate 65536)
                                       (protocol/write-message-bf % c/message-format-nippy)
                                       (.flip))
                                    responses))
                      (.position src (.limit src))
                      size))
                  (read [^ByteBuffer dst]
                    (if-let [^ByteBuffer frame (first @frames)]
                      (let [size (.remaining frame)]
                        (.put dst frame)
                        (swap! frames subvec 1)
                        size)
                      0))
                  (implConfigureBlocking [_])
                  (implCloseSelectableChannel []))
        connection (client/->Connection channel 5000 (ByteBuffer/allocate 65536))
        ^ConnectionPool pool (#'client/connection-pool "localhost" 19001 "session" 1 5000)
        _ (#'client/install-pool-connection! pool (aget ^objects (.-slots pool) 0) connection)]
    {:client (client/->Client "user" "password" "localhost" 19001 1 5000 "session" pool)
     :pool pool :requests requests}))

(defn- with-copy [metadata binary? f]
  (let [root (u/tmp-dir (str "copy-version-" (random-uuid)))
        connection (transport metadata binary?)]
    (u/create-dirs root)
    (try (f root connection)
         (finally
           (client/close-pool (:pool connection))
           (u/delete-files root)))))

(deftest file-copies-use-the-server-storage-version
  (doseq [version ["1.1.0" c/version "99.0.0"] standalone? [false true]]
    (testing (str {:version version :standalone? standalone?})
      (with-copy (assoc target :storage-version version) true
        (fn [root {:keys [client requests]}]
          (let [response (client/copy-to-file client "orders" (str root "/data.mdb")
                                               {:standalone? standalone?})]
            (is (= version (slurp (str root "/VERSION"))))
            (is (= version (get-in response [:copy-meta :storage-version])))
            (is (true? (:verified? response)))
            (is (= (seq payload) (seq (Files/readAllBytes (copy/path (str root "/data.mdb"))))))
            (is (= (if standalone? [:admin-status :copy] [:copy]) @requests))))))))

(deftest ordinary-remote-copy-uses-the-server-storage-version
  (doseq [version ["1.1.0" c/version "99.0.0"]]
    (with-copy (assoc target :storage-version version) true
      (fn [root {:keys [client]}]
        (let [store (remote/->KVStore "unused" "orders" client client nil false nil false (AtomicBoolean.))]
          (is (= version (:storage-version (i/copy store root false))))
          (is (= version (slurp (str root "/VERSION"))))
          (is (= (seq payload) (seq (Files/readAllBytes (copy/path (str root "/data.mdb")))))))))))

(deftest unsafe-version-metadata-never-overwrites-an-existing-version
  (doseq [[version code] [[nil :copy/missing-storage-version]
                         ["" :copy/invalid-storage-version]
                         ["not-a-version" :copy/invalid-storage-version]
                         [7 :copy/invalid-storage-version]
                         ["999999999999999999999.0.0" :copy/invalid-storage-version]]
          mode [:file :standalone :remote]]
    (testing (str {:version version :mode mode})
      (with-copy (cond-> target version (assoc :storage-version version)) true
        (fn [root {:keys [client requests]}]
          (spit (str root "/VERSION") "existing version")
          (spit (str root "/data.mdb") "existing backup")
          (let [error (try
                        (case mode
                          :file (client/copy-to-file client "orders" (str root "/data.mdb"))
                          :standalone (client/admin-backup! client target (str root "/data.mdb"))
                          :remote (i/copy (remote/->KVStore "unused" "orders" client client nil false nil false
                                                           (AtomicBoolean.))
                                          root false))
                        nil
                        (catch Exception e e))]
            (is (= code (:error (ex-data error))))
            (is (= "existing version" (slurp (str root "/VERSION"))))
            (when (= mode :remote)
              (is (= "existing backup" (slurp (str root "/data.mdb")))))
            (is (= 1 (count (filter #{:copy} @requests))))))))))

(deftest raw-copies-can-skip-version-metadata
  (doseq [binary? [false true]]
    (with-copy target binary?
      (fn [root {:keys [client]}]
        (let [response (client/copy-to-file client "orders" (str root "/data.mdb") {:version-file nil})]
          (is (= :command-complete (:type response)))
          (is (= (seq payload) (seq (Files/readAllBytes (copy/path (str root "/data.mdb"))))))
          (is (not (u/file-exists (str root "/VERSION")))))))))
