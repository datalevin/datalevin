(ns datalevin.ha-log-storage-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.util :as u])
  (:import
   [com.alipay.sofa.jraft.conf ConfigurationManager]
   [com.alipay.sofa.jraft.entity EnumOutter$EntryType LogEntry LogId]
   [com.alipay.sofa.jraft.entity.codec.v2 LogEntryV2CodecFactory]
   [com.alipay.sofa.jraft.option LogStorageOptions RaftOptions]
   [datalevin.cpp BufVal Dbi Env Txn]
   [datalevin.ha LMDBLogStorage]
   [java.lang.reflect Modifier]
   [java.nio ByteBuffer]
   [java.util UUID]))

(def ^:private default-map-size-bytes (* 64 1024 1024))
(def ^:private shrunken-map-size-bytes (* 1024 1024))
(def ^:private entry-payload-bytes (* 2 1024 1024))

(declare log-storage-options make-log-storage make-log-entry)

(defn- private-field
  [^Class cls field-name]
  (doto (.getDeclaredField cls field-name)
    (.setAccessible true)))

(defn- read-private-field
  [obj field-name]
  (.get ^java.lang.reflect.Field (private-field (class obj) field-name) obj))

(defn- read-private-long
  [obj field-name]
  (.getLong ^java.lang.reflect.Field (private-field (class obj) field-name) obj))

(defn- long-bytes
  [value]
  (let [buf (ByteBuffer/allocate Long/BYTES)]
    (.putLong buf (long value))
    (.array buf)))

(defn- buf-val
  [^bytes bytes]
  (let [buf-val (BufVal. (max 1 (alength bytes)))
        buf (.inBuf buf-val)]
    (.clear buf)
    (.put buf bytes)
    (.flip buf)
    (.reset buf-val)
    buf-val))

(defn- overwrite-log-entry-bytes!
  [^LMDBLogStorage storage index ^bytes bytes]
  (let [^Env env (read-private-field storage "env")
        ^Dbi log-dbi (read-private-field storage "logDbi")
        txn (Txn/create env)]
    (try
      (.put log-dbi txn
            (buf-val (long-bytes index))
            (buf-val bytes)
            0)
      (.commit txn)
      (finally
        (.close txn)))))

(deftest lmdb-log-storage-first-log-index-cache-fields-are-volatile-test
  (is (Modifier/isVolatile
       (.getModifiers (private-field LMDBLogStorage "firstLogIndex"))))
  (is (Modifier/isVolatile
       (.getModifiers (private-field LMDBLogStorage "hasLoadedFirstLogIndex")))))

(deftest lmdb-log-storage-get-entry-throws-on-corrupt-bytes-test
  (let [dir (u/tmp-dir (str "ha-log-storage-corrupt-" (UUID/randomUUID)))
        ^LMDBLogStorage storage (make-log-storage dir)]
    (try
      (let [initialized? (.init storage (log-storage-options))]
        (is initialized?)
        (when initialized?
          (is (.appendEntry storage (make-log-entry 1 64)))
          (overwrite-log-entry-bytes! storage 1 (byte-array [1 2 3 4]))
          (try
            (.getEntry storage 1)
            (is false "Expected corrupt log bytes to throw")
            (catch IllegalStateException e
              (is (re-find #"Bad log entry format" (ex-message e)))))))
      (finally
        (.shutdown storage)
        (u/delete-files dir)))))

(defn- log-storage-options
  []
  (doto (LogStorageOptions.)
    (.setGroupId (str "ha-log-storage-" (UUID/randomUUID)))
    (.setConfigurationManager (ConfigurationManager.))
    (.setLogEntryCodecFactory (LogEntryV2CodecFactory/getInstance))))

(defn- make-log-storage
  [dir]
  (LMDBLogStorage. dir (RaftOptions.)))

(defn- make-log-entry
  [index payload-size]
  (doto (LogEntry. EnumOutter$EntryType/ENTRY_TYPE_DATA)
    (.setId (LogId. index 1))
    (.setData (ByteBuffer/wrap (byte-array payload-size)))))

(deftest lmdb-log-storage-resizes-on-map-full-and-remains-appendable-after-restart-test
  (let [dir     (u/tmp-dir (str "ha-log-storage-" (UUID/randomUUID)))
        entries [(make-log-entry 1 entry-payload-bytes)
                 (make-log-entry 2 entry-payload-bytes)]
        ^LMDBLogStorage storage (make-log-storage dir)]
    (try
      (let [initialized? (.init storage (log-storage-options))]
        (is initialized?)
        (when initialized?
          (let [^Env env           (read-private-field storage "env")
                initial-map-size   (read-private-long storage "mapSizeBytes")]
            (is (= default-map-size-bytes initial-map-size))
            (.setMapSize env shrunken-map-size-bytes)

            (is (= 2 (.appendEntries storage entries)))
            (let [first-entry       (.getEntry storage 1)
                  second-entry      (.getEntry storage 2)
                  expanded-map-size (read-private-long storage "mapSizeBytes")]
              (is (= 2 (.getLastLogIndex storage)))
              (is (some? first-entry))
              (is (some? second-entry))
              (is (= 1 (-> ^LogEntry first-entry .getId .getIndex)))
              (is (= 2 (-> ^LogEntry second-entry .getId .getIndex)))
              (is (> expanded-map-size initial-map-size)))

            (.shutdown storage)

            (let [^LMDBLogStorage reopened (make-log-storage dir)]
              (try
                (let [reinitialized? (.init reopened (log-storage-options))]
                  (is reinitialized?)
                  (when reinitialized?
                    (is (= 2 (.getLastLogIndex reopened)))
                    (is (some? (.getEntry reopened 1)))
                    (is (some? (.getEntry reopened 2)))
                    (is (.appendEntry reopened
                                      (make-log-entry 3 entry-payload-bytes)))
                    (is (= 3 (.getLastLogIndex reopened)))
                    (is (some? (.getEntry reopened 3)))))
                (finally
                  (.shutdown reopened)))))))
      (finally
        (.shutdown storage)
        (u/delete-files dir)))))
