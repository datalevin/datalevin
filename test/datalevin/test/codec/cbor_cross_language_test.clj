;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.codec.cbor-cross-language-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [datalevin.codec.cbor :as cbor]
   [datalevin.test.codec.cbor-test-support :as support])
  (:import
   [datalevin.codec DLCbor$CodecException DLCbor$ErrorCode]
   [java.io BufferedReader BufferedWriter File InputStreamReader OutputStreamWriter]
   [java.lang ProcessBuilder ProcessBuilder$Redirect]
   [java.nio.charset StandardCharsets]
   [java.util Arrays]
   [java.util.concurrent TimeUnit]))

(defrecord ^:private RustPeer [^Process process
                               ^BufferedReader reader
                               ^BufferedWriter writer])

(def ^:dynamic *rust-peer* nil)

(defn- cargo-executable
  "Find Cargo on PATH or in the Rust installation, with a CARGO path override."
  ^String []
  (let [executable (if (= File/separator "\\") "cargo.exe" "cargo")
        override   (not-empty (System/getenv "CARGO"))
        path       (not-empty (System/getenv "PATH"))
        cargo-home (or (not-empty (System/getenv "CARGO_HOME"))
                       (io/file (System/getProperty "user.home") ".cargo"))
        candidates (if override
                     [(io/file override)]
                     (concat
                      (when path
                        (map #(io/file % executable)
                             (str/split path (re-pattern File/pathSeparator))))
                      [(io/file cargo-home "bin" executable)]))]
    (or (some (fn [^File candidate]
                (when (and (.isFile candidate) (.canExecute candidate))
                  ;; Keep the cargo filename: rustup may dispatch by symlink name.
                  (.getAbsolutePath candidate)))
              candidates)
        (throw (ex-info
                (str "Cannot find Cargo for the DL-CBOR cross-language tests. "
                     "Install Rust, add Cargo to PATH, or set CARGO to its "
                     "executable path.")
                {:searched (mapv str candidates)})))))

(defn- repository-root []
  (let [root     (.getCanonicalFile
                  (io/file (System/getProperty "user.dir")))
        manifest (io/file root "src/rust/Cargo.toml")]
    (when-not (.isFile manifest)
      (throw (ex-info "Cannot locate the Rust codec manifest"
                      {:working-directory (.getPath root)
                       :expected          (.getPath manifest)})))
    root))

(defn- start-rust-peer []
  (let [root     (repository-root)
        manifest (io/file root "src/rust/Cargo.toml")
        command  [(cargo-executable) "run" "--quiet" "--manifest-path"
                  (.getPath manifest) "--example" "dl_cbor_test_peer"]
        builder  (doto (ProcessBuilder. ^java.util.List command)
                   (.directory root)
                   (.redirectError ProcessBuilder$Redirect/INHERIT))
        process  (.start builder)
        reader   (BufferedReader.
                  (InputStreamReader. (.getInputStream process)
                                      StandardCharsets/UTF_8))
        writer   (BufferedWriter.
                  (OutputStreamWriter. (.getOutputStream process)
                                       StandardCharsets/UTF_8))
        ready    (.readLine reader)]
    (when-not (= "ready" ready)
      (.destroyForcibly process)
      (throw (ex-info "Rust DL-CBOR test peer failed to start"
                      {:command command :response ready})))
    (->RustPeer process reader writer)))

(defn- stop-rust-peer [{:keys [process reader writer]}]
  (try
    (.write ^BufferedWriter writer "quit\n")
    (.flush ^BufferedWriter writer)
    (catch Exception _exception))
  (try
    (.close ^BufferedWriter writer)
    (catch Exception _exception))
  (try
    (.close ^BufferedReader reader)
    (catch Exception _exception))
  (when-not (.waitFor ^Process process 5 TimeUnit/SECONDS)
    (.destroy ^Process process)
    (when-not (.waitFor ^Process process 1 TimeUnit/SECONDS)
      (.destroyForcibly ^Process process))))

(defn- with-rust-peer [tests]
  (let [peer (start-rust-peer)]
    (try
      (binding [*rust-peer* peer]
        (tests))
      (finally
        (stop-rust-peer peer)))))

(use-fixtures :once with-rust-peer)

(def ^:private hex-digits "0123456789abcdef")

(defn- bytes->hex ^String [^bytes value]
  (let [builder (StringBuilder. (* 2 (alength value)))]
    (dotimes [index (alength value)]
      (let [unsigned (bit-and (aget value index) 0xff)]
        (.append builder (.charAt ^String hex-digits
                                  (bit-shift-right unsigned 4)))
        (.append builder (.charAt ^String hex-digits
                                  (bit-and unsigned 0x0f)))))
    (.toString builder)))

(defn- hex->bytes ^bytes [^String value]
  (when (odd? (.length value))
    (throw (ex-info "Rust peer returned odd-length hex" {:hex value})))
  (let [result (byte-array (quot (.length value) 2))]
    (dotimes [index (alength result)]
      (let [offset (* 2 index)
            high   (Character/digit (.charAt value offset) 16)
            low    (Character/digit (.charAt value (inc offset)) 16)]
        (when (or (= -1 high) (= -1 low))
          (throw (ex-info "Rust peer returned invalid hex" {:hex value})))
        (aset-byte result index
                   (unchecked-byte (bit-or (bit-shift-left high 4) low)))))
    result))

(defn- rust-response ^String [operation ^String payload]
  (let [{:keys [process reader writer] :as peer} *rust-peer*]
    (when-not peer
      (throw (IllegalStateException. "Rust test peer is not running")))
    (locking peer
      (.write ^BufferedWriter writer ^String operation)
      (.write ^BufferedWriter writer "\t")
      (.write ^BufferedWriter writer payload)
      (.write ^BufferedWriter writer "\n")
      (.flush ^BufferedWriter writer)
      (or (.readLine ^BufferedReader reader)
          (throw (ex-info "Rust DL-CBOR test peer terminated"
                          {:operation operation
                           :exit-code (when-not (.isAlive ^Process process)
                                        (.exitValue ^Process process))}))))))

(defn- rust-request ^bytes [operation ^String payload]
  (let [response (rust-response operation payload)]
    (if (.startsWith ^String response "ok\t")
      (hex->bytes (.substring ^String response 3))
      (throw (ex-info "Rust DL-CBOR test peer rejected a request"
                      {:operation operation :response response})))))

(defn- rust-error [operation ^String hex]
  (let [response (rust-response operation hex)
        [status code offset & details] (str/split response #"\t" -1)]
    (when (or (not= "error" status) (seq details))
      (throw (ex-info "Rust DL-CBOR peer did not return a codec error"
                      {:operation operation :response response})))
    {:code code :offset (Long/parseLong offset)}))

(defn- jvm-error [operation ^bytes input]
  (try
    (support/decode-malformed operation input)
    (throw (ex-info "JVM accepted malformed DL-CBOR"
                    {:operation operation :hex (bytes->hex input)}))
    (catch DLCbor$CodecException exception
      {:code   (.name ^DLCbor$ErrorCode (.code exception))
       :offset (long (.offset exception))})))

(defn- rust-roundtrip ^bytes [operation ^bytes encoded]
  (rust-request operation (bytes->hex encoded)))

(defn- rust-generated ^bytes [operation ^long seed]
  (rust-request operation (Long/toUnsignedString seed 16)))

(def ^:private int32-gen
  (gen/choose Integer/MIN_VALUE Integer/MAX_VALUE))

(def ^:private seed-gen
  (gen/fmap
   (fn [[high low]]
     (bit-or (bit-shift-left (long high) 32)
             (bit-and (long low) 0xffffffff)))
   (gen/tuple int32-gen int32-gen)))

(deftest malformed-jvm-rust-error-agreement-test
  (let [rows (support/malformed-rows)]
    (is (= 68 (count rows)))
    (doseq [[id operation hex expected _note] rows]
      (testing id
        (let [jvm-error  (jvm-error operation (hex->bytes hex))
              rust-error (rust-error operation hex)]
          (is (= expected (:code jvm-error)))
          (is (= jvm-error rust-error)))))))

(deftest draft-extension-malformed-jvm-rust-error-agreement-test
  (let [rows (support/draft-extension-malformed-rows)]
    (is (= 64 (count rows)))
    (doseq [[id operation hex expected _note] rows]
      (testing id
        (let [jvm-error  (jvm-error operation (hex->bytes hex))
              rust-error (rust-error operation hex)]
          (is (= expected (:code jvm-error)))
          (is (= jvm-error rust-error)))))))

(defspec canonical-jvm-rust-jvm-roundtrip-property 500
  (prop/for-all [value support/value-gen]
    (let [jvm-encoded  (cbor/encode value)
          rust-encoded (rust-roundtrip "canonical" jvm-encoded)
          jvm-decoded  (cbor/decode rust-encoded)
          jvm-again    (cbor/encode jvm-decoded)
          rust-again   (rust-roundtrip "canonical" jvm-again)]
      (and (Arrays/equals jvm-encoded rust-encoded)
           (Arrays/equals rust-encoded jvm-again)
           (Arrays/equals jvm-again rust-again)
           (support/value= value jvm-decoded)))))

(defspec fast-jvm-rust-jvm-roundtrip-property 400
  (prop/for-all [value support/value-gen]
    (let [canonical    (cbor/encode value)
          jvm-fast     (cbor/encode value cbor/fast)
          rust-encoded (rust-roundtrip "fast" jvm-fast)
          jvm-decoded  (cbor/decode rust-encoded)]
      (and (Arrays/equals canonical rust-encoded)
           (Arrays/equals canonical (cbor/encode jvm-decoded))
           (support/value= value jvm-decoded)))))

(defspec storage-jvm-rust-jvm-roundtrip-property 400
  (prop/for-all [value support/value-gen]
    (let [jvm-encoded  (cbor/encode-storage value)
          rust-encoded (rust-roundtrip "storage" jvm-encoded)
          jvm-decoded  (cbor/decode-storage rust-encoded)]
      (and (Arrays/equals jvm-encoded rust-encoded)
           (Arrays/equals jvm-encoded (cbor/encode-storage jvm-decoded))
           (support/value= value jvm-decoded)))))

(defspec generated-rust-jvm-rust-roundtrip-property 500
  (prop/for-all [seed seed-gen]
    (let [rust-encoded  (rust-generated "generate" seed)
          jvm-decoded   (cbor/decode rust-encoded)
          jvm-encoded   (cbor/encode jvm-decoded)
          rust-again    (rust-roundtrip "canonical" jvm-encoded)
          jvm-storage   (cbor/encode-storage jvm-decoded)
          rust-storage  (rust-roundtrip "storage" jvm-storage)]
      (and (Arrays/equals rust-encoded jvm-encoded)
           (Arrays/equals rust-encoded rust-again)
           (Arrays/equals jvm-storage rust-storage)))))

(defspec fast-rust-jvm-rust-roundtrip-property 400
  (prop/for-all [seed seed-gen]
    (let [rust-canonical (rust-generated "generate" seed)
          rust-fast      (rust-generated "generate-fast" seed)
          jvm-decoded    (cbor/decode rust-fast false)
          jvm-canonical  (cbor/encode jvm-decoded)
          rust-again     (rust-roundtrip "canonical" jvm-canonical)]
      (and (Arrays/equals rust-canonical jvm-canonical)
           (Arrays/equals rust-canonical rust-again)))))
