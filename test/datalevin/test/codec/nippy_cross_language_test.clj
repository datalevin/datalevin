;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.codec.nippy-cross-language-test
  (:require
   [clojure.java.io :as io]
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators :as gen]
   [clojure.test.check.properties :as prop]
   [taoensso.nippy :as nippy]
   [taoensso.nippy.compression :as compression]
   [datalevin.test.codec.nippy-support :as support]
   [datalevin.interpret :as interpret]
   [datalevin.core :as d]
   [datalevin.util :as u]
   [datalevin.test-adapter :as adapter]
   [datalevin.test-adapter.rust :as rust-adapter])
  (:import
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
                (str "Cannot find Cargo for the Nippy cross-language tests. "
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
                  (.getPath manifest) "--example" "nippy_test_peer"]
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
      (throw (ex-info "Rust Nippy test peer failed to start"
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
          (throw (ex-info "Rust Nippy test peer terminated"
                          {:operation operation
                           :exit-code (when-not (.isAlive ^Process process)
                                        (.exitValue ^Process process))}))))))

(defn- request ^bytes [operation payload]
  (let [response (rust-response operation payload)
        [status hex] (str/split response #"\t" 2)]
    (when-not (= status "ok")
      (throw (ex-info "Rust Nippy codec rejected the request"
                      {:operation operation :response response})))
    (hex->bytes hex)))

(defn- roundtrip [value]
  (nippy/fast-thaw (request "roundtrip" (bytes->hex (nippy/fast-freeze value)))))

(deftest upstream-stress-and-datalevin-types
  (doseq [[id value] (support/fixtures)]
    (testing id
      (let [actual (roundtrip value)]
        (is (support/value= value actual))
        (is (= (meta value) (meta actual)))))))

(deftest rust-authored-values
  (doseq [[id expected] (support/native-fixtures)]
    (testing id
      (let [actual (nippy/fast-thaw (request "fixture" id))]
        (is (support/value= expected actual))
        (is (= (meta expected) (meta actual)))))))

(deftest compression-both-directions
  (let [value (vec (repeat 128 {:name "Ada" :values (vec (range 32))}))]
    (doseq [[id compressor] [["none" nil] ["lz4" compression/lz4-compressor]
                             ["snappy" (var-get (ns-resolve 'taoensso.nippy.compression 'snappy-compressor))]
                             ["zstd" compression/zstd-compressor]
                             ["lzma2" compression/lzma2-compressor]]]
      (testing id
        (is (= value (nippy/fast-thaw
                      (request "thaw" (bytes->hex (nippy/freeze value {:compressor compressor}))))))
        (is (= value (nippy/thaw
                      (request "freeze" (str id ":" (bytes->hex (nippy/fast-freeze value)))))))))))

(deftest cached-values-and-nested-closures
  (let [value (vec (repeat 140 (nippy/cache [(nippy/cache "repeat") :k])))
        expected (vec (repeat 140 ["repeat" :k]))]
    (is (= expected (roundtrip value))))
  (let [offset 7
        inner (interpret/inter-fn [x] (+ x offset))
        outer (interpret/inter-fn [x] (inner (* 2 x)))
        decoded (roundtrip outer)]
    (is (= 17 (decoded 5)))))

(deftest integer-compression-and-roaring-containers
  (doseq [n [0 1 3 4 31 32 33 127 128 129 255 256 257]
          width (range 33)]
    (let [mask (dec (bit-shift-left 1 width))
          values (map #(unchecked-int (bit-and mask (* % 123456789))) (range n))
          value (support/growing values)
          wire  (nippy/fast-freeze value)
          rust  (request "roundtrip" (bytes->hex wire))]
      (is (Arrays/equals ^bytes wire ^bytes rust)
          (str "exact JavaFastPFOR bytes: length " n ", width " width))
      (is (= value (nippy/fast-thaw rust)) (str "length " n ", width " width))))
  (doseq [run? [false true]]
    (let [value (support/bitmap (concat (range 100000) [4294967295]))]
      (when run? (.runOptimize ^org.roaringbitmap.RoaringBitmap value))
      (is (= value (roundtrip value))))))

(deftest entity-descriptors-and-touched-cache
  (let [dir (u/tmp-dir (str "nippy-entity-" (java.util.UUID/randomUUID)))
        db (-> (d/empty-db dir) (d/db-with [{:db/id 1 :name "Ada"}]))]
    (try
      (let [entity (d/entity db 1)
            decoded (roundtrip entity)]
        (is (= 1 (:db/id decoded)))
        (is (= "Ada" (:name decoded)))
        (d/touch entity)
        (is (= (into {} entity) (into {} (roundtrip entity)))))
      (finally (d/close-db db) (u/delete-files dir)))))

(deftest shared-dictionary-390
  (when-let [shared-dict (ns-resolve 'taoensso.nippy 'shared-dict)]
    (let [dict (shared-dict [:name "country" "currency"])
          value [:name "country" "currency" :name]
          bytes (nippy/freeze value {:compressor nil :shared-dict dict})
          bare (Arrays/copyOfRange bytes 4 (alength bytes))]
      (is (= value (nippy/fast-thaw (request "dictionary" (bytes->hex bare))))))
    (is (str/starts-with? (rust-response "roundtrip" "7c000100000000000000003b") "error\t"))))

(def scalar-gen
  (gen/one-of [gen/boolean gen/large-integer gen/string-alphanumeric
               (gen/fmap keyword gen/string-alphanumeric) (gen/return nil)]))

(def value-gen
  (gen/recursive-gen
   (fn [inner]
     ;; Respect recursive-gen's size budget at each level. Explicit collection
     ;; lengths bypass it and let nested values and shrink trees grow exponentially.
     (gen/scale #(min 16 (long %))
                (gen/one-of [(gen/vector inner)
                            (gen/list inner)
                            (gen/set inner)
                            (gen/map scalar-gen inner)])))
   scalar-gen))

(deftest generated-values-respect-small-sizes
  (doseq [size [0 1]
          seed (range 32)]
    (let [value (gen/generate value-gen size seed)]
      (is (or (not (coll? value)) (<= (count value) (long size)))
          (str "size " size ", seed " seed)))))

(defspec generated-jvm-rust-jvm 300
  (prop/for-all [value value-gen]
    (support/value= value (roundtrip value))))

(deftest rust-test-adapter-transport
  (let [command [(cargo-executable) "run" "--quiet" "--manifest-path"
                 (str (io/file (repository-root) "src/rust/test-adapter/Cargo.toml"))]
        peer (rust-adapter/start command)]
    (try
      (let [value {:query '[:find ?e :where [?e :name "Ada"]]
                   :rows #{[1 "Ada"]} :bytes (byte-array [0 -1])}]
        (is (support/value= value (adapter/invoke! peer :echo [value]))))
      (finally (adapter/stop! peer)))))
