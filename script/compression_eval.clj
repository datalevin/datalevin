;; Run from the repository root:
;; clojure -J-Xmx512m -M:dev script/compression_eval.clj /tmp/compression-evaluation.json
(set! *unchecked-math* true)
(require '[clojure.java.io :as io]
         '[datalevin.compress :as cp]
         '[datalevin.hu :as hu]
         '[jsonista.core :as json])
(import '[java.io ByteArrayInputStream DataInputStream]
        '[java.nio ByteBuffer]
        '[java.nio.file Files]
        '[java.security MessageDigest]
        '[java.util ArrayList Arrays Collections HexFormat Random]
        '[java.util.zip GZIPInputStream])

(def evaluation-file "doc/compression-evaluation-2026-09-10.json")
(def previous (json/read-value (io/file evaluation-file) json/keyword-keys-object-mapper))
(def seeds [20260910 20260911 20260912])
(def training-sizes [4096 8192 16384 32768 65536 131072])

(defn sha256 [^bytes bytes]
  (.formatHex (HexFormat/of) (.digest (MessageDigest/getInstance "SHA-256") bytes)))

(defn read-corpus [label]
  (let [file (str "test/data/compression/" label ".bin.gz")
        bytes (with-open [in (GZIPInputStream. (io/input-stream file))]
                (.readAllBytes in))
        hash (sha256 bytes)
        records (ArrayList.)]
    (assert (= hash (get-in previous [:corpus_sha256 (keyword label)]))
            (str "Corpus checksum mismatch: " file))
    (with-open [in (DataInputStream. (ByteArrayInputStream. bytes))]
      (while (pos? (.available in))
        (let [n (.readInt in)]
          (assert (<= 0 n 511) "Invalid corpus record length")
          (let [raw (byte-array n)]
            (.readFully in raw)
            (.add records raw)))))
    (assert (= 163840 (.size records)))
    {:records records :file file :sha256 hash}))

(defn verify-records
  "Measure actual encoded bytes and round trips. Optionally check unsigned order.
  Throw on the first mismatch; an unsuccessful run never publishes new results."
  [ht records ordered?]
  (let [dst (ByteBuffer/allocate 1024)]
    (dissoc
      (reduce
        (fn [{:keys [previous-raw previous-encoded] :as result} ^bytes raw]
          (.clear dst)
          (hu/encode ht (ByteBuffer/wrap raw) dst)
          (let [size (.position dst)
                encoded (Arrays/copyOf (.array dst) size)
                out (ByteBuffer/allocate (alength raw))]
            (hu/decode ht (.flip dst) out)
            (assert (and (= (.position out) (alength raw))
                         (not (.hasRemaining dst))
                         (Arrays/equals raw (.array out))) "Roundtrip mismatch")
            (when (and ordered? previous-raw)
              (assert (= (Integer/signum (Arrays/compareUnsigned ^bytes previous-raw raw))
                         (Integer/signum (Arrays/compareUnsigned ^bytes previous-encoded encoded)))
                      "Unsigned ordering mismatch"))
            (-> result
                (update :roundtrip-tested inc)
                (update :ordering-tested + (if (and ordered? previous-raw) 1 0))
                (update :raw-bytes + (alength raw))
                (update :encoded-bytes + size)
                (update :max-encoded-bytes max size)
                (update :expanded-records + (if (> size (alength raw)) 1 0))
                (update :overflow-keys + (if (> size 511) 1 0))
                (assoc :previous-raw raw :previous-encoded encoded))))
        {:roundtrip-tested 0 :roundtrip-failed 0 :ordering-tested 0 :ordering-failed 0
         :raw-bytes 0 :encoded-bytes 0 :max-encoded-bytes 0
         :expanded-records 0 :overflow-keys 0}
        records)
      :previous-raw :previous-encoded)))

(defn evaluate-stream [label]
  (let [{:keys [records file sha256]} (read-corpus label)
        rows (atom [])
        full-check (atom nil)]
    (doseq [seed seeds]
      (let [shuffled (ArrayList. ^java.util.Collection records)]
        (Collections/shuffle shuffled (Random. seed))
        (doseq [n training-sizes]
          (let [freqs (cp/init-key-freqs)
                _ (doseq [raw (.subList shuffled 0 n)]
                    (#'cp/collect-keys freqs (ByteBuffer/wrap raw)))
                start (System/nanoTime)
                ht (hu/new-hu-tucker freqs)
                build-ms (/ (- (System/nanoTime) start) 1e6)
                result (verify-records ht (.subList shuffled 131072 163840) false)
                row (assoc result :corpus label :seed seed :train n :heldout 32768
                           :build-ms build-ms :min-code (apply min (.-lens ht))
                           :max-code (apply max (.-lens ht))
                           :encoded-ratio (/ (double (:encoded-bytes result))
                                             (:raw-bytes result)))]
            (swap! rows conj row)
            (println label "seed" seed "train" n "ratio" (:encoded-ratio row))
            (flush)
            (when (and (= seed 20260910) (= n 65536))
              (reset! full-check
                      (assoc (verify-records ht (sort #(Arrays/compareUnsigned ^bytes %1 ^bytes %2)
                                                     records) true)
                             :corpus label :seed seed :train n :scope "full corpus")))))))
    {:rows @rows :check @full-check :corpus {:file file :sha256 sha256 :records 163840}}))

(let [streams (mapv evaluate-stream ["ave-key" "eav-value"])
      historical (or (:historical_pre_fix previous)
                     (select-keys previous [:rows :roundtrip_checks :method :corpus_summary]))
      result (-> previous
                 (dissoc :corpus_summary)
                 (assoc :historical_pre_fix historical
                        :rows (vec (mapcat :rows streams))
                        :roundtrip_checks (mapv :check streams)
                        :corpora (mapv :corpus streams)
                        :method (merge (:method previous)
                                       {:date "2026-09-11" :alphabet_size hu/symbol-count
                                        :sampling_date "2026-09-10"
                                        :training_holdout_split_seeds seeds
                                        :nested_training_sizes training-sizes
                                        :code_builder "Fixed Hu-Tucker terminal alphabet and LeftistHeap"
                                        :encoded_byte_model "Actual encode/decode of every holdout record; ordered full-corpus verification with 64K-trained dictionaries"
                                        :jvm_heap_limit "512 MiB"
                                        :limits "Encoded bytes only; not page savings or database throughput. Splits reuse one corpus. Full-corpus checks include training records."
                                        :reproduce "clojure -J-Xmx512m -M:dev script/compression_eval.clj /tmp/compression-evaluation.json"
                                        :java (System/getProperty "java.version")
                                        :architecture (System/getProperty "os.arch")})
                        :source_sha256 (into {}
                                             (for [file ["src/datalevin/hu.clj"
                                                         "src/datalevin/compress.clj"
                                                         "src/java/datalevin/utl/LeftistHeap.java"
                                                         "script/compression_eval.clj"]]
                                               [file (sha256 (Files/readAllBytes (.toPath (io/file file))))]))))
      target (or (first *command-line-args*) "/tmp/compression-evaluation.json")]
  (json/write-value (io/file target) result (json/object-mapper {:pretty true}))
  (println "Verified" (reduce + (map #(get-in % [:check :roundtrip-tested]) streams))
           "full-corpus round trips; results:" target))
(shutdown-agents)
