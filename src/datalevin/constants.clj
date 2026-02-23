;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.constants
  "System vars. Some can be dynamically rebound to change system behavior."
  (:refer-clojure :exclude [meta])
  (:require
   [clojure.string :as s]
   [datalevin.util :as u])
  (:import
   [java.io File]
   [java.util UUID Arrays HashSet]
   [java.math BigInteger BigDecimal]))

(def version
  "Version number of Datalevin"
  "0.10.5")

(def version-file-name
  "Name of the file that stores version on disk"
  "VERSION")

(def version-regex #"^(\d+)(?:\.(\d+))?(?:\.(\d+))?$")

(defn parse-version
  [s]
  (when-let [[_ major minor patch] (re-matches version-regex (s/trim s))]
    {:major (parse-long major)
     :minor (some-> minor parse-long)
     :patch (some-> patch parse-long)}))

(def require-migration?
  "if this version of Datalevin calls for migrating db"
  true)

(def rule-unbound-pattern-penalty
  "Multiplier for the cost of unbound EAV scans during rule evaluation."
  3)

(def rule-delta-index-threshold
  "Maximum delta size for using index probes in recursive rules.
   When deltas are larger than this, hash joins are preferred."
  100)

(def magic-explosion-factor
  "Factor by which magic seed can grow before falling back to non-magic.
   If total magic rule tuples exceed initial-seed-size * this factor,
   magic is considered ineffective and evaluation restarts without it."
  10)

(def data-file-name
  "Name of the file that stores data on disk"
  "data.mdb")

(def keycode-file-name
  "Name of the file that stores key compression codes on disk"
  "keycode.bin")

(def valcode-file-name
  "Name of the file that stores value compression codes on disk"
  "valcode.bin")

;;---------------------------------------------
;; system constants, fixed
;;---------------------------------------------

;; datom

(def ^:no-doc ^:const e0    0)
(def ^:no-doc ^:const emax  0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const tx0   1)
(def ^:no-doc ^:const txmax 0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const g0    1)
(def ^:no-doc ^:const gmax  0x7FFFFFFFFFFFFFFF)
(def ^:no-doc ^:const a0    0)
(def ^:no-doc ^:const amax  0x7FFFFFFF)
(def ^:no-doc v0    :db.value/sysMin)
(def ^:no-doc vmax  :db.value/sysMax)

;; schema

(def ^:no-doc implicit-schema
  {:db/ident      {:db/unique    :db.unique/identity
                   :db/valueType :db.type/keyword
                   :db/aid       0}
   :db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         1}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one
                   :db/aid         2}})

(def ^:no-doc entity-time-schema
  {:db/created-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}
   :db/updated-at {:db/valueType   :db.type/long
                   :db/cardinality :db.cardinality/one}})

;; lmdb

(def default-env-flags
  "Default LMDB env flag is `#{:nordahead}`. See
  [[datalevin.core/set-env-flags]] for a full list of flags.

  Passed as `:flags` option value to `open-kv` function."
  #{:nordahead})

(def default-dbi-flags
  "Default DBI flags is `#{:create :counted :prefix-compression}`. See http://www.lmdb.tech/doc/group__mdb__dbi__open.html for a list of flags for stock LMDB, and https://github.com/huahaiy/dlmdb for additional flags."
  #{:create :counted :prefix-compression})

(def default-put-flags
  "Default LMDB put flag is `#{}`. See http://www.lmdb.tech/doc/group__mdb__put.html for full list of flags"
  #{})

(def ^:const +max-key-size+
  "Maximum LMDB key size is 511 bytes"
  511)

(def ^:no-doc ^:const +buffer-grow-factor+ 10)

;; # of times db can be auto enlarged in a tx
(def ^:no-doc ^:const +in-tx-overflow-times+ 5)

;; tmp lmdb

(def ^:const default-spill-threshold
  "Default percentage of heap memory (Xmx) is 95, over which spill-to-disk
  will be triggered"
  95)

(def default-spill-root
  "Default root directory of spilled files is platform dependent. the same as
  the value of Java property `java.io.tmpdir`"
  (u/tmp-dir))

(def ^:const tmp-dbi
  "Default dbi name of the spilled db is `t`"
  "t")

;; index storage

(def ^:no-doc ^:const +val-bytes-wo-hdr+ 497)  ; - hdr - g - a - g?
(def ^:no-doc ^:const +val-bytes-trunc+  496)  ; - tr

(def ^:no-doc ^:const +id-bytes+ Long/BYTES)
(def ^:no-doc ^:const +short-id-bytes+ Integer/BYTES)

;; value headers
(def ^:no-doc ^:const type-long-neg   (unchecked-byte 0xC0))
(def ^:no-doc ^:const type-long-pos   (unchecked-byte 0xC1))
(def ^:no-doc ^:const type-bigint     (unchecked-byte 0xF1))
(def ^:no-doc ^:const type-bigdec     (unchecked-byte 0xF2))
(def ^:no-doc ^:const type-homo-tuple (unchecked-byte 0xF3))
(def ^:no-doc ^:const type-hete-tuple (unchecked-byte 0xF4))
(def ^:no-doc ^:const type-float      (unchecked-byte 0xF5))
(def ^:no-doc ^:const type-double     (unchecked-byte 0xF6))
(def ^:no-doc ^:const type-instant    (unchecked-byte 0xF7))
(def ^:no-doc ^:const type-ref        (unchecked-byte 0xF8))
(def ^:no-doc ^:const type-uuid       (unchecked-byte 0xF9))
(def ^:no-doc ^:const type-string     (unchecked-byte 0xFA))
(def ^:no-doc ^:const type-keyword    (unchecked-byte 0xFB))
(def ^:no-doc ^:const type-symbol     (unchecked-byte 0xFC))
(def ^:no-doc ^:const type-boolean    (unchecked-byte 0xFD))
(def ^:no-doc ^:const type-bytes      (unchecked-byte 0xFE))

(def ^:no-doc ^:const false-value     (unchecked-byte 0x01))
(def ^:no-doc ^:const true-value      (unchecked-byte 0x02))

(def ^:no-doc ^:const separator       (unchecked-byte 0x00))
(def ^:no-doc ^:const truncator       (unchecked-byte 0xFF))
(def ^:no-doc ^:const slash           (unchecked-byte 0x2F))

(def ^:no-doc separator-ba (byte-array [(unchecked-byte 0x00)]))

(def ^:no-doc max-uuid (UUID. -1 -1))
(def ^:no-doc min-uuid (UUID. 0 0))

(def ^:no-doc max-bytes (let [ba (byte-array +val-bytes-wo-hdr+)]
                          (Arrays/fill ba (unchecked-byte 0xFF))
                          ba))
(def ^:no-doc min-bytes (byte-array [0x00]))

(def ^:const +tuple-max+
  "Maximum length of a tuple is 255 bytes"
  255)

(def ^:no-doc tuple-max-bytes (let [ba (byte-array +tuple-max+)]
                                (Arrays/fill ba (unchecked-byte 0xFF))
                                ba))

(defn- max-bigint-bs
  ^bytes []
  (let [^bytes bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x7f))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0xff)))
    bs))

(def max-bigint
  "Maximum big integer is `2^1015-1`"
  (BigInteger. (max-bigint-bs)))

(defn- min-bigint-bs
  ^bytes []
  (let [bs (byte-array 127)]
    (aset bs 0 (unchecked-byte 0x80))
    (dotimes [i 126] (aset bs (inc i) (unchecked-byte 0x00)))
    bs))

(def min-bigint
  "Minimum big integer is `-2^1015`"
  (BigInteger. (min-bigint-bs)))

(def max-bigdec
  "Maximum big decimal has value of `(2^1015-1) x 10^2147483648`"
  (BigDecimal. ^BigInteger max-bigint Integer/MIN_VALUE))

(def min-bigdec
  "Minimum big decimal has value of `-2^1015 x 10^2147483648`"
  (BigDecimal. ^BigInteger min-bigint Integer/MIN_VALUE))

(def ^:no-doc ^:const normal 0)  ; non-giant datom

;; dbi-names

;; kv
(def ^:const kv-info
  "dbi name for kv store system information is `datalevin/kv-info`"
  "datalevin/kv-info")

;; dl
(def ^:const eav
  "dbi name for Datalog EAV index is `datalevin/eav`"
  "datalevin/eav")
(def ^:const ave
  "dbi name for Datalog AVE index is `datalevin/ave`"
  "datalevin/ave")
(def ^:const giants
  "dbi name for Datalog large datoms is `datalevin/giants`"
  "datalevin/giants")
(def ^:const schema
  "dbi name for Datalog schema is `datalevin/schema`"
  "datalevin/schema")
(def ^:const meta
  "dbi name for Datalog runtime meta information is `datalevin/meta`"
  "datalevin/meta")
(def ^:const opts
  "dbi name for Datalog options is `datalevin/opts`"
  "datalevin/opts")

;; compression

(def ^:no-doc ^:const +key-compress-num-symbols+ 65536)

(def ^:no-doc ^:const +value-compress-dict-size+ 32) ;; in KB


;; search

(def ^:const default-domain "datalevin")

(def default-display
  "default `search` function `:display` option value is `:refs`"
  :refs)

(def ^:const default-top
  "default `search` function `:top` option value is `10`"
  10)

(def ^:const default-proximity-expansion
  "default `search` function `:proximity-expansion` option value is `2`"
  2)

(def ^:const default-proximity-max-dist
  "default `search` function `:proximity-max-dist` option value is `45`"
  45)

(def default-doc-filter (constantly true))

(def ^:const terms
  "dbi name suffix for search engine terms index is `terms`"
  "terms")

(def ^:const docs
  "dbi name suffix for search engine documents index is `docs`"
  "docs")

(def ^:const positions
  "dbi name suffix for search engine positions index is `positions`"
  "positions")

(def ^:const rawtext
  "dbi name suffix for search engine raw text is `rawtext`"
  "rawtext")

(def ^:const vec-refs
  "dbi name suffix for vec-ref -> vec-id map is `vec-refs`"
  "vec-refs")

;; idoc

(def ^:const idoc-doc-ref
  "dbi name suffix for idoc doc-ref map is `doc-ref`"
  "doc-ref")

(def ^:const idoc-doc-index
  "dbi name suffix for idoc inverted index is `doc-index`"
  "doc-index")

(def ^:const idoc-path-dict
  "dbi name suffix for idoc path dictionary is `path-dict`"
  "path-dict")

(def ^:const +max-term-length+
  "The full text search engine ignores exceedingly long strings. The maximal
  allowed term length is 128 characters"
  128)

;; data types

(def ^:no-doc datalog-value-types
  #{:db.type/keyword :db.type/symbol :db.type/string :db.type/boolean
    :db.type/long :db.type/double :db.type/float :db.type/ref
    :db.type/bigint :db.type/bigdec :db.type/instant :db.type/uuid
    :db.type/bytes :db.type/tuple :db.type/vec :db.type/idoc})

(def ^:no-doc kv-value-types
  #{:keyword :symbol :string :boolean :long :double :float :instant :uuid
    :bytes :bigint :bigdec :data :id})

;; server / client

(def ^:no-doc ^:const +buffer-size+ 65536)

(def ^:no-doc ^:const +wire-datom-batch-size+ 1000)

(def ^:const default-port
  "The server default port number is 8898"
  (int 8898))

(def ^:const default-idle-timeout
  "The server session default idle timeout is 172800000ms (48 hours)"
  172800000)

(def ^:const default-connection-pool-size
  "The default client connection pool size is 3"
  3)

(def ^:const default-connection-timeout
  "The default connection timeout is 60000ms (1 minute)"
  60000)

(def ^:no-doc ^:const system-dir "system")

(def ^:const default-username
  "The server default username is `datalevin`"
  "datalevin")

(def ^:const default-password
  "The server default password is `datalevin`"
  "datalevin")

(def ^:no-doc ^:const db-store-datalog "datalog")
(def ^:no-doc ^:const db-store-kv "kv")

(def ^:no-doc dl-type :datalog)
(def ^:no-doc kv-type :key-value)

(def ^:no-doc ^:const message-header-size 5) ; bytes, 1 type + 4 length

(def ^:no-doc ^:const message-format-transit (unchecked-byte 0x01))
(def ^:no-doc ^:const message-format-nippy (unchecked-byte 0x02))

(def ^:const vector-index-suffix
  "File name suffix for vector index is `.vid`"
  ".vid")

(def ^:const default-metric-type
  "Default vector index metric type is :euclidean"
  :euclidean)

(def ^:const default-quantization
  "Default vector index quantization is :float"
  :float)

(def ^:const default-connectivity
  "Default vector index connectivity is 16"
  16)

(def ^:const default-expansion-add
  "Default vector index expansion-add is 128"
  128)

(def ^:const default-expansion-search
  "Default vector index expansion-search is 64"
  64)

(def ^:const default-multi?
  "Default vector index multi? flag is false"
  false)

;;-------------------------------------------------------------
;; user configurable
;;-------------------------------------------------------------

;; serialization

(def ^{:dynamic true
       :doc     "set of additional serializable classes, e.g.
                  `#{\"my.package.*\"}`"}
  *data-serializable-classes* #{})

;; lmdb

(def ^{:dynamic true
       :doc     "Maximum number of sub-databases allowed in a db file"}
  *max-dbs* 128)

(def ^{:dynamic true
       :doc     "Default number of concurrent readers allowed for a DB file is 1024. Can be set as `:max-readers` option when opening the DB."}
  *max-readers* 1024)

(def ^{:dynamic true
       :doc     "Initial db file size is 1000 MiB, automatically grown"}
  *init-db-size* 1000)

(def ^{:dynamic true
       :doc     "Initial maximal value size is 16384 bytes, automatically grown"}
  *init-val-size* 16384)

(defn ^:no-doc pick-mapsize
  "pick a map size from the growing factor schedule that is larger than or
  equal to the current size"
  [^File db-file]
  (let [cur-size (.length db-file)]
    (some #(when (<= cur-size (* ^long % 1024 1024)) %)
          (iterate #(* ^long +buffer-grow-factor+ ^long %)
                   *init-db-size*))))

(def ^{:dynamic true
       :doc     "The number of samples considered when build the key compression dictionary is 65536"}
  *compress-sample-size* 65536)

(def ^{:dynamic true
       :doc     "Time interval between automatic LMDB sync to disk, in seconds, default is 300"}
  lmdb-sync-interval 300)

;; datalog db

(def ^{:dynamic true :no-doc true
       :doc     "When true, use the prepare/apply transaction path"}
  *use-prepare-path* false)

(def ^{:dynamic true
       :doc     "Batch size (# of datoms) when filling Datalog DB"}
  *fill-db-batch-size* 1048576)

(def ^{:dynamic true
       :doc     "Datalog DB starts background sampling or not"}
  *db-background-sampling?* true)

;; datalog query engine

(def ^{:dynamic true
       :doc     "Limit of the number of items hold in global query result cache"}
  query-result-cache-size 1024)

(def ^{:dynamic true
       :doc     "Limit of the number of items hold in global query plan cache"}
  query-plan-cache-size 1024)

(def ^{:dynamic true
       :doc     "Size below which the initial plan will execute during planning,
above which, the same number of items will be sampled instead"}
  init-exec-size-threshold 500)

(def ^{:dynamic true
       :doc     "Upper bound on the plan search space. When the number of plans
considered exceeds this cap, the planner switches from exhaustive to greedy. Default
is Integer/MAX_VALUE, i.e. no cap."}
  plan-search-max Integer/MAX_VALUE)

(def ^{:dynamic true
       :doc     "Default ratio for merge scan size change estimate"}
  magic-scan-ratio (double (/ 1 ^long init-exec-size-threshold)))

(def ^{:dynamic true
       :doc     "Default ratio for link size change estimate"}
  magic-link-ratio 1.0)

(def ^{:dynamic true
       :doc     "Prior sample size used when blending sample link ratios with
the default ratio to reduce skew from tiny samples"}
  link-estimate-prior-size 100)

(def ^{:dynamic true
       :doc     "Variance inflation factor for link ratio prior size. Higher
values put more weight on the default ratio when sample CV^2 is large."}
  link-estimate-var-alpha 0.4)

(def ^{:dynamic true
       :doc     "Multiplier over mean for link ratio upper bound. Higher
values means trust sampled high ratio more."}
  link-estimate-max-multi 1.0)

(def ^{:dynamic true
       :doc     "Default expansion ratio for or-join size estimate"}
  magic-or-join-ratio 10.0)

(def ^{:dynamic true
       :doc     "Cost associated with running a predicate during scan"}
  magic-cost-pred 3.5)

(def ^{:dynamic true
       :doc     "Cost associated with adding a variable during scan"}
  magic-cost-var 5.5)

(def ^{:dynamic true
       :doc     "Cost associated with running a filter during scan"}
  magic-cost-fidx 1.4)

(def ^{:dynamic true
       :doc     "Cost associated with scanning e based on a"}
  magic-cost-init-scan-e 2.0)

(def ^{:dynamic true
       :doc     "Cost associated with merge-scan join"}
  magic-cost-merge-scan-v 5.5)

(def ^{:dynamic true
       :doc     "Cost per index probe in indexed nested loop join"}
  magic-cost-link-probe 2.5)

(def ^{:dynamic true
       :doc     "Cost per tuple retrieved from index in indexed nested loop join"}
  magic-cost-link-retrieval 1.2)

(def ^{:dynamic true
       :doc     "Cost associated with hash join"}
  magic-cost-hash-join (* 5.0
                          ;; for hash join is a barrier to parallelism
                          (.availableProcessors (Runtime/getRuntime))))

(def ^{:dynamic true
       :doc     "Minimum input size before considering hash join"}
  hash-join-min-input-size 20000)

(def ^{:dynamic true
       :doc     "Maximum number of single-value ranges for SIP optimization in hash join. When the input cardinality is at or below this threshold, entity IDs are converted to individual ranges instead of using a bitmap predicate."}
  sip-range-threshold 1000)

(def ^{:dynamic true
       :doc     "Ratio threshold for SIP optimization. SIP is applied when target size > input size * this ratio."}
  sip-ratio-threshold 5)

(def ^{:dynamic true
       :doc     "Time interval between sample processing, in seconds "}
  sample-processing-interval 10)

(def ^{:dynamic true
       :doc     "Change ratio of an attribute's values, beyond which re-sampling will be done"}
  sample-change-ratio 0.05)


(def ^{:dynamic true
       :doc     "Max milliseconds to wait for a tuple before failing. nil to wait forever."}
  query-pipe-timeout 3000000)

(def ^{:dynamic true
       :doc     "Maximum queue size for a tuple pipe. Producers block when full, providing back-pressure."}
  query-pipe-capacity 10000000)

(def ^{:dynamic true
       :doc     "Batch size for pipelined scans. Tuples are buffered, sorted, then scanned in batch for sequential seeks. Set to 0 to disable batching."}
  query-pipe-batch-size 12384)


;; search engine

(def ^{:dynamic true
       :doc     "The set of English stop words, a Java HashSet, contains
the same words as that of Lucene. Used in English analyzer."}
  *en-stop-words-set*
  (let [s (HashSet.)]
    (doseq [w ["a",    "an",   "and",   "are",  "as",    "at",   "be",
               "but",  "by",   "for",   "if",   "in",    "into", "is",
               "it",   "no",   "not",   "of",   "on",    "or",   "such",
               "that", "the",  "their", "then", "there", "these",
               "they", "this", "to",    "was",  "will",  "with"]]
      (.add s w))
    s))

(def ^{:dynamic true
       :doc     "The set of English punctuation characters, a Java HashSet.
Used in English analyzer."}
  *en-punctuations-set*
  (let [s (HashSet.)]
    (doseq [c [\: \/ \. \; \, \! \= \? \" \' \( \) \[ \] \{ \}
               \| \< \> \& \@ \# \^ \* \\ \~ \`]]
      (.add s c))
    s))

(def ^{:dynamic true
       :doc     "batch size when using search index writer and `:index-position?` is `false`"}
  *index-writer-batch-size* 500)

(def ^{:dynamic true
       :doc     "batch size  when using search index writer and `:index-position?` is `true`"}
  *index-writer-batch-size-pos* 200000)
