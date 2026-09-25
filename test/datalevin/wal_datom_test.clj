(ns datalevin.wal-datom-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :as walk]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.txlog :as txlog]
            [datalevin.txlog.codec :as codec]
            [datalevin.util :as u])
  (:import [datalevin.lmdb DatomKVTxData]
           [java.io ByteArrayOutputStream]
           [java.nio.file Files CopyOption StandardCopyOption]
           [java.util Arrays]))

(use-fixtures :each
  (fn [f] (binding [c/*db-background-sampling?* false] (f))))

(defn- comparable [x]
  (walk/postwalk #(if (bytes? %) (vec %) %) x))

(deftest compact-datoms-roundtrip-with-physical-rows
  (doseq [term [nil 7]]
    (let [avg (byte-array (range 110))
          rows [(l/kv-tx :put "custom" :key :value)
                (DatomKVTxData. 0 avg true true)
                (DatomKVTxData. Long/MAX_VALUE avg false false)
                (l/kv-tx :put c/giants 99 {:value "large"} :id :data)]
          expanded (l/expand-datom-kv-txs rows)
          old-body (codec/encode-commit-row-payload 3 5 expanded {:ha-term term})
          body (codec/encode-commit-row-payload 3 5 rows {:ha-term term})
          decoded (codec/decode-commit-row-payload body)]
      (is (= 1 (aget ^bytes old-body 4)))
      (is (= 2 (aget ^bytes body 4)))
      (is (< (alength ^bytes body) (alength ^bytes old-body)))
      (is (= (comparable (codec/decode-commit-row-payload old-body))
             (comparable decoded)))
      (is (= 4 (:op-count (codec/decode-commit-row-payload-header body))))
      (is (= term (:ha-term decoded)))
      (testing "HA mirroring reconstitutes exactly the same compact payload"
        (is (Arrays/equals body
                           (codec/encode-commit-row-payload
                            3 5 (codec/compact-replay-rows (:ops decoded))
                            {:ha-term term}))))
      (testing "header patching keeps the compact operations intact"
        (codec/patch-commit-row-payload-header! body 11 13)
        (is (= (assoc (comparable decoded) :lsn 11 :ts 13)
               (comparable (codec/decode-commit-row-payload body))))))))

(deftest replay-compaction-requires-an-exact-pair
  (let [avg (byte-array [1 2 3])
        ave [:put c/ave avg 3 :raw :id]
        eav [:put c/eav 3 avg :id :raw]]
    (doseq [rows [[ave]
                  [eav ave]
                  [ave (assoc eav 2 4)]
                  [ave (assoc eav 3 (byte-array [4 5 6]))]
                  [ave (conj eav [:nooverwrite])]
                  [ave [:put "custom" :k :v] eav]
                  [[:del-list c/ave avg [3 4] :raw :id]
                   [:del-list c/eav 3 [avg] :id :raw]]]]
      (is (= (comparable rows)
             (comparable (vec (codec/compact-replay-rows rows))))))))

(deftest compact-datoms-parallel-encoding-and-corruption
  (let [rows (mapv #(DatomKVTxData. % (byte-array 110 (byte (mod % 127)))
                                  (even? %) false)
                   (range 2048))
        parallel (codec/encode-commit-row-payload 1 2 rows)
        output (ByteArrayOutputStream.)
        _ (.write output ^bytes parallel 0 28)
        _ (doseq [row rows]
            ;; Single-row payloads use the serial encoder even with compiler
            ;; direct linking enabled. Keep the batch's header/op count.
            (let [body (codec/encode-commit-row-payload 1 2 [row])]
              (.write output ^bytes body 28 (- (alength ^bytes body) 28))))
        serial (.toByteArray output)]
    (is (Arrays/equals serial parallel))
    (is (= 4096 (count (:ops (codec/decode-commit-row-payload parallel)))))
    (doseq [length [0 27 28 29 36 38 (dec (alength ^bytes serial))]]
      (is (thrown? clojure.lang.ExceptionInfo
                   (codec/decode-commit-row-payload
                    (Arrays/copyOf ^bytes serial (int length))))))
    (let [bad-version (aclone ^bytes serial)]
      (aset-byte bad-version 4 (byte 3))
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unsupported.*major"
                            (codec/decode-commit-row-payload bad-version))))
    (let [bad-op (aclone ^bytes serial)]
      (aset-byte bad-op 28 (byte 127))
      (is (thrown? clojure.lang.ExceptionInfo
                   (codec/decode-commit-row-payload bad-op))))))

(def ^:private opts
  {:wal? true :wal-durability-profile :strict
   :wal-commit-marker? true :snapshot-bootstrap-force? false})

(def ^:private schema
  {:value {:db/valueType :db.type/string}
   :tags {:db/valueType :db.type/string :db/cardinality :db.cardinality/many}})

(defn- copy-file! [from to]
  (Files/copy (.toPath (u/file from)) (.toPath (u/file to))
              (into-array CopyOption [StandardCopyOption/REPLACE_EXISTING])))

(defn- datoms [conn index]
  (set (map (juxt :e :a :v) (d/datoms @conn index))))

(deftest noindex-backfill-wal-recovery
  (let [dir (u/tmp-dir (str "wal-noindex-" (random-uuid)))
        source (str dir "/source")
        baseline (str dir "/baseline.mdb")
        baseline-meta (str dir "/baseline-meta")
        schema {:body {:db/valueType :db.type/string :db/noindex true}}
        giant (apply str (repeat 1000 "payload"))]
    (try
      (let [conn (d/create-conn source schema opts)]
        (try
          (d/transact! conn [{:db/id 1 :body "initial"}])
          (is (empty? (d/datoms @conn :ave)))
          (finally (d/close conn))))
      (copy-file! (str source "/data.mdb") baseline)
      (copy-file! (txlog/meta-path (str source "/txlog")) baseline-meta)
      (let [conn (d/create-conn source nil opts)]
        (try
          (d/transact! conn [{:db/id 1 :body giant}])
          (d/index-attr conn :body)
          (d/transact! conn [{:db/id 2 :body "indexed"}])
          (finally (d/close conn))))
      (copy-file! baseline (str source "/data.mdb"))
      (copy-file! baseline-meta (txlog/meta-path (str source "/txlog")))
      (let [conn (d/create-conn source nil opts)]
        (try
          (is (nil? (get-in (d/schema conn) [:body :db/noindex])))
          (is (= #{[1 :body giant] [2 :body "indexed"]}
                 (datoms conn :eav) (datoms conn :ave)))
          (is (= #{[1]} (d/q '[:find ?e :in $ ?v :where [?e :body ?v]] @conn giant)))
          (finally (d/close conn))))
      (finally (u/delete-files dir)))))

(deftest compact-wal-recovery-and-follower-mirroring
  (let [dir (u/tmp-dir (str "wal-datoms-" (random-uuid)))
        source (str dir "/source")
        follower (str dir "/follower")
        baseline (str dir "/baseline.mdb")
        baseline-meta (str dir "/baseline-meta")
        giant (apply str (repeat 2000 "x"))
        expected (atom nil)
        tail (atom nil)
        floor (atom nil)]
    (u/create-dirs follower)
    (try
      (let [conn (d/get-conn source schema opts)]
        (try
          (d/transact! conn [{:db/id 0 :value "old" :tags ["keep" "remove"]}
                             {:db/id 1 :value (str "old-" giant)}])
          (reset! floor (:lsn (last (d/open-tx-log (d/datalog-kv conn) 1))))
          (finally (d/close conn))))
      ;; Save a complete, closed LMDB checkpoint. Later restore only LMDB so
      ;; the surviving WAL must redo the tail, including both giant directions.
      (copy-file! (str source "/data.mdb") baseline)
      (copy-file! (txlog/meta-path (str source "/txlog")) baseline-meta)
      (copy-file! baseline (str follower "/data.mdb"))
      (copy-file! (str source "/" c/version-file-name)
                  (str follower "/" c/version-file-name))
      (let [conn (d/get-conn source schema opts)]
        (try
          (d/transact! conn [{:db/id 0 :value giant}
                             {:db/id 1 :value "small"}
                             [:db/retract 0 :tags "remove"]
                             [:db/add 0 :tags "new"]])
          (reset! expected (datoms conn :eav))
          (is (= #{[0 :value giant] [0 :tags "keep"] [0 :tags "new"]
                   [1 :value "small"]}
                 @expected))
          (is (= @expected (datoms conn :ave)))
          (reset! tail (vec (kv/open-tx-log-rows (d/datalog-kv conn)
                                               (inc @floor))))
          (is (seq @tail))
          (doseq [record @tail]
            (let [legacy (codec/encode-commit-row-payload
                          (:lsn record) (:tx-time record) (:rows record))]
              (is (< (:payload-bytes record) (alength ^bytes legacy)))))
          (finally (d/close conn))))
      (testing "recovery rebuilds AVE and EAV from compact WAL"
        (copy-file! baseline (str source "/data.mdb"))
        (copy-file! baseline-meta (txlog/meta-path (str source "/txlog")))
        (dotimes [_ 2]
          (let [conn (d/get-conn source schema opts)]
            (try
              (is (= @expected (datoms conn :eav) (datoms conn :ave)))
              (is (= {:value giant} (d/pull @conn [:value] 0)))
              (is (= [0] (mapv :e (d/datoms @conn :ave :value giant))))
              (is (= #{[1]} (d/q '[:find ?e :where [?e :value "small"]]
                                 @conn)))
              (finally (d/close conn))))))
      (testing "followers also retain compact WAL and replay both indexes"
        (let [conn (d/get-conn follower schema opts)]
          (try
            (doseq [record @tail]
              (kv/mirror-replayed-txlog-record! (d/datalog-kv conn) record)
              ;; Compact datom replay is idempotent. Generic giant-body rows
              ;; retain their original append flags and are not part of this
              ;; repeated-index-write check.
              (kv/replay-txlog-rows!
               (d/datalog-kv conn)
               (filterv #(#{c/ave c/eav} (second %)) (:rows record))
               (:lsn record)))
            ;; Refresh Datalog caches after physical replication.
            (finally (d/close conn))))
        (let [conn (d/get-conn follower schema opts)]
          (try
            (is (= @expected (datoms conn :eav) (datoms conn :ave)))
            (is (= {:value giant} (d/pull @conn [:value] 0)))
            (is (= [0] (mapv :e (d/datoms @conn :ave :value giant))))
            (doseq [record (kv/open-tx-log-rows (d/datalog-kv conn) (inc @floor))]
              (is (< (:payload-bytes record)
                     (alength ^bytes (codec/encode-commit-row-payload
                                      (:lsn record) (:tx-time record)
                                      (:rows record))))))
            (finally (d/close conn)))))
      (finally (u/delete-files dir)))))
