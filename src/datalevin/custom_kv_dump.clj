;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0/)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.custom-kv-dump
  "Dependency-complete physical custom KV dumps and preflighted restores."
  (:require [clojure.pprint :as p]
            [taoensso.nippy :as nippy]
            [datalevin.bits :as b]
            [datalevin.constants :as c]
            [datalevin.custom-data :as custom]
            [datalevin.custom-datalog :as cd]
            [datalevin.custom-kv :as ck]
            [datalevin.custom-value :as cv]
            [datalevin.interface :as i]
            [datalevin.kv :as kv]
            [datalevin.lmdb :as l]
            [datalevin.util :refer [raise]])
  (:import [datalevin.binding.cpp CppLMDB]
           [datalevin.bits CustomReference Retrieved]
           [java.nio ByteBuffer]
           [java.util Arrays]))

(defn- info-key [^bytes bytes]
  (case (int (aget bytes 0))
    -12 (b/read-buffer (ByteBuffer/wrap bytes) [:keyword :string])
    -5 (b/read-buffer (ByteBuffer/wrap bytes) :keyword)
    nil))

(defn- custom-options? [opts] (or (:key-type opts) (:value-type opts)))

(defn- owners [sections]
  (reduce
   (fn [owners [{:keys [dbi opts]} rows]]
     (reduce (fn [owners [k v]]
               (let [[avg eid] (cond (= dbi c/eav) [v k]
                                     (= dbi c/ave) [k v])
                     custom? (and avg (>= (alength ^bytes avg) 5)
                                  (= c/type-custom (aget ^bytes avg 4)))
                     r (when custom? (b/read-buffer (ByteBuffer/wrap ^bytes avg) :avg))
                     ref (when r (.-reference ^CustomReference (.-v ^Retrieved r)))]
                 (cond-> owners
                 custom?
                 (update (cv/reference-id ref) (fnil conj #{})
                         [:datom (b/read-buffer (ByteBuffer/wrap ^bytes eid) :id)
                          (.-a ^Retrieved r) (vec ref)])
                 (:key-type opts)
                 (update (cv/reference-id k) (fnil conj #{}) [:key dbi (vec k)])
                 (:value-type opts)
                 (update (cv/reference-id v) (fnil conj #{}) [:item dbi (vec k) (vec v)]))))
             owners rows)) {} sections))

(defn- section [raw dbi]
  [{:dbi dbi :opts (i/dbi-opts raw dbi)}
   (vec (i/get-range raw dbi [:all] :raw :raw))])

(defn capture
  "Capture dependencies and entries in one snapshot. A single-DBI bundle
  includes only its required definitions and payloads, plus allocation state."
  [db root]
  (let [raw (kv/raw-lmdb db)
        datalog? (#{:datalog c/eav c/ave c/schema} root)
        dbis (cond
               datalog? (sort (filter #(.startsWith ^String % "datalevin/")
                                     (remove #{c/kv-info c/custom-values} (i/list-dbis raw))))
               root [root]
               :else (sort (remove #{c/kv-info c/custom-values} (i/list-dbis raw))))
        dbis (if (some #{c/schema} dbis)
               (cons c/schema (remove #{c/schema} dbis)) dbis)]
    ;; Open native handles before acquiring the snapshot.
    (doseq [dbi (concat [c/kv-info c/custom-values] dbis)] (i/get-dbi raw dbi false))
    (let [rtx (when-not (l/writing? raw) (i/get-rtx raw))]
      (try
        (let [data (mapv #(section raw %) dbis)
              ids (set (keys (owners data)))
              types (into #{} (mapcat #(keep (:opts (first %)) [:key-type :value-type])) data)
              types (if datalog?
                      (into types
                            (comp (map #(-> % second b/deserialize :db/valueType))
                                  (filter cd/custom-type?))
                            (second (section raw c/schema)))
                      types)
              wanted (into #{cv/id-key :custom-types-revision [:dbis c/custom-values]}
                           (concat (map #(vector :types %) types)
                                   (map #(vector :dbis %) dbis)))
              metadata (cond-> (section raw c/kv-info)
                         root (update 1 #(filterv (fn [[k _]] (contains? wanted (info-key k))) %)))
              payloads (if root
                         [{:dbi c/custom-values :opts (i/dbi-opts raw c/custom-values)}
                          (mapv (fn [id]
                                  (let [payload (i/get-value raw c/custom-values id :id :raw)]
                                    (when-not payload
                                      (raise "Missing custom payload while dumping"
                                             {:error :custom-type/missing-payload :id id}))
                                    [(cv/encode-order :id id) payload])) (sort ids))]
                         (section raw c/custom-values))]
          {:datalevin/custom-kv-dump 1 :root-dbi root
           :sections (mapv (fn [[header rows]]
                             [(assoc header :entries (count rows))
                              (mapv (fn [[k v]] [(b/encode-base64 k) (b/encode-base64 v)]) rows)])
                           (into [metadata payloads] data))})
        (finally (when rtx (i/return-rtx raw rtx)))))))

(defn dump! [db root output section?]
  (let [bundle (capture db root)]
    (if output
      (nippy/freeze-to-out! output bundle)
      (doseq [[n [header rows]] (map-indexed vector (:sections bundle))]
        (p/pprint (cond-> header
                    section? (assoc :datalevin.dump/section :kv)
                    (zero? (long n)) (assoc :datalevin.dump/custom 1
                                     :section-count (count (:sections bundle))
                                     :root-dbi root)))
        (doseq [row rows] (p/pprint row))))))

(defn read-bundle [header read-form]
  (let [n (:section-count header)]
    (when-not (and (int? n) (<= 2 (long n)))
      (raise "Invalid custom dump section count" {:error :custom-type/dump}))
    {:datalevin/custom-kv-dump 1 :root-dbi (:root-dbi header)
     :sections (loop [i 0 header header sections []]
                 (if (= i n)
                   sections
                   (let [cnt (:entries header)]
                     (when-not (and (string? (:dbi header)) (int? cnt) (<= 0 (long cnt)))
                       (raise "Invalid custom dump section" {:error :custom-type/dump}))
                     (let [rows (vec (repeatedly cnt read-form))]
                       (when-not (every? #(and (vector? %) (= 2 (count %))
                                               (every? string? %)) rows)
                         (raise "Truncated or invalid custom dump" {:error :custom-type/dump}))
                       (recur (inc i) (when (< (inc i) (long n)) (read-form))
                              (conj sections [header rows]))))))}))

(defn- conflict! [message context]
  (raise message (assoc context :error :custom-type/restore-conflict)))

(defn- byte-equal? [^bytes a ^bytes b] (Arrays/equals a b))

(defn- rows-equal? [a b]
  (and (= (count a) (count b))
       (every? true? (map (fn [[ak av] [bk bv]]
                            (and (byte-equal? ak bk) (byte-equal? av bv))) a b))))

(defn- preflight [db raw sections]
  (let [names (mapv (comp :dbi first) sections)
        _ (when (or (not= [c/kv-info c/custom-values] (vec (take 2 names)))
                    (not= (count names) (count (set names))))
            (conflict! "Custom dump dependencies must come first, without duplicate DBIs" {}))
        metadata (into {} (keep (fn [[k v]]
                                 (let [key (info-key k)]
                                   (when (or (#{cv/id-key :custom-types-revision} key)
                                             (and (vector? key) (#{:dbis :types} (first key))))
                                     [key (b/deserialize v)]))))
                       (second (first sections)))
        definitions (into {} (keep (fn [[k v]]
                                     (when (and (vector? k) (= :types (first k)))
                                       [(second k) v]))) metadata)
        {:keys [revision types]} (custom/registry db)
        id-bytes (fn [k] (b/read-buffer (ByteBuffer/wrap ^bytes k) :id))
        payload-rows (second (second sections))
        payloads (into {} (map (fn [[k v]] [(id-bytes k) v])) payload-rows)
        incoming-owners (owners (drop 2 sections))
        _ (when-not (= (count payloads) (count payload-rows))
            (conflict! "Duplicate custom payload IDs in dump" {}))
        existing-sections (mapv #(section raw %)
                               (concat (:custom-dbis @(i/kv-info raw))
                                       (filter #(i/dbi-opts raw %) [c/eav c/ave])))
        existing-owners (owners existing-sections)]
    (when-not (= (get metadata [:dbis c/custom-values]) (:opts (first (second sections))))
      (conflict! "Missing or inconsistent payload DBI metadata" {}))
    (doseq [[name definition] definitions]
      (when-let [installed (get types name)]
        (when-not (custom/same-definition? installed definition)
          (conflict! "Conflicting custom type definition" {:type-name name}))))
    (doseq [[{:keys [dbi opts]} rows] (drop 2 sections)]
      (when-not (= opts (get metadata [:dbis dbi]))
        (conflict! "Missing or inconsistent DBI metadata" {:dbi dbi}))
      (doseq [name (keep opts [:key-type :value-type])]
        (when-not (or (contains? definitions name) (contains? types name))
          (conflict! "Missing custom type definition" {:type-name name :dbi dbi})))
      (when-let [installed (i/dbi-opts raw dbi)]
        (when-not (= (select-keys installed [:key-type :value-type :flags])
                     (select-keys opts [:key-type :value-type :flags]))
          (conflict! "Conflicting DBI declaration" {:dbi dbi}))
        (when (or (custom-options? opts) (#{c/eav c/ave c/schema} dbi))
          (let [old (second (section raw dbi))]
            (when (and (seq old) (not (rows-equal? old rows)))
              (conflict! "Custom target DBI must be empty or identical to the dump" {:dbi dbi})))))
      (doseq [[k v] rows field [:key-type :value-type] :when (get opts field)]
        (let [ref (if (= field :key-type) k v)
              id (:id (cv/decode-reference ref))]
          (when-not (contains? payloads id)
            (conflict! "Missing referenced custom payload in dump" {:dbi dbi :id id})))))
    (doseq [[id occurrences] incoming-owners]
      (when-not (contains? payloads id)
        (conflict! "Missing referenced custom payload in dump" {:id id}))
      (when (> (count occurrences) 1)
        (conflict! "Custom payload is shared by unrelated entries" {:id id})))
    (let [schema-rows (some (fn [[header rows]] (when (= c/schema (:dbi header)) rows)) sections)
          schema (into {} (map (fn [[k v]] [(b/read-buffer (ByteBuffer/wrap ^bytes k) :attr)
                                           (b/deserialize v)])) schema-rows)
          by-aid (into {} (map (fn [[_ props]] [(:db/aid props) props])) schema)
          eav (owners (filter #(= c/eav (:dbi (first %))) sections))
          ave (owners (filter #(= c/ave (:dbi (first %))) sections))]
      (when-not (= eav ave)
        (conflict! "Custom Datalog EAV and AVE ownership must agree" {}))
      (doseq [[_ occurrences] eav
              [_ _ aid] occurrences]
        (let [name (:db/valueType (by-aid aid))]
          (when-not (and (cd/custom-type? name)
                         (or (contains? definitions name) (contains? types name)))
            (conflict! "Missing custom attribute definition" {:aid aid}))))
      (doseq [[_ props] schema
              :let [name (:db/valueType props)] :when (cd/custom-type? name)]
        (when-not (or (contains? definitions name) (contains? types name))
          (conflict! "Missing custom type definition" {:type-name name}))))
    (doseq [[id payload] payloads]
      (when-not (< cv/min-id (long id) cv/max-id)
        (conflict! "Invalid custom payload ID" {:id id}))
      (when-let [existing (when (i/dbi-opts raw c/custom-values)
                           (i/get-value raw c/custom-values id :id :raw))]
        (when-not (and (byte-equal? existing payload)
                       (= (get existing-owners id) (get incoming-owners id)))
          (conflict! "Conflicting custom payload ID or owner" {:id id}))))
    (let [incoming-id (get metadata cv/id-key 0)
          old-id (or (i/get-value raw c/kv-info cv/id-key :keyword :data) 0)
          highest (when (i/dbi-opts raw c/custom-values)
                    (first (i/get-first raw c/custom-values [:all-back] :id :ignore false)))]
      (doseq [id [incoming-id old-id]]
        (when-not (and (int? id) (<= 0 (long id)) (< (long id) cv/max-id))
          (conflict! "Invalid custom allocation state" {:id id})))
      {:last-id (reduce max (max (long old-id) (long incoming-id) (long (or highest 0)))
                        (keys payloads))
       :revision (if (some #(not (contains? types %)) (keys definitions))
                   (inc (long revision)) revision)})))

(defn restore! [db bundle root]
  (when-not (= 1 (:datalevin/custom-kv-dump bundle))
    (conflict! "Unsupported custom dump version" {}))
  (when (and root (not= root (:root-dbi bundle)))
    (conflict! "Custom single-DBI restore must retain its DBI name" {:dbi root}))
  (let [raw (kv/raw-lmdb db)
        sections (mapv (fn [[header rows]]
                         (when-not (= (:entries header) (count rows))
                           (conflict! "Incorrect custom dump entry count" {}))
                         [header (mapv (fn [[k v]] [(b/decode-base64 k) (b/decode-base64 v)]) rows)])
                       (:sections bundle))]
    ;; Native DBI creation owns a transaction. Match its DBI -> writer lock
    ;; order, preflight before creation, and retain both locks through commit.
    (locking (.-dbis ^CppLMDB raw)
      (locking (l/write-txn raw)
        (when (some? @(l/write-txn raw))
          (conflict! "Restore must run outside an explicit transaction" {}))
        (let [{:keys [last-id revision]} (preflight db raw sections)]
          (binding [l/*raw-kv?* true]
            (doseq [[{:keys [dbi opts]} _] (rest sections)]
              (i/open-dbi db dbi opts))
            (l/with-transaction-kv [tx db]
              (i/transact-kv
               tx
               (into [(l/kv-tx :put c/kv-info cv/id-key last-id :keyword :data)
                      (l/kv-tx :put c/kv-info :custom-types-revision revision :keyword :data)]
                     (mapcat (fn [[{:keys [dbi]} rows]]
                               (for [[k v] rows
                                     :when (not (and (= dbi c/kv-info)
                                                      (#{cv/id-key :custom-types-revision} (info-key k))))]
                                 (l/kv-tx :put dbi k v :raw :raw)))) sections))))
          (ck/initialize! db raw)))))
  :loaded)
