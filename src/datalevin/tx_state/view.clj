;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state.view
  "Immutable encoded KV deltas over a pinned native snapshot. Duplicate values
  retain individual insert/delete deltas, never copies of entire native lists."
  (:require [datalevin.bits :as b]
            [datalevin.interface :as i]
            [datalevin.lmdb :as l])
  (:import [java.lang AutoCloseable Iterable]
           [java.nio ByteBuffer BufferOverflowException]
           [java.util Arrays HashMap IdentityHashMap Iterator Map$Entry]
           [datalevin.lmdb RangeContext]))

(defn compare-bytes ^long [^bytes a ^bytes b] (Arrays/compareUnsigned a b))
(defn ordered [] (sorted-map-by compare-bytes))
(defn empty-root [lsn]
  {:lsn (long lsn) :pruned-through (long lsn)
   :dbis {} :by-lsn (sorted-map)})

(defn encode
  "Own the uncompressed encoding, rejecting output beyond the reserved bound."
  ^bytes [value type max-bytes]
  (let [limit (long max-bytes)]
    (loop [size (min 512 limit)]
      (let [buffer (ByteBuffer/allocate (int size))
            success? (try (b/put-buffer buffer value type) true
                          (catch BufferOverflowException _ false))]
        (if success?
          (Arrays/copyOf (.array buffer) (.position buffer))
          (if (< size limit)
            (recur (min limit (* 2 (max 1 size))))
            (throw (ex-info "Encoded value exceeds its reserved budget"
                            {:error :txlog/pending-capacity :outcome :not-committed
                             :max-bytes limit :retryable? false}))))))))

(defn decode [^bytes value type]
  (b/read-buffer (ByteBuffer/wrap value) type))

(defn stage
  "Apply owned, encoded put/del/put-list/del-list rows to an immutable root.
  Update the root/LSN index once per call and index each key once per LSN,
  including across successive requests in the same record. Tree and index
  retain the same canonical key object so pruning can deduplicate by identity.
  Preparation owns the expected next LSN; insertion must verify it before I/O."
  [root lsn rows list-dbi?]
  (if (seq rows)
    (let [lsn (long lsn)
          indexed (get (:by-lsn root) lsn [])]
      (loop [rows (seq rows) dbis (:dbis root) touched indexed]
        (if-let [[op dbi key value] (first rows)]
          (let [delta (get dbis dbi)
                tree (or (:keys delta) (ordered))
                previous (find tree key)
                key (if previous (clojure.core/key previous) key)
                node (when previous (val previous))
                base (assoc node :lsn lsn)
                next-node
                (case op
                  :del (assoc base :delete-lsn lsn :put nil :values (ordered))
                  :put (if (list-dbi? dbi)
                         (update base :values #(assoc (or % (ordered)) value [lsn true]))
                         (assoc base :put [lsn value]))
                  (:put-list :del-list)
                  (let [version [lsn (= op :put-list)]]
                    (update base :values
                            #(reduce (fn [m v] (assoc m v version))
                                     (or % (ordered)) value)))
                  (throw (ex-info "Operation is not supported by the KV pending view"
                                  {:error :txlog/unsupported-operation :op op
                                   :outcome :not-committed})))]
            (recur (next rows)
                   (assoc dbis dbi (assoc delta :keys (assoc tree key next-node)))
                   (if (= lsn (:lsn node)) touched (conj touched [dbi key]))))
          (assoc root :lsn lsn :dbis dbis
                 :by-lsn (if (identical? indexed touched)
                           (:by-lsn root)
                           (assoc (:by-lsn root) lsn touched))))))
    root))

(defn- prune-node [node ^long applied]
  (let [put (:put node) deleted (:delete-lsn node) values (:values node)
        next-put (when (> (long (or (first put) 0)) applied) put)
        next-deleted (when (> (long (or deleted 0)) applied) deleted)
        next-values (when values
                      (reduce-kv (fn [remaining value [lsn _]]
                                   (if (<= (long lsn) applied)
                                     (dissoc remaining value)
                                     remaining))
                                 values values))]
    (if (and (identical? put next-put) (identical? deleted next-deleted)
             (identical? values next-values))
      node
      (assoc node :put next-put :delete-lsn next-deleted :values next-values))))

(defn prune
  "Return a new root without changes already present in the native prefix.
  The LSN index visits only keys touched by the covered prefix; persistent
  trees keep their unaffected branches. Older pinned roots remain immutable."
  [root applied]
  (let [applied (long applied)]
    (cond
      (<= applied (long (:pruned-through root))) root
      (<= (long (:lsn root)) applied) (empty-root (:lsn root))
      :else
      (let [expired (seq (subseq (:by-lsn root) <= applied))
            ;; stage canonicalizes index keys to the persistent tree's keys.
            ;; This temporary set needs neither byte comparisons nor persistent
            ;; sorted nodes, and never escapes into a published/pinned root.
            affected (HashMap.)
            _ (doseq [[_ keys] expired [dbi key] keys]
                (let [^IdentityHashMap seen (or (.get affected dbi)
                                                (let [seen (IdentityHashMap.)]
                                                  (.put affected dbi seen)
                                                  seen))]
                  (.put seen key true)))
            dbis
            (reduce
             (fn [dbis ^Map$Entry affected-entry]
               (let [dbi (.getKey affected-entry)
                     keys (.keySet ^IdentityHashMap (.getValue affected-entry))
                     delta (get dbis dbi)
                     tree (reduce (fn [tree key]
                                    (let [node (get tree key)]
                                      (if (<= (long (:lsn node)) applied)
                                        (dissoc tree key)
                                        (assoc tree key (prune-node node applied)))))
                                  (:keys delta) keys)]
                 (if (seq tree)
                   (assoc dbis dbi (assoc delta :keys tree))
                   (dissoc dbis dbi))))
             (:dbis root) (.entrySet affected))
            by-lsn (reduce (fn [index [lsn _]] (dissoc index lsn))
                           (:by-lsn root) expired)]
        (assoc root :dbis dbis :by-lsn by-lsn :pruned-through applied)))))

(defn- encoded-range [[type & bounds] value-type]
  (into [type] (map #(encode % value-type 511)) bounds))

(defn- range-context ^RangeContext [[type lo hi]] (l/range-table type lo hi))

(defn- range-entries [tree ^RangeContext ctx]
  (let [forward? (.-forward? ctx)
        lo (if forward? (.-start-bf ctx) (.-stop-bf ctx))
        hi (if forward? (.-stop-bf ctx) (.-start-bf ctx))
        lo-test (if (if forward? (.-include-start? ctx) (.-include-stop? ctx)) >= >)
        hi-test (if (if forward? (.-include-stop? ctx) (.-include-start? ctx)) <= <)
        select (if forward? subseq rsubseq)]
    (cond (and lo hi) (select tree lo-test lo hi-test hi)
          lo (select tree lo-test lo)
          hi (select tree hi-test hi)
          forward? (seq tree)
          :else (rseq tree))))

(defn active-nodes
  "Encoded keys changed after base within the requested key range."
  [root dbi k-range k-type base]
  (when-let [tree (get-in root [:dbis dbi :keys])]
    (filter (fn [[_ node]] (> (long (:lsn node)) (long base)))
            (range-entries tree (range-context (encoded-range k-range k-type))))))

(defn footprint
  "Classify bounded native-plus-delta reads on a matched snapshot. Count
  duplicate-value probes as well as bytes so tiny values cannot create an
  unbounded small-delta path."
  [root dbi k-range k-type base]
  (reduce
   (fn [out [^bytes key node]]
     (-> out (update :keys inc)
         (update :items + (max 1 (count (:values node))))
         (update :bytes + (alength key)
                 (if-let [[_ ^bytes value] (:put node)] (alength value) 0)
                 (reduce (fn [^long n [^bytes v _]] (+ n (alength v)))
                         0 (:values node)))))
   {:keys 0 :items 0 :bytes 0}
   (active-nodes root dbi k-range k-type base)))

(defn fast-path
  "Choose native, bounded correction, or merged traversal for a read."
  [footprint]
  (cond (zero? (long (:keys footprint))) :native
        (and (<= (long (:keys footprint)) 32)
             (<= (long (or (:items footprint) 0)) 32)
             (<= (long (:bytes footprint)) 16384)) :small
        :else :merged))

(defn- buffer-bytes ^bytes [^ByteBuffer buffer]
  (let [buffer (.duplicate buffer) bytes (byte-array (.remaining buffer))]
    (.get buffer bytes)
    bytes))

(defn- native-pairs [^Iterator iterator]
  (lazy-seq
   (when (.hasNext iterator)
     (let [row (.next iterator)
           pair [(buffer-bytes (l/k row)) (buffer-bytes (l/v row))]]
       (cons pair (native-pairs iterator))))))

(defn- merge-pairs [compare-pair native delta]
  (lazy-seq
   (let [native (seq native) delta (seq delta)]
     (cond (nil? native) delta
           (nil? delta) native
           (neg? (long (compare-pair (first native) (first delta))))
           (cons (first native) (merge-pairs compare-pair (rest native) delta))
           :else (cons (first delta) (merge-pairs compare-pair native (rest delta)))))))

(defn with-rows
  "Call f on the ordered raw pairs in a matched native/delta snapshot. f must
  consume its result while the cursor is open. No native writer is acquired."
  [raw rtx root base dbi-name k-range k-type v-range v-type f]
  (let [dbi (i/get-dbi raw dbi-name false)
        list? (i/list-dbi? raw dbi-name)
        kr (encoded-range k-range k-type)
        vr (encoded-range v-range v-type)
        kc (range-context kr) vc (range-context vr)
        tree (get-in root [:dbis dbi-name :keys] (ordered))
        active? (fn [node] (and node (> (long (:lsn node)) (long base))))
        native-visible?
        (fn [[key value]]
          (let [node (get tree key)]
            (or (not (active? node))
                (and (<= (long (or (:delete-lsn node) 0)) (long base))
                     (if list?
                       (<= (long (or (first (get (:values node) value)) 0)) (long base))
                       (<= (long (or (first (:put node)) 0)) (long base)))))))
        delta (mapcat
               (fn [[key node]]
                 (when (active? node)
                   (if list?
                     (keep (fn [[value [version present?]]]
                             (when (and present? (> (long version) (long base)))
                               [key value]))
                           (range-entries (or (:values node) (ordered)) vc))
                     (when-let [[version value] (:put node)]
                       (when (> (long version) (long base)) [[key value]])))))
               (range-entries tree kc))
        compare-pair (fn [[ka va] [kb vb]]
                       (let [keys (compare-bytes ka kb)]
                         (if (zero? keys)
                           (if (.-forward? vc) (compare-bytes va vb) (compare-bytes vb va))
                           (if (.-forward? kc) keys (- keys)))))
        cur (l/get-cursor dbi rtx)]
    (try
      (let [^Iterable rows (if list?
                             (l/iterate-list dbi rtx cur kr :raw vr :raw)
                             (l/iterate-kv dbi rtx cur kr :raw :raw))]
        (with-open [^AutoCloseable iterator (.iterator rows)]
          (f (merge-pairs compare-pair
                          (filter native-visible? (native-pairs iterator)) delta))))
      (finally
        (if (l/read-only? rtx) (l/return-cursor dbi cur) (l/close-cursor dbi cur))))))
