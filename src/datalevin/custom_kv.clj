;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0/)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.custom-kv
  "Logical KV operations over registered custom keys and duplicate items."
  (:require
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.custom-data :as custom]
   [datalevin.custom-value :as cv]
   [datalevin.interface :as i]
   [datalevin.lmdb :as l]
   [datalevin.validate :as validate]
   [datalevin.util :refer [raise]])
  (:import [datalevin.lmdb KVTxData DatomKVTxData]
           [java.nio ByteBuffer]))

(defn custom-dbi? [kv dbi]
  (and (not l/*raw-kv?*)
       (contains? (:custom-dbis @(i/kv-info kv)) dbi)))

(defn prepare-dbi!
  "Validate declarations before native creation. Omitted declarations retain
  installed types; changing the interpretation of an existing DBI is rejected."
  [kv raw dbi opts]
  (let [old (i/dbi-opts raw dbi)
        opts (merge (select-keys old [:key-type :value-type]) opts)
        types (select-keys opts [:key-type :value-type])]
    (doseq [[field name] types]
      (when (and old (not= (get old field) name))
        (raise "Cannot change a DBI's installed custom type"
               {:error :custom-type/dbi-conflict :dbi dbi :option field}))
      (custom/resolve-type kv name))
    (when (seq types)
      (when (.startsWith ^String dbi "datalevin/")
        (raise "Custom DBI declarations require a user DBI"
               {:error :custom-type/dbi :dbi dbi}))
      (let [flags (set (or (:flags opts) (:flags old) c/default-dbi-flags))]
        (when (and old
                    (or (not= (contains? (set (:flags old)) :dupsort)
                               (contains? flags :dupsort))
                        (and (:key-type old) (:key-size opts)
                              (not= (:key-size old) (:key-size opts)))))
          (raise "Cannot change a custom index's layout or key-size budget"
                 {:error :custom-type/dbi-conflict :dbi dbi}))
        (when (or (some flags [:reversekey :integerkey :reversedup :integerdup :dupfixed])
                  (and (:value-type opts) (not (flags :dupsort))))
          (raise "Custom values require ordinary byte ordering and duplicate items require dupsort"
                 {:error :custom-type/dbi :dbi dbi :flags flags})))
      (cv/open-store! kv))
    opts))

(defn initialize! [kv raw]
  (when (or (seq (:custom-dbis @(i/kv-info raw)))
            (i/dbi-opts raw c/custom-values))
    (cv/open-store! kv)
    (doseq [dbi (:custom-dbis @(i/kv-info raw))]
      (i/get-dbi raw dbi false)))
  kv)

(defn field-type [opts field supplied]
  (if-let [declared (get opts field)]
    (if (or (nil? supplied) (= supplied :data) (= supplied declared)
            (and (= field :value-type) (= supplied :ignore)))
      declared
      (raise "Operation type disagrees with the DBI's custom type"
             {:error :custom-type/type-mismatch :declared declared :supplied supplied}))
    (let [t (or supplied :data)]
      (when (qualified-keyword? t)
        (raise "Custom storage types must be declared when opening the DBI"
               {:error :custom-type/undeclared :supplied t}))
      t)))

(defn custom-txs? [raw dbi txs]
  (when (and (not l/*raw-kv?*) (seq (:custom-dbis @(i/kv-info raw))))
    (if dbi
      (custom-dbi? raw dbi)
      (some (fn [row]
              (when-not (instance? DatomKVTxData row)
                (custom-dbi? raw (.-dbi-name ^KVTxData (l/->kv-tx-data row)))))
            txs))))

(defn- index [raw dbi position key]
  (cond-> {:dbi dbi :position position
           :max-size (if (= position :key)
                       (:key-size (i/dbi-opts raw dbi)) c/+max-key-size+)}
    (= position :item) (assoc :key key)))

(defn- key-ref [kv raw dbi opts key kt]
  (if-let [name (:key-type opts)]
    (:reference (cv/find-value kv (index raw dbi :key nil)
                               (custom/resolve-type kv name) key))
    (cv/encode-order kt key)))

(defn- ensure-key! [kv raw dbi opts key kt]
  (if-let [name (:key-type opts)]
    (let [type (custom/resolve-type kv name)
          prefix (cv/order-prefix type key (:key-size opts))
          payload ((:serialize type) key)]
      (if-let [ref (key-ref kv raw dbi opts key kt)]
        (do (cv/transact! kv [(l/kv-tx :put c/custom-values
                                       (cv/reference-id ref) payload :id :raw)])
            ref)
        (let [{:keys [id txs]} (cv/allocate-payload kv payload)]
          (cv/transact! kv txs)
          (cv/reference prefix id))))
    (cv/encode-order kt key)))

(defn- delete-key! [kv raw dbi opts ref]
  (let [item-refs (when (:value-type opts) (i/get-list raw dbi ref :raw :raw))
        payloads (cond-> (vec item-refs) (:key-type opts) (conj ref))]
    (cv/transact! kv (into [(l/kv-tx :del dbi ref :raw)]
                           (map cv/delete-payload-tx) payloads))))

(defn- write-row! [kv raw ^KVTxData row]
  (let [dbi (.-dbi-name row)
        opts (i/dbi-opts raw dbi)
        kt (field-type opts :key-type (.-kt row))
        vt (field-type opts :value-type (.-vt row))
        op (.-op row)
        key (.-k row)
        values (if (#{:put-list :del-list} op) (.-v row) [(.-v row)])
        flags (set (.-flags row))]
    (validate/validate-kv-op op)
    (validate/validate-kv-key key kt (and (:validate-data? opts) (not (:key-type opts))))
    (when (and (#{:put-list :del-list} op)
                (not (or (sequential? values) (instance? java.util.List values))))
      (raise "List value must be a sequential collection" {:error :custom-type/transaction}))
    (when (#{:put :put-list} op)
      (doseq [value values]
        (validate/validate-kv-value value vt (and (:validate-data? opts) (not (:value-type opts))))))
    (when (some flags [:reserve :current])
      (raise "Unsupported custom index write flags"
             {:error :custom-type/write-flags :flags flags}))
    (case op
      (:put :put-list)
      (when (seq values)
        (when (and (flags :nooverwrite)
                    (when-let [ref (key-ref kv raw dbi opts key kt)]
                      (some? (i/get-value raw dbi ref :raw :raw))))
          (raise "Custom key already exists" {:error :custom-type/key-exists}))
        (let [ref (ensure-key! kv raw dbi opts key kt)]
          (doseq [value values]
            (if-let [name (:value-type opts)]
              (cv/put-value! kv (assoc (index raw dbi :item ref) :flags flags)
                             (custom/resolve-type kv name) value nil)
              (cv/transact! kv [(l/kv-tx :put dbi ref (cv/encode-order vt value)
                                         :raw :raw flags)])))))

      :del
      (when-let [ref (key-ref kv raw dbi opts key kt)]
        (delete-key! kv raw dbi opts ref))

      :del-list
      (when-let [ref (key-ref kv raw dbi opts key kt)]
        (doseq [value values]
          (if-let [name (:value-type opts)]
            (cv/delete-value! kv (index raw dbi :item ref)
                              (custom/resolve-type kv name) value)
            (cv/transact! kv [(l/kv-tx :del-list dbi ref
                                       [(cv/encode-order vt value)] :raw :raw)])))
        (when (and (:key-type opts) (nil? (i/get-value raw dbi ref :raw :raw)))
          (cv/transact! kv [(cv/delete-payload-tx ref)])))

      (raise "Unsupported custom index transaction operation"
             {:error :custom-type/transaction :op op}))))

(defn transact!
  "Translate a logical batch under one writer; generated physical rows retain
  the KV wrapper's WAL path. Staged values participate in subsequent matches."
  [kv raw dbi rows kt vt]
  (let [rows (mapv (fn [row]
                     (if dbi
                       (let [^KVTxData r (l/->kv-tx-data row kt vt)]
                         (l/kv-tx (.-op r) dbi (.-k r) (.-v r)
                                  (.-kt r) (.-vt r) (.-flags r)))
                       (if (instance? DatomKVTxData row) row (l/->kv-tx-data row)))) rows)]
    (l/with-transaction-kv [tx kv]
      (let [raw-tx (l/mark-write raw)]
        (try
          (doseq [row rows]
            (if (and (instance? KVTxData row)
                      (custom-dbi? raw (.-dbi-name ^KVTxData row)))
              (write-row! tx raw-tx row)
              (cv/transact! tx [row])))
          :transacted
          (catch Throwable e
            (when-not (l/resized? e) (i/abort-transact-kv tx))
            (throw e)))))))

(declare read-operation)

(defn guard-internal! [raw dbi]
  (when (and (not l/*raw-kv?*)
             (or (seq (:custom-dbis @(i/kv-info raw)))
                 (and (i/dbi-opts raw c/custom-values) (i/dbi-opts raw c/eav)))
             (#{c/kv-info c/custom-values} dbi))
    (raise "Custom indexes depend on this internal DBI"
           {:error :custom-type/dependency :dbi dbi})))

(defn clear!
  "Remove an owning DBI's references and payloads together, without user code."
  [kv raw dbi]
  (let [opts (i/dbi-opts raw dbi)]
    (l/with-transaction-kv [tx kv]
      (let [rows (i/get-range (l/mark-write raw) dbi [:all] :raw :raw)
            ids (into #{} (mapcat (fn [[k v]]
                                   (cond-> []
                                     (:key-type opts) (conj (cv/reference-id k))
                                     (:value-type opts) (conj (cv/reference-id v))))) rows)]
        (cv/transact! tx (into (mapv #(l/kv-tx :del dbi (first %) :raw) rows)
                               (map #(l/kv-tx :del c/custom-values % :id)) ids)))))
  nil)

(defmacro read-kv [op kv raw dbi & args]
  `(if (custom-dbi? ~raw ~dbi)
     (read-operation ~kv ~raw ~(keyword op) ~dbi [~@args])
     (~(symbol "datalevin.interface" (name op)) ~raw ~dbi ~@args)))

(defn- defaults [args defaults]
  (into (vec args) (drop (count args) defaults)))

(defn- with-snapshot [kv raw f]
  (let [writing? (l/writing? raw)
        rtx (if writing? @(l/write-txn raw) (i/get-rtx raw))]
    (try (f rtx)
         (finally (when-not writing? (i/return-rtx kv rtx))))))

(defn- decode-field [kv opts field supplied rtx]
  (let [t (field-type opts field supplied)]
    (cond
      (= supplied :ignore) (constantly nil)
      (get opts field) (let [type (custom/resolve-type kv t)]
                         #(cv/read-value-at kv type % rtx))
      :else #(b/read-buffer (ByteBuffer/wrap ^bytes %) t))))

(defn- pair-decoder [kv opts kt vt ignore-key? rtx]
  (when (and ignore-key? (= vt :ignore))
    (raise "Cannot ignore both key and value" {}))
  (let [dk (when-not ignore-key? (decode-field kv opts :key-type kt rtx))
        dv (decode-field kv opts :value-type vt rtx)]
    (fn [[k v]] (if ignore-key? (dv v) [(dk k) (dv v)]))))

(defn- physical-range [kv opts field supplied r]
  (let [t (field-type opts field supplied)]
    (if-let [name (get opts field)]
      (let [[kind a b] r
            ^datalevin.lmdb.RangeContext context (l/range-table kind a b)
            forward? (.-forward? ^datalevin.lmdb.RangeContext context)
            start (.-start-bf ^datalevin.lmdb.RangeContext context)
            stop (.-stop-bf ^datalevin.lmdb.RangeContext context)
            type (delay (custom/resolve-type kv name))
            budget (if (= field :key-type) (:key-size opts) c/+max-key-size+)
            bound (fn [v side inclusive?]
                    (when (some? v)
                      (cv/boundary (cv/order-prefix @type v budget) side inclusive?)))
            lower (bound (if forward? start stop) :lower
                         (if forward? (.-include-start? context) (.-include-stop? context)))
            upper (bound (if forward? stop start) :upper
                         (if forward? (.-include-stop? context) (.-include-start? context)))]
        [(cond
           (and lower upper) (if forward? [:closed lower upper] [:closed-back upper lower])
           lower (if forward? [:at-least lower] [:at-least-back lower])
           upper (if forward? [:at-most upper] [:at-most-back upper])
           :else [(if forward? :all :all-back)]) :raw])
      [(into [(first r)] (map #(when (some? %) (cv/encode-order t %))) (rest r)) :raw])))

(defn- raw-pair [entry]
  [(b/read-buffer (l/k entry) :raw) (b/read-buffer (l/v entry) :raw)])

(defn- scan-callback
  [kv raw dbi opts op pred kr kt vr vt ignore-key? raw? rtx]
  (let [[kr pkt] (physical-range kv opts :key-type kt kr)
        [vr pvt] (when vr (physical-range kv opts :value-type vt vr))
        decode (pair-decoder kv opts kt vt ignore-key? rtx)
        decode-pair (delay (pair-decoder kv opts kt vt false rtx))
        mode (case op
               (:get-some :range-filter :list-range-filter) :filter
               (:range-filter-count :list-range-filter-count) :count
               (:visit :visit-list-range :visit-list-key-range) :visit
               :keep)
        first? (#{:get-some :range-some :list-range-some} op)
        count (volatile! 0)
        f (fn [entry]
            ;; Copy before a raw callback or payload lookup can reuse buffers.
            (let [pair (raw-pair entry)
                  result (if raw? (pred entry) (apply pred (@decode-pair pair)))]
              (case mode
                :filter (when result [(decode pair)])
                :count (do (when result (vswap! count #(inc (long %)))) nil)
                :visit result
                (when (if first? result (some? result)) [result]))))
        result (cond
                 (= mode :visit)
                 (if vr
                   (i/visit-list-range raw dbi f kr pkt vr pvt true)
                   (i/visit raw dbi f kr pkt :raw true))
                 vr
                 ((if first? i/list-range-some i/list-range-keep)
                  raw dbi f kr pkt vr pvt true)
                 :else
                 ((if first? i/range-some i/range-keep) raw dbi f kr pkt :raw true))]
    (case mode
      :count @count
      :visit nil
      (if first? (first result) (mapv first result)))))

(defn read-operation [kv raw op dbi args]
  (let [opts (i/dbi-opts raw dbi)]
    (if (= op :range-seq)
      (let [[kr kt vt ignore? seq-opts] (defaults args [nil :data :data false nil])
            [kr pkt] (physical-range kv opts :key-type kt kr)
            kt (field-type opts :key-type kt)
            vt (if (= vt :ignore) :ignore (field-type opts :value-type vt))
            key-type (when (and (:key-type opts) (not ignore?)) (custom/resolve-type kv kt))
            val-type (when (and (:value-type opts) (not= vt :ignore)) (custom/resolve-type kv vt))]
        ;; Decode in the iterator's own snapshot, including deferred batches.
        (i/range-seq raw dbi kr pkt :raw false
                     (assoc seq-opts :datalevin.scan/map-kv
                            (fn [entry rtx]
                              (let [[k v] (raw-pair entry)
                                    v (cond (= vt :ignore) nil
                                            val-type (cv/read-value-at kv val-type v rtx)
                                            :else (b/read-buffer (ByteBuffer/wrap ^bytes v) vt))]
                                (if ignore? v
                                    [(if key-type (cv/read-value-at kv key-type k rtx)
                                         (b/read-buffer (ByteBuffer/wrap ^bytes k) kt)) v]))))))
      (with-snapshot kv raw
        (fn [rtx]
          (case op
            :get-value
            (let [[key kt vt ignore?] (defaults args [nil :data :data true])
                  kt (field-type opts :key-type kt)
                  decode (pair-decoder kv opts kt vt ignore? rtx)]
              (when-let [ref (key-ref kv raw dbi opts key kt)]
                (when-let [pair (i/get-value raw dbi ref :raw :raw false)]
                  (decode pair))))

            (:get-range :get-first :get-first-n)
            (let [[n args] (if (= op :get-first-n) [(first args) (subvec args 1)] [nil args])
                  [kr kt vt ignore?] (defaults args [nil :data :data false])
                  [kr pkt] (physical-range kv opts :key-type kt kr)
                  decode (pair-decoder kv opts kt vt ignore? rtx)]
              (case op
                :get-first (some-> (i/get-first raw dbi kr pkt :raw false) decode)
                :get-first-n (mapv decode (i/get-first-n raw dbi n kr pkt :raw false))
                (mapv decode (i/get-range raw dbi kr pkt :raw false))))

            (:range-count :key-range-count :key-range-list-count :list-range-count)
            (let [[kr kt] (defaults args [nil :data])
                  [kr pkt] (physical-range kv opts :key-type kt kr)]
              ((case op :key-range-count i/key-range-count
                     :range-count i/range-count
                     :list-range-count i/list-range-count i/key-range-list-count)
               raw dbi kr pkt))

            :key-range
            (let [[kr kt] (defaults args [nil :data])
                  [kr pkt] (physical-range kv opts :key-type kt kr)
                  decode (decode-field kv opts :key-type kt rtx)]
              (mapv decode (i/key-range raw dbi kr pkt)))

            :get-rank
            (let [[key kt] (defaults args [nil :data])
                  kt (field-type opts :key-type kt)]
              (when-let [ref (key-ref kv raw dbi opts key kt)]
                (i/get-rank raw dbi ref :raw)))

            (:get-by-rank :sample-kv)
            (let [[n kt vt ignore?] (defaults args [nil :data :data true])
                  decode (pair-decoder kv opts kt vt ignore? rtx)]
              (if (= op :sample-kv)
                (mapv decode (i/sample-kv raw dbi n :raw :raw false))
                (some-> (i/get-by-rank raw dbi n :raw :raw false) decode)))

            (:get-list :list-count :in-list? :near-list :visit-list)
            (let [[visitor args] (if (= op :visit-list)
                                   [(first args) (subvec args 1)] [nil args])
                  [key value kt vt raw?]
                  (if (#{:in-list? :near-list} op)
                    (defaults args [nil nil :data :data true])
                    (let [[key kt vt raw?] (defaults args [nil :data :data true])]
                      [key nil kt vt raw?]))
                  kt (field-type opts :key-type kt)
                  vt (field-type opts :value-type vt)
                  ref (key-ref kv raw dbi opts key kt)]
              (case op
                :list-count (if ref (i/list-count raw dbi ref :raw) 0)
                :get-list (if ref (mapv (decode-field kv opts :value-type vt rtx)
                                        (i/get-list raw dbi ref :raw :raw)) [])
                :in-list? (boolean
                           (when ref
                             (if-let [name (:value-type opts)]
                               (cv/find-value kv (index raw dbi :item ref)
                                              (custom/resolve-type kv name) value)
                               (i/in-list? raw dbi ref value :raw vt))))
                :near-list
                (when ref
                  (let [[vr pvt] (physical-range kv opts :value-type vt [:at-least value])]
                    (when-let [[_ v] (i/list-range-first raw dbi [:closed ref ref] :raw vr pvt)]
                      (if (:value-type opts)
                        (ByteBuffer/wrap ^bytes
                                         (i/get-value raw c/custom-values
                                                      (cv/reference-id v) :id :raw))
                        (ByteBuffer/wrap ^bytes v)))))
                :visit-list
                (when ref
                  (i/visit-list raw dbi
                                (if raw? visitor
                                    (let [decode (decode-field kv opts :value-type vt rtx)]
                                      (fn [v] (visitor (decode v)))))
                                ref :raw :raw raw?))))

            (:list-range :list-range-first :list-range-first-n)
            (let [[n args] (if (= op :list-range-first-n)
                             [(first args) (subvec args 1)] [nil args])
                  [kr kt vr vt] args
                  [kr pkt] (physical-range kv opts :key-type kt kr)
                  [vr pvt] (physical-range kv opts :value-type vt vr)
                  decode (pair-decoder kv opts kt vt false rtx)
                  physical (fn [[k v]] [(if (= pkt :raw) k (cv/encode-order pkt k))
                                        (if (= pvt :raw) v (cv/encode-order pvt v))])]
              (case op
                :list-range-first (some-> (i/list-range-first raw dbi kr pkt vr pvt) physical decode)
                :list-range-first-n (mapv (comp decode physical)
                                          (i/list-range-first-n raw dbi n kr pkt vr pvt))
                (mapv (comp decode physical) (i/list-range raw dbi kr pkt vr pvt))))

            (:get-some :range-filter :range-keep :range-some :range-filter-count :visit)
            (let [[pred kr kt vt & tail] (defaults args [nil nil :data :data])
                  [ignore? raw?] (if (#{:get-some :range-filter} op)
                                  (defaults tail [false true])
                                  [false (first (defaults tail [true]))])]
              (scan-callback kv raw dbi opts op pred kr kt nil vt ignore? raw? rtx))

            (:list-range-filter :list-range-filter-count :list-range-keep
                                :list-range-some :visit-list-range :visit-list-key-range)
            (let [[pred kr kt vr vt raw?]
                  (if (= op :visit-list-key-range)
                    (let [[pred kr kt vt raw?] (defaults args [nil nil :data :data true])]
                      [pred kr kt [:all] vt raw?])
                    (defaults args [nil nil :data nil :data true]))]
              (scan-callback kv raw dbi opts op pred kr kt vr vt false raw? rtx))

            (:visit-key-range :visit-key-sample :visit-list-sample)
            (let [[indices args] (if (#{:visit-key-sample :visit-list-sample} op)
                                  [(first args) (subvec args 1)] [nil args])
                  [visitor kr kt vt raw?]
                  (if (= op :visit-list-sample)
                    (defaults args [nil nil :data :data true])
                    (let [[visitor kr kt raw?] (defaults args [nil nil :data true])]
                      [visitor kr kt :ignore raw?]))
                  [kr pkt] (physical-range kv opts :key-type kt kr)
                  decode (pair-decoder kv opts kt vt false rtx)
                  dk (decode-field kv opts :key-type kt rtx)
                  f (if raw? visitor
                        (if (= op :visit-list-sample)
                          #(apply visitor (decode (raw-pair %)))
                          (if (= op :visit-key-range)
                            #(visitor (dk (b/read-buffer % :raw)))
                            #(visitor (dk (b/read-buffer (l/k %) :raw)) nil))))]
              (case op
                :visit-key-range (i/visit-key-range raw dbi f kr pkt true)
                :visit-key-sample (i/visit-key-sample raw dbi indices f kr pkt true)
                (i/visit-list-sample raw dbi indices f kr pkt :raw true)))

            (raise "Unsupported custom KV read" {:operation op})))))))
