;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.tx-state.kv
  "Standalone KV preparation adapter. Migration is environment-wide; this
  internal adapter is not selected by the public open path until its gates pass."
  (:require [datalevin.bits :as b]
            [datalevin.binding.cpp :as cpp]
            [datalevin.constants :as c]
            [datalevin.interface :as i]
            [datalevin.kv.txlog :as kvtx]
            [datalevin.lmdb :as l]
            [datalevin.tx-group :as group]
            [datalevin.tx-state :as state]
            [datalevin.tx-state.view :as view]
            [datalevin.validate :as vld]
            [datalevin.txlog :as wal])
  (:import [java.nio ByteBuffer]
           [java.util Arrays]
           [java.util.concurrent ConcurrentLinkedQueue]))

(defrecord Preparation [raw state catalog snapshot token thread lsn root rows
                        budget used aborted? finished? encoding-only?])

(defn- native-applied-lsn ^long [raw reader]
  (l/put-key reader c/wal-local-payload-lsn :keyword)
  (let [value (l/get-kv (i/get-dbi raw c/kv-info false) reader)]
    (long (if value (b/read-buffer ^ByteBuffer value :data) 0))))

(defn native-snapshot
  "Read the applied marker through the same reader used for all KV probes."
  [raw]
  (let [reader (cpp/get-pending-rtx raw)]
    (try
      {:reader reader :base-lsn (native-applied-lsn raw reader)
       :close! #(i/return-rtx raw reader)}
      (catch Throwable e (i/return-rtx raw reader) (throw e)))))

(declare prepare-group!)

(defn create
  "Attach one preparation adapter to an already recovered private KV runtime.
  The environment opener must enforce protocol ownership before native open.
  Catalog and unsupported operators must never fall through to a legacy writer."
  [raw opts]
  (when-not (pos-int? (get opts :wal-preparation-timeout-ms 30000))
    (throw (ex-info "WAL preparation timeout must be a positive integer"
                    {:error :txlog/invalid-preparation-timeout})))
  (when-not (nat-int? (get opts :wal-preparation-max-nanos 1000000))
    (throw (ex-info "WAL preparation window must be a nonnegative integer"
                    {:error :txlog/invalid-preparation-window})))
  (when-not (some? (:db-identity @(i/kv-info raw)))
    (throw (ex-info "Independent KV preparation requires a persisted database identity"
                    {:error :txlog/missing-database-identity})))
  (when (or (:ha-mode @(i/kv-info raw))
            (seq (:custom-dbis @(i/kv-info raw))))
    (throw (ex-info "KV pending preparation does not support HA/custom stores"
                    {:error :txlog/unsupported-write-protocol})))
  ;; Reopening a known DBI may allocate a native handle. Finish that work at
  ;; attach time; preparation must never lazily acquire a writer for a read.
  (doseq [dbi (i/list-dbis raw)] (i/get-dbi raw dbi false))
  (let [wal-state (wal/enabled-state raw)
        slot (volatile! nil)
        pipeline-slot (volatile! nil)
        runtime (state/create
                 wal-state
                 (assoc opts
                        :db-identity (:db-identity @(i/kv-info raw))
                        :applied-lsn @(:meta-last-applied-lsn wal-state)
                        :apply-range! #(kvtx/apply-appended-range! raw @slot %1 %2)))]
    (vreset! slot runtime)
    (vswap! (i/kv-info raw) assoc
            :native-lifetime (:lifetime runtime)
            :unpublished-reader-lsn (let [owner-slot (:owner runtime)
                                          failure-slot (:failure runtime)
                                          published-slot (:published runtime)]
                                      (fn [reader]
                                      ;; The snapshot already exists. An owner
                                      ;; starting after this read cannot have
                                      ;; committed into it. Check failure after
                                      ;; owner: a failed applicant clears owner
                                      ;; only after fencing publication.
                                        (let [owner @owner-slot]
                                          (when @failure-slot (state/check-admission! runtime))
                                          (when owner
                                            (let [lsn (native-applied-lsn raw reader)]
                                              (when (> lsn (long @published-slot)) lsn))))))
            :await-native-publication! #(state/await-publication! runtime %)
            :close-independent!
            #(state/close! runtime (or (:wal-close-timeout-ms opts) 30000)
                           (fn []
                             (i/close-kv raw)
                             (kvtx/close-txlog-state! raw))))
    (let [collector (group/create (or (:write-batch-size opts) 256))
          appending (volatile! false)
          sealed-pending (volatile! 0)]
      ;; A decoupled sync owner waits for an adjacent record while a group is in
      ;; its append step, while a sealed group is still waiting for its ordered
      ;; preparation turn, or while ready requests are queued. A slow body still
      ;; cannot hold the flush back: waiting is driven by imminent/ready records,
      ;; never by a body timer.
      (vswap! (:application-hooks wal-state)
              assoc :sync-more-work?
              (fn []
                (or (boolean @appending)
                    (pos? (long @sealed-pending))
                    (not (.isEmpty ^ConcurrentLinkedQueue (.-queue collector))))))
      (vreset! pipeline-slot
               {:raw raw :state runtime :catalog (:dbis @(i/kv-info raw))
                :preparation-timeout-ms (get opts :wal-preparation-timeout-ms 30000)
                :preparation-max-nanos (get opts :wal-preparation-max-nanos 1000000)
                :appending appending
                :sealed-pending sealed-pending
                :collector collector}))))

(defn- check-context! [context]
  (when-not (and (identical? (:thread context) (Thread/currentThread))
                 (not @(:finished? context)))
    (throw (IllegalStateException. "Transaction context has left preparation")))
  (state/check-admission! (:state context)))

(defn- supported-dbi! [context dbi]
  (when (or (= dbi c/kv-info) (not (string? dbi)))
    (throw (ex-info "Internal/catalog writes require the catalog barrier"
                    {:error :txlog/unsupported-operation :outcome :not-committed})))
  (let [opts (get (:catalog context) dbi)]
    (when (or (nil? opts) (:key-type opts) (:value-type opts)
              (some #{:reversekey :reversedup :integerkey :integerdup :dupfixed}
                    (:flags opts)))
      (throw (ex-info "DBI is not supported by ordered KV preparation"
                      {:error :txlog/unsupported-dbi :dbi dbi
                       :outcome :not-committed}))))
  nil)

(defn- list-dbi? [context dbi]
  (boolean (some #{:dupsort} (:flags (get (:catalog context) dbi)))))

(defn transact!
  "Stage typed canonical KV rows in this explicit request. Every invocation in
  the body contributes to the same atomic record/LSN. No native write occurs."
  [context rows]
  (check-context! context)
  (when @(:aborted? context)
    (throw (ex-info "Transaction was aborted" {:outcome :not-committed})))
  (doseq [[op dbi key value kt vt flags :as row] rows]
    (supported-dbi! context dbi)
    (when (or (not (#{:put :del :put-list :del-list} op)) (seq flags))
      (throw (ex-info "Unsupported prepared KV operation or put flags"
                      {:error :txlog/unsupported-operation :op op
                       :outcome :not-committed})))
    (try
      (vld/validate-kv-tx-data (l/->kv-tx-data
                                (if (= op :del)
                                  [op dbi key (or value :data)]
                                  [op dbi key value (or kt :data) (or vt :data)]))
                               (boolean (:validate-data? (get (:catalog context) dbi))))
      (catch Throwable e
        (throw (ex-info (ex-message e)
                        (assoc (ex-data e) :outcome :not-committed) e))))
    (let [remaining (- (long (:budget context)) (long @(:used context)))
          ;; Budget includes owned encoding, WAL encoding scratch and immutable
          ;; index nodes. Reject before allocation/append instead of waiting
          ;; while holding the preparation turn.
          limit (max 0 (quot (- remaining 512) 3))
          key-type (or (if (= op :del) value kt) :data)
          value-type (or vt :data)
          k (view/encode key key-type (min 511 limit))
          list? (list-dbi? context dbi)
          v (case op
              :del nil
              (:put-list :del-list)
              (loop [values (seq value) out [] left (- limit (alength k))]
                (if values
                  (let [v (view/encode (first values) value-type (min 511 (max 0 left)))]
                    (recur (next values) (conj out v) (- left (alength v) 64)))
                  out))
              (view/encode value value-type
                           (max 0 (min (- limit (alength k))
                                       (if list? 511 Long/MAX_VALUE)))))
          size (+ 512 (* 3 (+ (alength k)
                              (long (if (= op :del) 0
                                        (if (#{:put-list :del-list} op)
                                          (reduce (fn [^long n ^bytes v] (+ n 64 (alength v))) 0 v)
                                          (alength ^bytes v)))))))]
      (try
        (vld/validate-encoded-key k)
        (when (and list? (not= op :del))
          (if (#{:put-list :del-list} op)
            (doseq [value v] (vld/validate-encoded-key value))
            (vld/validate-encoded-key v)))
        (catch Throwable e
          (throw (ex-info (ex-message e)
                          (assoc (ex-data e) :outcome :not-committed) e))))
      (when (> size remaining)
        (throw (ex-info "Transaction exceeds its pending-state reservation"
                        {:error :txlog/pending-capacity :outcome :not-committed
                         :retryable? false})))
      (when (and (#{:put-list :del-list} op) (not list?))
        (throw (ex-info "List operation requires a duplicate DBI"
                        {:error :txlog/unsupported-operation :row row
                         :outcome :not-committed})))
      (let [encoded (if (= op :del) [op dbi k :raw] [op dbi k v :raw :raw])]
        (vswap! (:rows context) conj encoded)
        (when-not (:encoding-only? context)
          (vswap! (:root context) view/stage (:lsn context) [[op dbi k v]]
                  #(list-dbi? context %)))
        (vswap! (:used context) #(+ (long %) size)))))
  :transacted)

(defn abort! [context]
  (check-context! context)
  (vreset! (:aborted? context) true)
  nil)

(defn- snapshot! [context]
  (or @(:snapshot context)
      (let [snapshot (state/capture-view (:state context) @(:root context)
                                         #(native-snapshot (:raw context)))]
        (vreset! (:snapshot context) snapshot)
        snapshot)))

(defn with-rows
  "Consume raw encoded pairs while this request's matched reader is pinned."
  [context dbi key-range key-type value-range value-type f]
  (check-context! context)
  (supported-dbi! context dbi)
  (let [snapshot (snapshot! context)]
    (view/with-rows (:raw context) (:reader snapshot)
      @(:root context) (:base-lsn snapshot) dbi
      key-range key-type value-range value-type f)))

(defn get-value [context dbi key kt vt]
  (check-context! context)
  (supported-dbi! context dbi)
  (let [raw (:raw context)
        snapshot (snapshot! context)
        root @(:root context)
        base (long (:base-lsn snapshot))
        tree (get-in root [:dbis dbi :keys])
        node (when (seq tree) (get tree (view/encode key kt 511)))]
    (cond
      (list-dbi? context dbi)
      (with-rows context dbi [:closed key key] kt [:all] vt
        #(when-let [[_ v] (first %)] (view/decode v vt)))
      (> (long (or (first (:put node)) 0)) base)
      (view/decode (second (:put node)) vt)
      (> (long (or (:delete-lsn node) 0)) base) nil
      :else
      (let [reader (:reader snapshot)
            handle (i/get-dbi raw dbi false)]
        (l/put-read-key handle reader key kt)
        (when-let [buffer (l/get-kv handle reader)]
          (b/read-buffer buffer vt))))))

(defn get-range [context dbi key-range kt vt]
  (with-rows context dbi key-range kt [:all] vt
    #(mapv (fn [[k v]] [(view/decode k kt) (view/decode v vt)]) %)))

(defn- small-range-count-adjustment
  [context snapshot root dbi key-range kt]
  (let [raw (:raw context)
        reader (:reader snapshot)
        base (long (:base-lsn snapshot))
        list? (list-dbi? context dbi)]
    (reduce
     (fn [^long adjustment [^bytes key node]]
       (if list?
         (if (> (long (or (:delete-lsn node) 0)) base)
           ;; A whole-key delete hides every native duplicate. Count the old
           ;; key with the native kernel, then add only later staged values.
           (+ adjustment
              (- (long (reduce (fn [^long n [_ [lsn present?]]]
                                 (if (and present? (> (long lsn) base)) (inc n) n))
                               0 (:values node)))
                 (long (cpp/range-count-pending raw reader dbi
                                                [:closed key key] :raw))))
           (reduce
            (fn [^long n [^bytes value [lsn present?]]]
              (if (<= (long lsn) base)
                n
                (let [native? (cpp/list-value-pending? raw reader dbi key value)]
                  (cond (and present? (not native?)) (inc n)
                        (and (not present?) native?) (dec n)
                        :else n))))
            adjustment (:values node)))
         (let [native? (cpp/key-present-pending? raw reader dbi key)
               present? (> (long (or (first (:put node)) 0)) base)]
           (+ adjustment (- (if present? 1 0) (if native? 1 0))))))
     0 (view/active-nodes root dbi key-range kt base))))

(defn range-count [context dbi key-range kt]
  (check-context! context)
  (supported-dbi! context dbi)
  (let [snapshot (snapshot! context)
        root @(:root context)
        base (:base-lsn snapshot)
        path (view/fast-path (view/footprint root dbi key-range kt base))]
    (if (= path :merged)
      (view/with-rows (:raw context) (:reader snapshot) root base dbi
                      key-range kt [:all] :raw count)
      (let [native (long (cpp/range-count-pending
                          (:raw context) (:reader snapshot) dbi key-range kt))]
        (if (= path :native)
          native
          (+ native (long (small-range-count-adjustment context snapshot root dbi
                                                         key-range kt))))))))

(defn in-list? [context dbi key value kt vt]
  (let [encoded (view/encode value vt 511)]
    (with-rows context dbi [:closed key key] kt [:closed value value] vt
      #(boolean (some (fn [[_ ^bytes v]] (Arrays/equals ^bytes encoded v)) %)))))

(defrecord Submission [reservation body wal-body candidate])
(defrecord Candidate [generation base-lsn rows result aborted? wal-body pin])

(defn- preparation-context [pipeline reservation root token snapshot]
  (when (< (long (:bytes reservation)) 1024)
    (throw (ex-info "Reservation cannot hold transaction control state"
                    {:error :txlog/pending-budget-exceeded
                     :outcome :not-committed :retryable? false})))
  (state/preparation-remaining-ms reservation)
  (->Preparation (:raw pipeline) (:state pipeline) (:catalog pipeline)
                 (volatile! snapshot) token (Thread/currentThread)
                 (inc (long (:lsn root))) (volatile! root) (volatile! [])
                 (:bytes reservation) (volatile! 1024) (volatile! false)
                 (volatile! false) false))

(defn- prepare!
  "Validate a caller's candidate or run an ordered body once. The preparation
  turn protects the ordered head; collector ownership has already ended."
  [pipeline {:keys [reservation body wal-body candidate]} batch]
  (let [runtime (:state pipeline)
        head @(:head batch)
        context (assoc (preparation-context pipeline reservation head (:token batch) nil)
                       :lsn (inc (long (:lsn (:expected-root batch)))))
        accepted? (and candidate
                       (identical? (:generation candidate) (:generation runtime))
                       (= (long (:base-lsn candidate)) (long (:lsn head))))]
    (try
      (let [result (if accepted?
                     (do
                       (state/phase! :speculative-accepted candidate)
                       (vreset! (:aborted? context) (:aborted? candidate))
                       (when-not (:aborted? candidate)
                         (vreset! (:rows context) (:rows candidate))
                         (vreset! (:root context)
                                  (view/stage head (:lsn context) (:rows candidate)
                                              #(list-dbi? context %))))
                       (:result candidate))
                     (do
                       (when candidate (state/phase! :speculative-conflict candidate))
                       (body context)))
            wal-body (if accepted? (:wal-body candidate) wal-body)]
        (state/preparation-remaining-ms reservation)
        (if (or @(:aborted? context) (empty? @(:rows context)))
          (let [observed (when-let [record @(:record batch)]
                           (state/prepare-entry reservation nil nil record))]
            (group/receipt (:execute batch) result
                           #(try
                              (if observed
                                (state/await! runtime observed)
                                (state/await-prefix! runtime (:lsn head)))
                              (finally (state/release! reservation)))))
          (let [entry (if-let [record @(:record batch)]
                        (state/prepare-entry reservation @(:rows context) result record)
                        (let [entry (state/prepare-entry reservation @(:rows context) result)]
                          (vreset! (:record batch) entry)
                          entry))]
            (vreset! (:head batch) @(:root context))
            (vswap! (:entries batch) conj entry)
            (vswap! (:bodies batch) conj wal-body)
            (group/receipt (:execute batch) result #(state/await! runtime entry)))))
      (finally
        (vreset! (:finished? context) true)
        (try
          (when-let [snapshot @(:snapshot context)] (state/release-view! snapshot))
          (finally
            ;; Validation consumed the candidate. Release its detached pin
            ;; before append, so cleanup cannot replace a committed outcome.
            (when candidate (state/release-view! (:pin candidate)))))))))

(defn- prepare-group!
  "Complete a fixed collection under an already-acquired preparation token.
  Ordered bodies execute once; an explicitly replayable candidate gets at most
  one fallback. Neither native readers nor a preparation token cross caller
  threads. The caller releases the token."
  [pipeline execute submissions token]
  (let [runtime (:state pipeline)
        base @(:root runtime)
        batch {:execute execute :token token :expected-root base
               :head (volatile! base) :entries (volatile! [])
               :bodies (volatile! []) :record (volatile! nil)}
        results (mapv (fn [submission]
                        (try (prepare! pipeline submission batch)
                             (catch Throwable e (group/rejected e))))
                      submissions)
        appending (:appending pipeline)]
    (when (seq @(:entries batch)) (vreset! appending true))
    (try
      (state/append-batch! runtime @(:entries batch)
                           {:expected-root base :root @(:head batch)
                            :bodies @(:bodies batch)})
      (object-array results)
      (finally (vreset! appending false)))))

(defn- latest-reservation
  "Bound a shared preparation wait by the last live member's deadline; each
  request still keeps its own budget."
  [submissions]
  (reduce (fn [latest submission]
            (let [reservation (:reservation submission)]
              (if (> (long (:deadline reservation)) (long (:deadline latest)))
                reservation latest)))
          (:reservation (first submissions)) submissions))

(defn- submit-reserved!
  ([pipeline reservation body wal-body]
   (submit-reserved! pipeline reservation body wal-body nil))
  ([pipeline reservation body wal-body candidate]
   (try
     (let [submission (->Submission reservation body wal-body candidate)]
       (group/submit!
        (:collector pipeline)
        (fn [execute]
          ;; Collect the ready descriptors, then give up the collector lock
          ;; (without handing off) while waiting for the ordered preparation
          ;; turn. Arrivals queue for this same group and are drained below,
          ;; so one physical record covers everything ready at the turn.
          (let [extract (fn [op _] (op nil))
                initial (group/collect-submissions execute extract)
                head-reservation (latest-reservation initial)]
            (group/release-collection! execute)
            (let [token (try
                          (state/acquire-preparation! (:state pipeline) head-reservation
                                                      (state/preparation-remaining-ms
                                                       head-reservation))
                          (catch Throwable e
                            (group/acquire-collection! execute)
                            (throw e)))]
              (try
                (group/acquire-collection! execute)
                (let [late (group/collect-submissions execute extract (count initial))
                      submissions (into initial late)]
                  (group/seal! execute)
                  (vswap! (:sealed-pending pipeline) inc)
                  ;; Preparation and append run without the collector lock; the
                  ;; runner still leads, so no other group forms meanwhile.
                  (group/release-collection! execute)
                  (try
                    (prepare-group! pipeline execute submissions token)
                    (finally
                      (vswap! (:sealed-pending pipeline) dec)
                      (group/acquire-collection! execute))))
                (finally
                  (state/release-preparation! token))))))
        (constantly submission)))
     (catch Throwable e
       (when-not (.get ^java.util.concurrent.atomic.AtomicBoolean (:appended? reservation))
         (state/release! reservation))
       (throw e)))))

(defn submit!
  "Admit an explicit request once. Its body runs once under ordered preparation,
  outside collector leadership. Use submit-replayable! for concurrent caller
  preparation of a pure body. The submission budget covers admission, queueing
  and preparation; native resize retries replay only the encoded rows."
  [pipeline budget-bytes body]
  (submit-reserved! pipeline (state/reserve! (:state pipeline) budget-bytes
                                             (:preparation-timeout-ms pipeline)) body nil))

(defn- speculate!
  "Capture a matched view under a short turn, then evaluate and encode on the
  submitting thread. Return only owned data and a root pin, never a reader."
  [pipeline reservation body]
  (let [runtime (:state pipeline)
        owned (volatile! nil)]
    (try
      (let [token (state/acquire-preparation! runtime reservation
                                             (state/preparation-remaining-ms reservation))
            context (try
                      (let [root @(:root runtime)
                            context (preparation-context pipeline reservation root nil nil)]
                        (vreset! owned context)
                        (vreset! (:snapshot context)
                                 (state/capture-view runtime root
                                                     #(native-snapshot (:raw pipeline))))
                        context)
                      (finally (state/release-preparation! token)))
            result (body context)
            _ (state/preparation-remaining-ms reservation)
            aborted? @(:aborted? context)
            rows (when-not aborted? @(:rows context))
            wal-body (when (seq rows) (wal/prepare-append-body rows (:hooks runtime)))
            pin (state/detach-view! @(:snapshot context))]
        (vreset! (:snapshot context) nil)
        (->Candidate (:generation runtime) (dec (long (:lsn context)))
                     rows result aborted? wal-body pin))
      (finally
        (when-let [context @owned]
          (vreset! (:finished? context) true)
          (when-let [snapshot @(:snapshot context)] (state/release-view! snapshot)))))))

(defn submit-replayable!
  "Prepare a pure body concurrently on its submitting thread, outside collector
  and preparation ownership. A changed generation/head discards the candidate
  and executes the body once more under ordered preparation, still outside the
  collector. The body must be side-effect-free: at most two evaluations, one
  admission/reservation, and no replay after append. Native readers close on
  their owning thread before queueing; their root pin survives validation."
  [pipeline budget-bytes body]
  (let [reservation (state/reserve! (:state pipeline) budget-bytes
                                    (:preparation-timeout-ms pipeline))]
    (try
      (let [candidate (speculate! pipeline reservation body)]
        (try
          (state/phase! :speculative-prepared candidate)
          (submit-reserved! pipeline reservation body nil candidate)
          (finally (state/release-view! (:pin candidate)))))
      (catch Throwable e
        (when-not (.get ^java.util.concurrent.atomic.AtomicBoolean (:appended? reservation))
          (state/release! reservation))
        (throw e)))))

(defn submit-rows!
  "Validate and encode state-independent KV input, including its owned WAL body,
  on its caller before collection. The reservation covers both representations.
  Ordered delta installation shares the preparation turn with explicit RMW
  bodies, after collector leadership has ended. The sealed group shares one
  record and LSN; each request retains its own result and admission credits."
  [pipeline budget-bytes rows]
  (let [reservation (state/reserve! (:state pipeline) budget-bytes
                                    (:preparation-timeout-ms pipeline))
        context (->Preparation (:raw pipeline) (:state pipeline) (:catalog pipeline)
                               nil nil (Thread/currentThread) 0 nil (volatile! [])
                               budget-bytes (volatile! 1024)
                               (volatile! false) (volatile! false) true)]
    (try
      (transact! context rows)
      (let [encoded @(:rows context)
            used @(:used context)
            wal-body (when (seq encoded)
                       (wal/prepare-append-body encoded (:hooks (:state pipeline))))]
        (submit-reserved!
         pipeline reservation
         (fn [tx]
           (vreset! (:rows tx) encoded)
           (vreset! (:used tx) used)
           (vreset! (:root tx)
                    (view/stage @(:root tx) (:lsn tx) encoded #(list-dbi? tx %)))
           :transacted)
         wal-body))
      (catch Throwable e
        (when-not (.get ^java.util.concurrent.atomic.AtomicBoolean (:appended? reservation))
          (state/release! reservation))
        (throw e)))))
