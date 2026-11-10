(ns ^:no-doc datalevin.query.source.cursor
  "Exhaustive execution of captured v2 access plans. Provider batches are
  borrowed only during delivery; a retaining operator must own its output."
  (:require [datalevin.join :as join]
            [datalevin.parser]
            [datalevin.query.access :as access]
            [datalevin.query.source :as source]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.relation :as relation])
  (:import [datalevin.utl QueryKey]
           [java.io Closeable]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private common-keys
  [:spi-version :features :query-id :authorization :consistency :control :limits])

(def ^:private object-array-class (Class/forName "[Ljava.lang.Object;"))

(defn- ensure! [ok phase]
  (when-not ok (fail! :query-source/invalid phase)))

(defn- nonnegative? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- captured!
  "Execution identity comes from the captured request, never caller overrides.
  The caller has already validated all descriptors, alternatives and regions."
  [runtime group plan dependent?]
  (let [options (get-in plan [:path :options])
        {:keys [request alternative]} (:capture options)
        entry (some #(when (= (:symbol %) (:source-symbol options)) %) (:entries group))
        descriptor (:descriptor entry)
        columns #(mapv (fn [c] (select-keys c [:variable :type :nullable?])) %)]
    (control/check-active! (:control runtime))
    (ensure! (and (= :query-source (get-in plan [:path :method]))
                  (identical? (:control runtime) (get-in group [:context :control]))
                  (= (:query-id runtime) (get-in group [:context :query-id]))
                  (= 2 (get-in group [:context :spi-version]))
                  (= (:context group) (select-keys request common-keys))
                  (satisfies? source/IQuerySourceSession (:session group))
                  entry (identical? descriptor (:source-descriptor request))
                  (= (:source-symbol options) (:source-symbol request))
                  (= (:region-id options) (:region-id request))
                  (nonnegative? (:region-id options))
                  (= (:scope-id options) (:scope-id request) (:scope-id alternative)
                     (get-in descriptor [:scope :id]))
                  (= :all-bindings (:completion alternative))
                  (= (:alternative-id options) (:id alternative))
                  (some? (:plan alternative))
                  (= (:requires options) (columns (:requires alternative)))
                  (= (:columns options) (columns (:columns alternative)))
                  (vector? (:requires alternative))
                  (= dependent? (boolean (seq (:requires alternative))))
                  (identical? (:consumption options) (:consumption group))) :open)
    {:alternative alternative
     :request (cond-> (assoc (:context group)
                            :alternative-id (:id alternative) :plan (:plan alternative)
                            :source-symbol (:symbol entry)
                            :definition-fingerprint (:definition-fingerprint descriptor)
                            :scope-id (:scope-id options) :region-id (:region-id options))
                (contains? (:properties alternative) :consumption-group)
                (assoc :consumption-group (get-in alternative [:properties :consumption-group])))}))

(defn- deliver-batch!
  [runtime columns dependent? request batch deliver!]
  (batch/accept! runtime columns dependent? request batch
    (fn [batch]
      (try
        (deliver! batch)
        (control/check-active! (:control runtime))
        (finally
          (when-let [token (:reservation batch)] (control/release! token)))))))

(defn- finish! [runtime primary close]
  (try (close)
       (catch Throwable e
         (if primary
           (.addSuppressed ^Throwable primary (rt/redacted-error runtime :close e))
           (let [error (rt/redacted-error runtime :close e)
                 cancelled (try (control/check-active! (:control runtime)) nil
                                (catch Throwable active
                                  (rt/redacted-error runtime (or (:phase (ex-data active)) :execution) active)))
                 error (if cancelled (doto ^Throwable cancelled (.addSuppressed error)) error)]
             (control/cancel! (:control runtime) (select-keys (ex-data error) [:error :phase]))
             (throw error))))))

(defn- read-open!
  [runtime group alternative request dependent? deliver!]
  (let [ctl (:control runtime)
        trace (diagnostics/cursor! runtime request dependent?)
        permission (permissions/reserve-open! (:consumption group) alternative request)
        close-permission (rt/own! runtime #(.close ^Closeable permission))
        close (volatile! close-permission)
        primary (volatile! nil)]
    (try
      (let [cursor (rt/invoke runtime :open #(source/-open-cursor (:session group) request))]
        (ensure! (satisfies? source/IQuerySourceCursor cursor) :open)
        ;; Own the returned resource before checking a cancellation that won
        ;; the handoff race. Keep concurrency reserved through provider close.
        (vreset! close (rt/own! runtime
                       #(try (source/-close-cursor! cursor) (finally (close-permission)))))
        (control/check-active! ctl)
        (let [limits (control/limits ctl)
              fetch {:control ctl :max-rows (:batch-rows limits) :max-bytes (:batch-bytes limits)}]
          (loop []
            (let [batch (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch))]
              (deliver-batch! runtime (:columns alternative) dependent? fetch batch deliver!)
              (diagnostics/batch! runtime trace (:row-count batch) (:byte-count batch))
              (when-not (:exhausted? batch) (recur))))
        (when (:diagnostics runtime)
          (diagnostics/metrics! runtime trace (rt/invoke runtime :metrics #(source/-cursor-metrics cursor))))))
      (catch Throwable e
        (let [error (rt/redacted-error runtime (or (:phase (ex-data e)) :execution) e)]
          (vreset! primary error)
          (control/cancel! ctl (select-keys (ex-data error) [:error :phase]))
          (throw error)))
      (finally (finish! runtime @primary @close)))))

(defn- emit-all! [runtime emit! & args]
  (control/check-active! (:control runtime))
  (when (reduced? (apply emit! args)) (fail! :query-source/incomplete :execution))
  (control/check-active! (:control runtime)))

(defn scan!
  "Exhaust one input-free access. emit! borrows one row until it returns.
  This path constructs no binding IDs, input vectors, or association indexes."
  [runtime group plan emit!]
  (let [{:keys [alternative request]} (captured! runtime group plan false)]
    (rt/with-context runtime (select-keys request [:source-symbol :region-id :alternative-id])
      (fn [runtime]
        (read-open! runtime group alternative request false
          (fn [batch]
            (doseq [row (:rows batch)] (emit-all! runtime emit! row))))))))

(defn lookup!
  "Collect all outer bindings before any lookup, then exhaust each legal
  distinct-key batch once. ids is the invocation's shared lazy ID space.
  emit! borrows [outer row]; retaining operators must copy/account separately."
  [runtime group plan ids outer-bindings emit!]
  (let [{:keys [alternative request]} (captured! runtime group plan true)]
    (rt/with-context runtime (select-keys request [:source-symbol :region-id :alternative-id])
      (fn [runtime]
        (with-open [^Closeable keys (binding/collect! runtime @ids (:requires alternative))]
          (doseq [outer outer-bindings]
            (control/check-active! (:control runtime))
            (ensure! (and (value/plain-map? outer)
                          (every? #(contains? outer (:variable %)) (:requires alternative))) :open)
            (binding/add! keys (mapv #(get outer (:variable %)) (:requires alternative)) outer))
          (let [opens (:opens (permissions/planning-permission (:consumption group) alternative))
                n (binding/seal! keys (:constraints alternative) opens)]
            (dotimes [i n]
              (read-open! runtime group alternative (assoc request :bindings (binding/batch-inputs keys i)) true
                (fn [batch]
                  (binding/consume! keys i (:columns alternative) (:rows batch) (:binding-ids batch)
                                    (:exhausted? batch) #(emit-all! runtime emit! %1 %2)))))
            (when-not (binding/complete? keys) (fail! :query-source/incomplete :execution))))))))

(defn- tuple-bindings! [runtime columns tuple]
  (let [array? (instance? object-array-class tuple)
        n (if array? (alength ^objects tuple) (when (vector? tuple) (count tuple)))]
    (ensure! (= (count columns) n) :execution)
    ;; Reserve before allocating the map, then detach mutable leaves before an
    ;; upstream iterator can advance/reuse its tuple or value buffers.
    (let [token (control/reserve! (:control runtime) {:retained-bytes (+ 256 (* 64 (long n)))})]
      (try
        (let [check! #(control/check-active! (:control runtime))
              bindings (persistent!
                         (reduce-kv (fn [m i column]
                                      (check!)
                                      (assoc! m column (if array? (aget ^objects tuple (long i)) (nth tuple i))))
                                    (transient {}) columns))
              owned-token (rt/charge-copy! runtime bindings)]
          (try {:token owned-token :bindings (value/owned-copy bindings check!)}
               (catch Throwable e (control/release! owned-token) (throw e))))
        (finally (control/release! token))))))

(defn- execute-shared!
  [runtime group path demand bounds work ids input-cols output-cols tuples]
  (let [ctl (:control runtime) plan {:path path}
        dependent? (boolean (seq (get-in path [:options :requires])))
        columns (get-in path [:options :columns])
        tokens (volatile! []) success? (volatile! false)
        own! #(do (vswap! tokens conj %) %)
        check! #(control/check-active! ctl)]
    (try
      ;; This adapter implements complete regions only. Final windows belong
      ;; downstream, and cannot authorize a prefix of a dependent access.
      (ensure! (and (= :exact (:quality demand)) (empty? (:ordering demand))
                    (zero? (long (or (:offset bounds) 0))) (nil? (:limit bounds))
                    (nil? (:required-count bounds)) (nil? (:max-candidates work))) :execution)
      (captured! runtime group plan dependent?)
      (own! (control/reserve! ctl {:retained-bytes 2048}))
      (let [output (FastList.)
            input-tokens (volatile! [])
            ;; Realize maps once under admission, including a streaming/reused
            ;; input tuple. Collector deduplication never reads a mutable outer
            ;; tuple again. Input-free cross products also keep owned witnesses.
            outer-input? (or (seq input-cols) (some? tuples))
            outers (when outer-input?
                     (eduction (map (fn [tuple]
                                      (let [{:keys [bindings token]} (tuple-bindings! runtime input-cols tuple)]
                                        (vswap! input-tokens conj token)
                                        bindings))) tuples))
            save! (fn [outer row]
                    (check!)
                    (let [scratch (control/reserve! ctl
                                    {:retained-bytes (+ 256 (* 96 (+ (count input-cols) (count columns))))})]
                      (try
                        (let [returned (zipmap (map :variable columns) row)]
                          (when (every? (fn [[k x]]
                                          (check!)
                                          (or (not (contains? outer k)) (QueryKey/equals (get outer k) x)))
                                        returned)
                            (let [values (mapv (merge outer returned) output-cols)
                                  token (rt/charge-copy! runtime values)]
                              (own! token)
                              ;; Includes the object array, list slot and the
                              ;; transient owned vector in the conservative copy.
                              (.add output (object-array (value/owned-copy values check!))))))
                        (finally (control/release! scratch)))))]
        (try
          (let [result
                (cond
                  dependent? (do (lookup! runtime group plan ids outers save!) output)
                  outer-input?
                  (let [owned (into [] outers)]
                    (if (empty? owned) output
                      (let [left-rows (FastList.) right-rows (FastList.)
                            right-cols (mapv :variable columns)]
                        (own! (control/reserve! ctl
                                {:retained-bytes
                                 (value/add-charge 2048
                                   (value/multiply-charge (count owned)
                                     (+ 128 (* 16 (count input-cols)))))}))
                        (doseq [outer owned]
                          (check!)
                          (.add left-rows (object-array (mapv outer input-cols))))
                        ;; The shared join borrows these detached values. Keep
                        ;; their reservations through the invocation, including
                        ;; after the provider releases its final batch.
                        (doseq [token @input-tokens] (own! token))
                        (vreset! input-tokens [])
                        (scan! runtime group plan
                          (fn [row]
                            (own! (rt/charge-copy! runtime row))
                            (.add right-rows (object-array (value/owned-copy row check!)))))
                        (let [joined (join/hash-join-bounded
                                       (relation/relation! (zipmap input-cols (range)) left-rows)
                                       (relation/relation! (zipmap right-cols (range)) right-rows)
                                       {:check! check!
                                        :reserve! (fn [^long rows bytes]
                                                    (when (> rows Integer/MAX_VALUE)
                                                      (fail! :query-source/limit :reserve))
                                                    (control/reserve! ctl {:retained-bytes bytes}))
                                        :retain! own! :release! control/release!})]
                          (ensure! (= output-cols (mapv key (sort-by val (:attrs joined)))) :execution)
                          (:tuples joined)))))
                  :else (do (scan! runtime group plan #(save! {} %)) output))]
            (check!)
            (let [held @tokens]
              ;; Shared operators may retain these tuples after cursor/session
              ;; close. Their independent reservations last through invocation.
              (rt/own! runtime #(doseq [token held] (control/release! token))))
            (vreset! success? true)
            result)
          (finally (doseq [token @input-tokens] (control/release! token)))))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))

(defn method
  "Shared AccessStep implementation for already acquired/captured v2 paths.
  Pass the SAME delay of binding/id-space! to all methods in one invocation.
  No source detection, planning callback, or ID space is created by this factory."
  [runtime group ids]
  (reify
    access/IAccessMethod
    (-access-plans [_ _] (fail! :query-source/unsupported :planning))
    (-open-access [_ _ _ _ _ _] (fail! :query-source/unsupported :open))
    (-frontier-satisfies? [_ _ _ _ _] false)
    access/IBatchedAccessMethod
    (-execute-batched-access [_ path demand bounds work _ input-cols output-cols tuples]
      (execute-shared! runtime group path demand bounds work ids input-cols output-cols tuples))))
