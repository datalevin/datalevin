(ns ^:no-doc datalevin.rules.fixed-point
  "Shared recursive stratum iteration. Clause evaluation remains with the
  existing rule engine or its compiled operators; this kernel interprets no
  clauses, variables, sources, or provider plans."
  (:refer-clojure :exclude [run!])
  (:require [datalevin.relation :as relation])
  (:import [java.math BigInteger]
           [java.util HashSet IdentityHashMap List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:private objects-class (class (object-array 0)))

(defn- initial-allocation-bytes
  "Estimate table/schema storage exactly. Scaling the sum of all head widths
  may overflow a long even though each collection count fits in an int."
  [^long tables ^long columns]
  (let [base (+ 4096 (* 1024 tables))]
    (try
      (Math/addExact base (Math/multiplyExact 256 columns))
      (catch ArithmeticException _
        (bigint (.add (BigInteger/valueOf base)
                      (.multiply (BigInteger/valueOf 256)
                                 (BigInteger/valueOf columns))))))))

(defn run!
  "Drive the native delta iteration with caller-supplied relation operations.
  Bounded callers use serial evaluation on the pinned view's owning thread.
  after! can release superseded iteration state after merge/growth admission."
  [initial rule-ids {:keys [dependencies prepare! evaluate! merge! parallel? check! growth! after!]}]
  (loop [totals initial deltas initial]
    (when check! (check!))
    (if-not (some relation/rel-not-empty (vals deltas)) totals
      (let [context (prepare! totals deltas)
            evaluate-one
            (fn [rule-id]
              (when check! (check!))
              (when (some #(some-> (get deltas %) relation/rel-not-empty) (get dependencies rule-id))
                (when-let [rel (evaluate! context rule-id)]
                  (when (relation/rel-not-empty rel) [rule-id rel]))))
            next-deltas (into {} (keep identity)
                          (if parallel? (pmap evaluate-one rule-ids) (map evaluate-one rule-ids)))
            next-totals (merge! totals next-deltas)]
        (when growth! (growth! next-totals))
        (when after! (after! totals deltas next-totals next-deltas))
        (recur next-totals next-deltas)))))

(defn run-bounded!
  "Run complete compiled rule bodies through shared delta iteration. Initial
  relations define full head schemas; body relations may carry extra columns.
  evaluate! takes (iteration, rule-id, consume!) and must synchronously pass one
  completed body relation to consume! while its workspace is still live.
  Optional initialize! takes (rule-id, consume!) at the same handoff boundary;
  initial then defines schemas without retaining the producer's body storage.

  Hooks admit containers before allocation and own each unseen projected row
  before the next borrowed tuple is read. validate-row! bounds logical values
  before shared hashing; own-row! returns a detached Object[]. Value storage is
  the caller's lease; the returned close releases this kernel's seen/index/list
  reservations and clears its lists. Superseded lists release immediately."
  [initial rule-ids dependencies evaluate!
   {:keys [check! reserve! release! validate-row! own-row! invalid! initialize!]}]
  (check!)
  (let [release-token! release!
        tokens (IdentityHashMap.) seen (atom {}) lists (IdentityHashMap.) closed? (atom false)
        active! #(do (check!) (when @closed? (invalid!)))
        reserve! (fn [rows bytes]
                   (active!)
                   (let [token (reserve! rows bytes)] (.put tokens token true) token))
        release! (fn [token]
                   (when (.remove tokens token) (release-token! token)))
        close (fn []
                (when (compare-and-set! closed? false true)
                  (doseq [^List rows (.keySet lists)] (.clear rows))
                  (.clear lists)
                  (doseq [^HashSet keys (vals @seen)] (.clear keys))
                  (reset! seen {})
                  (let [iterator (.iterator (.keySet tokens)) error (volatile! nil)]
                    (while (.hasNext iterator)
                      (let [token (.next iterator)]
                        (.remove iterator)
                        (try (release-token! token)
                             (catch Throwable e
                               (if-let [primary @error] (.addSuppressed ^Throwable primary e)
                                   (vreset! error e))))))
                    (when-let [e @error] (throw e)))))
        schemas (volatile! nil)
        new-list! (fn [^long width]
                    (let [token (reserve! 0 (+ 1024 (* 128 width))) rows (FastList.)]
                      (.put lists rows token) rows))
        release-list! (fn [rel]
                        (let [^List rows (:tuples rel)]
                          (when-let [token (.remove lists rows)]
                            (.clear rows) (release! token))))
        delta!
        (fn [id body]
          (active!)
          (let [variables (get @schemas id) width (count variables)
                _ (when-not (and variables (instance? datalevin.relation.Relation body)
                                 (map? (:attrs body))
                                 (every? #(contains? (:attrs body) %) variables)
                                 (instance? List (:tuples body))
                                 (<= 0 (.size ^List (:tuples body)))) (invalid!))
                workspace (reserve! 0 (+ 2048 (* 256 width)))]
            (try
              (let [_ (when-not (every? #(and (integer? %) (<= 0 % Integer/MAX_VALUE))
                                       (map (:attrs body) variables)) (invalid!))
                    ^List input (:tuples body) ^HashSet keys (get @seen id)
                    ^List rows (new-list! width) probe (relation/array-lookup)
                    scratch (object-array width) indices (int-array (map (:attrs body) variables))]
                (dotimes [i (.size input)]
                  (active!)
                  (let [tuple (.get input i)]
                    (when-not (instance? objects-class tuple) (invalid!))
                    (dotimes [j width]
                      (let [index (aget indices j)]
                        (when-not (< -1 index (alength ^objects tuple)) (invalid!))
                        (aset scratch j (aget ^objects tuple index))))
                    ;; A completed body may still contain borrowed values.
                    ;; Bound their logical depth before shared key hashing.
                    (validate-row! scratch)
                    (when-not (.contains keys (relation/reset-array-lookup! probe scratch))
                      ;; Covers the immutable key/tuple, hash table growth and
                      ;; delta/total list slots before retaining a new answer.
                      (reserve! (inc (.size keys)) (+ 512 (* 64 width)))
                      (let [^objects owned (own-row! scratch)]
                        (active!)
                        (when-not (and (instance? objects-class owned) (not (identical? owned scratch))
                                       (= width (alength owned))) (invalid!))
                        (when (.add keys (relation/wrap-array owned)) (.add rows owned))))))
                (relation/relation! (zipmap variables (range)) rows))
              (finally (release! workspace)))))
        merge!
        (fn [totals deltas]
          (reduce-kv
            (fn [totals id delta]
              (active!)
              (let [before (get totals id)
                    n (+ (long (.size ^List (:tuples before)))
                         (long (.size ^List (:tuples delta))))
                    width (count (get @schemas id))
                    token (reserve! n (+ 1024 (* 16 n) (* 128 width)))
                    rel (relation/sum-rel before delta)]
                (.put lists (:tuples rel) token)
                (assoc totals id rel))) totals deltas))
        after!
        (fn [totals deltas next-totals next-deltas]
          (let [keep (IdentityHashMap.)]
            (doseq [rel (concat (vals next-totals) (vals next-deltas))] (.put keep (:tuples rel) true))
            (doseq [rel (concat (vals totals) (vals deltas))]
              (active!)
              (when-not (.containsKey keep (:tuples rel)) (release-list! rel)))))]
    (try
      (let [tables (long (+ (count initial) (count rule-ids) (count dependencies)))
            columns (long (reduce (fn [^long total rel]
                                    (+ total (long (count (:attrs rel)))))
                                  0 (vals initial)))]
        (reserve! 0 (initial-allocation-bytes tables columns)))
      (when-not (and (vector? rule-ids) (= (count rule-ids) (count (set rule-ids)))
                     (= (set rule-ids) (set (keys initial)) (set (keys dependencies)))
                     (every? #(every? (set rule-ids) %) (vals dependencies))) (invalid!))
      (vreset! schemas (into {} (map (fn [[id rel]] [id (mapv key (sort-by val (:attrs rel)))])) initial))
      (doseq [id rule-ids]
        (reserve! 0 (+ 2048 (* 256 (count (get @schemas id)))))
        (swap! seen assoc id (HashSet.)))
      (let [consume-one!
            (fn [id produce!]
              (let [accepting? (volatile! true) consumed? (volatile! false) result (volatile! nil)]
                (try
                  (produce! (fn [body]
                              (active!)
                              (when-not (and @accepting? (not @consumed?)) (invalid!))
                              (vreset! consumed? true)
                              (vreset! result (delta! id body))))
                  (when-not @consumed? (invalid!))
                  @result
                  (finally (vreset! accepting? false)))))
            initial (into {} (map (fn [id]
                                    [id (if initialize!
                                          (consume-one! id #(initialize! id %))
                                          (delta! id (get initial id)))])) rule-ids)
            result
            (run! initial rule-ids
              {:dependencies dependencies :check! active! :parallel? false
               :prepare! (fn [totals deltas] {:totals totals :deltas deltas})
               :evaluate!
               (fn [iteration id]
                 (let [rel (consume-one! id #(evaluate! iteration id %))]
                   (if (relation/rel-not-empty rel) rel (do (release-list! rel) nil))))
               :merge! merge! :after! after!})]
        {:relations result :check! active! :close close})
      (catch Throwable e
        (try (close) (catch Throwable secondary (.addSuppressed e secondary)))
        (throw e)))))
