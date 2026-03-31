;;
;; Copyright (c) Huahai Yang, Nikita Prokopov. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.query-optimizer
  "Optimizer helpers extracted from datalevin.query."
  (:require
   [clojure.set :as set]
   [clojure.core.reducers :as rd]
   [clojure.walk :as w]
   [datalevin.constants :as c]
   [datalevin.db :as db]
   [datalevin.interface :refer [av-size]]
   [datalevin.lmdb :as l]
   [datalevin.parser :as dp]
   [datalevin.query.optimizer.graph :as qog]
   [datalevin.pipe :as p]
   [datalevin.query.optimizer.range :as qor]
   [datalevin.query-util :as qu]
   [datalevin.util :as u :refer [cond+ raise conjv concatv map+]])
  (:import
   [java.util List]
   [java.util.concurrent ConcurrentHashMap]
   [datalevin.db DB]
   [datalevin.storage Store]
   [datalevin.utl LRUCache]
   [datalevin.parser And BindColl BindIgnore BindScalar BindTuple Constant
    DefaultSrc Function Or Variable Pattern Predicate Not RuleExpr]
   [org.eclipse.collections.impl.list.mutable FastList]))

(def ^:dynamic *optimizer-deps* nil)

(defn- dep [k]
  (or (get *optimizer-deps* k)
      (raise "Missing query optimizer dependency " k {:dependency k})))

(defn- resolve-pattern-lookup-refs [source pattern]
  ((dep :resolve-pattern-lookup-refs) source pattern))

(defn- make-call [f]
  ((dep :make-call) f))

(defn- resolve-pred [f context]
  ((dep :resolve-pred) f context))

(defn- or-join-execute-link
  [db sources rules tuples clause bound-var bound-idx free-vars tgt-attr]
  ((dep :or-join-execute-link)
   db sources rules tuples clause bound-var bound-idx free-vars tgt-attr))

(defn- execute-steps [context db steps]
  ((dep :execute-steps) context db steps))

(defn- resolve-clause [context clause]
  ((dep :resolve-clause) context clause))

(defn- -sample [step db source]
  ((dep :step-sample) step db source))

(defn- -execute [step db source]
  ((dep :step-execute) step db source))

(defn- -type [step]
  ((dep :step-type) step))

(defn- map->Clause [m]
  ((dep :map->Clause) m))

(defn- map->Node [m]
  ((dep :map->Node) m))

(defn- mk-link [type tgt var attrs attr]
  ((dep :link) type tgt var attrs attr))

(defn- mk-or-join-link [type tgt clause bound-var free-vars tgt-attr source]
  ((dep :or-join-link) type tgt clause bound-var free-vars tgt-attr source))

(defn- map->init-step [m]
  ((dep :map->InitStep) m))

(defn- mk-merge-scan-step
  [index attrs-v vars in out cols strata seen-or-joins result sample]
  ((dep :merge-scan-step)
   index attrs-v vars in out cols strata seen-or-joins result sample))

(defn- mk-link-step [type index attr var fidx in out cols strata seen-or-joins]
  ((dep :link-step) type index attr var fidx in out cols strata seen-or-joins))

(defn- mk-hash-join-step
  [link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size]
  ((dep :hash-join-step)
   link link-e in out in-cols cols strata seen-or-joins tgt-steps in-size tgt-size))

(defn- mk-or-join-step
  [clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins]
  ((dep :or-join-step)
   clause bound-var bound-idx free-vars tgt tgt-attr sources rules in out cols strata seen-or-joins))

(defn- mk-not-join-step
  [clause vars sources rules in out cols strata seen-or-joins]
  ((dep :not-join-step) clause vars sources rules in out cols strata seen-or-joins))

(defn- make-plan [steps cost size recency]
  ((dep :plan) steps cost size recency))

(defn- plan-cache ^LRUCache []
  ^LRUCache (dep :plan-cache))

(declare estimate-hash-join-cost)

;; optimizer

(defn- or-join-var?
  [clause s]
  (and (list? clause)
       (= 'or-join (first clause))
       (some #(= % s) (tree-seq sequential? seq (second clause)))))

(defn- not-join-clause?
  [clause]
  (and (sequential? clause)
       (not (vector? clause))
       (= 'not-join
          (if (qu/source? (first clause))
            (second clause)
            (first clause)))))

(defn- get-not-join-vars
  [clause]
  (let [clause (if (qu/source? (first clause)) (next clause) clause)
        [_ vars & _] clause]
    (into [] (filter qu/binding-var?) vars)))

(defn- get-not-join-source
  [clause]
  (if (qu/source? (first clause)) (first clause) '$))

(defn- clause-source-symbol
  [source]
  (if (instance? DefaultSrc source) '$ (:symbol source)))

(defn- not-join-optimizable?
  "Conservative check for planner-handled not-join.
   Easy cases only: explicit not-join form, non-empty join vars, pattern-only
   body, single source, and all join vars used in the body."
  [sources parsed-clause orig-clause]
  (when (and (instance? Not parsed-clause) (not-join-clause? orig-clause))
    (let [vars             (into []
                                 (comp (map :symbol) (filter qu/binding-var?))
                                 (:vars parsed-clause))
          clauses          (:clauses parsed-clause)
          src              (get-not-join-source orig-clause)
          pattern-only?    (every? #(instance? Pattern %) clauses)
          clause-sources   (into #{} (map #(clause-source-symbol (:source %)))
                                 clauses)
          body-vars        (qu/collect-vars clauses)
          all-vars-used?   (set/subset? (set vars) body-vars)
          searchable-src?  (when-let [db (get sources src)]
                             (db/-searchable? db))]
      (when (and searchable-src?
                 (seq vars)
                 pattern-only?
                 (= 1 (count clause-sources))
                 (= src (first clause-sources))
                 all-vars-used?)
        orig-clause))))

(defn- plugin-inputs*
  [parsed-q inputs]
  (let [qins    (:qin parsed-q)
        finds   (tree-seq sequential? seq (:qorig-find parsed-q))
        owheres (:qorig-where parsed-q)
        to-rm   (keep-indexed
                  (fn [i qin]
                    (let [v (:variable qin)
                          s (:symbol v)
                          val (nth inputs i)]
                      (when (and (instance? BindScalar qin)
                                 (instance? Variable v)
                                 ;; keep sequential inputs as variables so
                                 ;; function calls don't eagerly evaluate them
                                 (not (sequential? val))
                                 (not (some #(= s %) finds))
                                 (not (some #(or-join-var? % s) owheres)))
                        [i s])))
                  qins)
        rm-idxs (into #{} (map first) to-rm)
        smap    (reduce (fn [m [i s]] (assoc m s (nth inputs i))) {} to-rm)]
    [(assoc parsed-q
            :qwhere (reduce-kv
                      (fn [ws s v]
                        (w/postwalk
                          (fn [e]
                            (if (and (instance? Variable e)
                                     (= s (:symbol e)))
                              (Constant. v)
                              e))
                          ws))
                      (:qwhere parsed-q) smap)
            :qorig-where (w/postwalk-replace smap owheres)
            :qin (u/remove-idxs rm-idxs qins))
     (u/remove-idxs rm-idxs inputs)]))

(defn plugin-inputs
  "optimization that plugs simple value inputs into where clauses"
  [parsed-q inputs]
  (let [ins (:qin parsed-q)
        cb  (count ins)
        cv  (count inputs)]
    (cond
      (< cb cv) (raise "Extra inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      (> cb cv) (raise "Too few inputs passed, expected: "
                       (mapv #(:source (meta %)) ins) ", got: " cv
                       {:error :query/inputs :expected ins :got inputs})
      :else     (plugin-inputs* parsed-q inputs))))

(defn- var-symbol
  [v]
  (when (instance? Variable v)
    (:symbol v)))

(defn- collect-var-usage
  [qwhere]
  (let [counts    (volatile! {})
        kinds     (volatile! {})
        protected (volatile! #{})]
    (letfn [(note-var! [sym kind]
              (when (qu/binding-var? sym)
                (vswap! counts update sym (fnil inc 0))
                (vswap! kinds update sym (fnil conj #{}) kind)))
            (protect-var! [sym]
              (when (qu/free-var? sym)
                (vswap! protected conj sym)))
            (note-var [v kind]
              (when-let [sym (var-symbol v)]
                (note-var! sym kind)))
            (protect-var [v]
              (when-let [sym (var-symbol v)]
                (protect-var! sym)))
            (protect-vars-in-form [form]
              (doseq [sym (qu/collect-vars form)]
                (protect-var! sym)))
            (protect-arg-vars [arg]
              (when (instance? Constant arg)
                (protect-vars-in-form (:value arg))))
            (walk-binding [binding]
              (cond
                (instance? BindScalar binding)
                (note-var (:variable binding) :binding)

                (instance? BindTuple binding)
                (doseq [b (:bindings binding)]
                  (walk-binding b))

                (instance? BindColl binding)
                (walk-binding (:binding binding))

                :else nil))
            (walk-clause [clause]
              (cond
                (instance? Pattern clause)
                (doseq [el (:pattern clause)]
                  (note-var el :pattern))

                (instance? Function clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg))
                  (walk-binding (:binding clause)))

                (instance? Predicate clause)
                (do
                  (protect-var (:fn clause))
                  (doseq [arg (:args clause)]
                    (protect-var arg)
                    (protect-arg-vars arg)))

                (instance? RuleExpr clause)
                (doseq [arg (:args clause)]
                  (protect-var arg))

                (instance? And clause)
                (doseq [c (:clauses clause)]
                  (walk-clause c))

                (instance? Or clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                (instance? Not clause)
                (doseq [c (:clauses clause)]
                  (protect-vars-in-form c)
                  (walk-clause c))

                :else nil))]
      (doseq [c qwhere] (walk-clause c))
      {:counts @counts :kinds @kinds :protected @protected})))

(defn unused-var-replacements
  [parsed-q]
  (let [find-vars (set (dp/find-vars (:qfind parsed-q)))
        with-vars (set (map :symbol (or (:qwith parsed-q) [])))
        in-vars   (set (map :symbol (dp/collect-vars-distinct (:qin parsed-q))))
        used      (set/union find-vars with-vars in-vars)
        {:keys [counts kinds protected]}
        (collect-var-usage (:qwhere parsed-q))]
    (into {}
          (keep (fn [[sym n]]
                  (when (and (= 1 n)
                             (not (contains? used sym))
                             (not (contains? protected sym)))
                    (let [kind (get kinds sym)]
                      [sym (if (contains? kind :binding)
                             '_
                             (qu/placeholder-sym sym))]))))
          counts)))

(defn- replace-unused-vars-form
  [form replacements]
  (letfn [(walk [form]
            (cond
              (qu/quoted-form? form) form
              (symbol? form)         (get replacements form form)
              (map? form)            (into (empty form)
                                           (map (fn [[k v]]
                                                  [(walk k) (walk v)]))
                                           form)
              (seq? form)            (apply list (map walk form))
              (coll? form)           (into (empty form) (map walk) form)
              :else                  form))]
    (walk form)))

(defn rewrite-unused-vars
  [{:keys [parsed-q] :as context}]
  (let [replacements (unused-var-replacements parsed-q)]
    (if (empty? replacements)
      context
      (let [qorig-where  (mapv #(replace-unused-vars-form % replacements)
                               (:qorig-where parsed-q))
            qwhere       (dp/parse-where qorig-where)]
        (assoc context :parsed-q
               (assoc parsed-q :qorig-where qorig-where :qwhere qwhere))))))

(def combine-ranges qor/combine-ranges)
(def flip-ranges qor/flip-ranges)
(def intersect-ranges qor/intersect-ranges)
(def ^:private add-pred qor/add-pred)
(def ^:private range->inequality qor/range->inequality)

(defn- activate-var-pred
  [var clause]
  (qor/activate-var-pred {:make-call make-call
                          :resolve-pred resolve-pred}
                         var clause))

(def build-graph qog/build-graph)

(defn- estimate-round [x]
  (let [v (Math/ceil (double x))]
    (if (>= v (double Long/MAX_VALUE))
      Long/MAX_VALUE
      (long v))))

(defn- attr-var [{:keys [var]}] (or var '_))

(defn- nillify [v] (if (or (identical? v c/v0) (identical? v c/vmax)) nil v))

(defn- range->start-end [[[_ lv] [_ hv]]] [(nillify lv) (nillify hv)])

(defn- range-count
  [db attr ranges ^long cap]
  (if (identical? ranges :empty-range)
    0
    (unreduced
      (reduce
        (fn [^long sum range]
          (let [s (+ sum (let [[lv hv] (range->start-end range)]
                           ^long (db/-index-range-size db attr lv hv)))]
            (if (< s cap) s (reduced cap))))
        0 ranges))))

(defn- count-node-datoms
  [^DB db {:keys [free bound] :as node}]
  (let [store (.-store db)]
    (reduce
      (fn [{:keys [mcount] :as node} [k i {:keys [attr val range]}]]
        (let [^long c (cond
                        (some? val) (av-size store attr val)
                        range       (range-count db attr range mcount)
                        :else       (db/-count db [nil attr nil] mcount))]
          (cond
            (zero? c)          (reduced (assoc node :mcount 0))
            (< c ^long mcount) (-> node
                                   (assoc-in [k i :count] c)
                                   (assoc :mcount c :mpath [k i]))
            :else              (assoc-in node [k i :count] c))))
      (assoc node :mcount Long/MAX_VALUE)
      (let [flat (fn [k m] (map-indexed (fn [i clause] [k i clause]) m))]
        (concat (flat :bound bound) (flat :free free))))))

(defn- count-known-e-datoms
  [db e {:keys [free] :as node}]
  (u/reduce-indexed
    (fn [{:keys [mcount] :as node} {:keys [attr]} i]
      (let [^long c (db/-count db [e attr nil] mcount)]
        (cond
          (zero? c)          (reduced (assoc node :mcount 0))
          (< c ^long mcount) (-> node
                                 (assoc-in [:free i :count] c)
                                 (assoc :mcount c :mpath [:free i]))
          :else              (assoc-in node [:free i :count] c))))
    (assoc node :mcount Long/MAX_VALUE) free))

(defn- count-datoms
  [db e node]
  (unreduced (if (int? e)
               (count-known-e-datoms db e node)
               (count-node-datoms db node))))

(defn- add-back-range
  [v {:keys [pred range]}]
  (if range
    (reduce
      (fn [p r]
        (if r
          (add-pred p (activate-var-pred v (range->inequality v r)) true)
          p))
      pred range)
    pred))

(defn- attrs-vec
  [attrs preds skips fidxs]
  (mapv (fn [a p f]
          [a (cond-> {:pred p :skip? false :fidx nil}
               (skips a) (assoc :skip? true)
               f         (assoc :fidx f :skip? true))])
        attrs preds fidxs))

(defn- aid [db] #(((db/-schema db) %) :db/aid))

(defn- init-steps
  [db e node single?]
  (let [{:keys [bound free mpath mcount]}            node
        {:keys [attr var val range pred] :as clause} (get-in node mpath)

        know-e? (int? e)
        no-var? (or (not var) (qu/placeholder? var))

        init (cond-> (map->init-step
                       {:attr attr :vars [e] :out [e]
                        :mcount (:count clause)})
               var     (assoc :pred pred
                              :vars (cond-> [e]
                                      (not no-var?) (conj var))
                              :range range)
               (some? val) (assoc :val val)
               know-e? (assoc :know-e? true)
               true    (#(let [vars (:vars %)]
                           (assoc % :cols (if (= 1 (count vars))
                                            [e]
                                            [e #{attr var}])
                                  :strata [(set vars)]
                                  :seen-or-joins #{})))

               (not single?)
               (#(if (< ^long c/init-exec-size-threshold ^long mcount)
                   (assoc % :sample (-sample % db nil))
                   (assoc % :result (-execute % db nil)))))]
    (cond-> [init]
      (< 1 (+ (count bound) (count free)))
      (conj
        (let [[k i]   mpath
              bound1  (mapv (fn [{:keys [val] :as b}]
                              (-> b
                                  (update :pred add-pred #(= val %))
                                  (assoc :var (gensym "?bound"))))
                            (if (= k :bound) (u/vec-remove bound i) bound))
              all     (->> (concatv bound1
                                    (if (= k :free) (u/vec-remove free i) free))
                           (sort-by (fn [{:keys [attr]}] ((aid db) attr))))
              attrs   (mapv :attr all)
              vars    (mapv attr-var all)
              skips   (cond-> (set (sequence
                                     (comp (map (fn [a v]
                                               (when (or (= v '_)
                                                         (qu/placeholder? v))
                                                 a)))
                                        (remove nil?))
                                     attrs vars))
                        no-var? (conj attr))
              preds   (mapv add-back-range vars all)
              attrs-v (attrs-vec attrs preds skips (repeat nil))
              cols    (into (:cols init)
                            (sequence
                              (comp (map (fn [a v] (when-not (skips a) #{a v})))
                                 (remove nil?))
                              attrs vars))
              strata  (conj (:strata init) (set vars))
              ires    (:result init)
              isp     (:sample init)
              step    (mk-merge-scan-step 0 attrs-v vars [e] [e] cols strata
                                          #{} nil nil)]
          (cond-> step
            ires (assoc :result (-execute step db ires))
            isp  (assoc :sample (-sample step db isp))))))))

(defn- n-items
  [attrs-v k]
  (reduce
    (fn [^long c [_ m]] (if (m k) (inc c) c))
    0 attrs-v))

(defn- estimate-scan-v-size
  [^long e-size steps]
  (cond+
    (= (count steps) 1) e-size ; no merge step

    :let [{:keys [know-e?] res1 :result sp1 :sample} (first steps)
          {:keys [attrs-v result sample]} (peek steps)]

    know-e? (count attrs-v)

    :else
    (estimate-round
      (* e-size (double
                  (cond
                    result (let [s (.size ^List result)]
                             (if (< 0 s)
                               (/ s (.size ^List res1))
                               c/magic-scan-ratio))
                    sample (let [s (.size ^List sample)]
                             (if (< 0 s)
                               (/ s (.size ^List sp1))
                               c/magic-scan-ratio))))))))

(defn- factor
  [magic ^long n]
  (if (zero? n) 1 ^long (estimate-round (* ^double magic n))))

(defn- estimate-scan-v-cost
  [{:keys [attrs-v vars]} ^long size]
  (* size
     ^double c/magic-cost-merge-scan-v
     ^long (factor c/magic-cost-var (count vars))
     ^long (factor c/magic-cost-pred (n-items attrs-v :pred))
     ^long (factor c/magic-cost-fidx (n-items attrs-v :fidx))))

(defn- estimate-base-cost
  [{:keys [mcount]} steps]
  (let [{:keys [pred]} (first steps)
        init-cost      (estimate-round
                         (cond-> (* ^double c/magic-cost-init-scan-e
                                    ^long mcount)
                           pred (* ^double c/magic-cost-pred)))]
    (if (< 1 (count steps))
      (+ ^long init-cost ^long (estimate-scan-v-cost (peek steps) mcount))
      init-cost)))

(defn- base-plan
  ([db nodes e]
   (base-plan db nodes e false))
  ([db nodes e single?]
   (let [node   (get nodes e)
         mcount (:mcount node)]
     (when-not (zero? ^long mcount)
       (let [isteps (init-steps db e node single?)]
         (if single?
           (make-plan isteps nil nil 0)
           (make-plan isteps
                      (estimate-base-cost node isteps)
                      (estimate-scan-v-size mcount isteps)
                      0)))))))

(defn writing? [db] (l/writing? (.-lmdb ^Store (.-store ^DB db))))

(defn- update-nodes
  [db nodes]
  (if (= (count nodes) 1)
    (let [[e node] (first nodes)] {e (count-datoms db e node)})
    (let [f (bound-fn [e] [e (count-datoms db e (get nodes e))])]
      (into {} (if (writing? db)
                 (map f (keys nodes))
                 (map+ f (keys nodes)))))))

(defn- build-base-plans
  [db nodes component]
  (let [f (bound-fn [e] [[e] (base-plan db nodes e)])]
    (into {} (if (writing? db)
               (map f component)
               (map+ f component)))))

(defn find-index
  [a-or-v cols]
  (when a-or-v
    (u/index-of (fn [x] (if (set? x) (x a-or-v) (= x a-or-v))) cols)))

(defn- merge-scan-step
  [db last-step index new-key new-steps]
  (let [in       (:out last-step)
        out      (if (set? in) (set new-key) new-key)
        lcols    (:cols last-step)
        lstrata  (:strata last-step)
        ncols    (:cols (peek new-steps))
        [s1 s2]  new-steps
        val1     (:val s1)
        [_ v1]   (:vars s1)
        a1       (:attr s1)
        ip       (cond-> (add-back-range v1 s1)
                   (some? val1) (add-pred #(= % val1)))
        attrs-v2 (:attrs-v s2)
        get-a    (fn [coll] (some #(when (keyword? %) %) coll))
        [attrs-v vars cols]
        (reduce
          (fn [[attrs-v vars cols] col]
            (let [v (some #(when (symbol? %) %) col)]
              (if (and ip (= v v1))
                [attrs-v vars cols]
                (let [a (get-a col)
                      p (some #(when (= a (first %)) (:pred (peek %))) attrs-v2)]
                  (if-let [f (find-index v lcols)]
                    [(conj attrs-v [a {:pred p :skip? true :fidx f}]) vars cols]
                    [(conj attrs-v [a {:pred  p
                                       :skip? (if (some #(when (= a (first %))
                                                           (:skip? (peek %)))
                                                        attrs-v2)
                                                true false)
                                       :fidx  nil}])
                     (conj vars v) (conj cols col)])))))
          (if (or ip (nil? v1))
            [[[a1 {:pred  ip
                   :skip? (if (and v1 (find-index v1 ncols)) false true)
                   :fidx  nil}]]
             (if v1 [v1] [])
             (if v1 [#{a1 v1}] [])]
            [[] [] []])
          (rest ncols))
        fcols    (into lcols (sort-by (comp (aid db) get-a) cols))
        strata   (conj lstrata (set vars))
        lseen    (:seen-or-joins last-step)]
    (mk-merge-scan-step index attrs-v vars in out fcols strata lseen nil nil)))

(defn- index-by-link
  [cols link-e link]
  (case (:type link)
    :ref     (or (find-index (:tgt link) cols)
                 (find-index (:attr link) cols))
    :_ref    (find-index link-e cols)
    :val-eq  (or (find-index (:var link) cols)
                 (find-index ((:attrs link) link-e) cols))
    ;; For or-join, return index where tgt will be after step adds free-vars
    ;; and tgt
    :or-join (+ (count cols) (count (:free-vars link)))))

(defn- enrich-cols
  [cols index attr]
  (let [pa (cols index)]
    (mapv (fn [e] (if (and (= e pa) (set? e)) (conj e attr) e)) cols)))

(defn- col-var
  [col]
  (if (set? col)
    (some #(when (symbol? %) %) col)
    col))

(defn- col-attrs
  [col]
  (if (set? col)
    (into #{} (filter keyword?) col)
    #{}))

(defn- merge-join-cols
  "Merge input and target cols for hash join output, preserving input order.
   Returns [merged-cols new-vars]."
  [in-cols tgt-cols]
  (let [in-vars    (mapv col-var in-cols)
        tgt-vars   (mapv col-var tgt-cols)
        tgt-map    (zipmap tgt-vars tgt-cols)
        in-var-set (set in-vars)
        merged-in  (mapv (fn [col v]
                           (if-let [tcol (tgt-map v)]
                             (let [attrs (set/union (col-attrs col)
                                                    (col-attrs tcol))]
                               (if (seq attrs)
                                 (conj attrs v)
                                 v))
                             col))
                         in-cols in-vars)
        new-cols   (reduce (fn [acc [v col]]
                             (if (in-var-set v) acc (conj acc col)))
                           [] (map vector tgt-vars tgt-cols))
        new-vars   (set/difference (set tgt-vars) in-var-set)]
    [(into merged-in new-cols) new-vars]))

(defn- link-step
  [type last-step index attr tgt new-key]
  (let [in      (:out last-step)
        out     (if (set? in) (set new-key) new-key)
        lcols   (:cols last-step)
        lstrata (:strata last-step)
        lseen   (:seen-or-joins last-step)
        fidx    (find-index tgt lcols)
        cols    (cond-> (enrich-cols lcols index attr)
                  (nil? fidx) (conj tgt))]
    [(mk-link-step type index attr tgt fidx in out cols (conj lstrata #{tgt}) lseen)
     (or fidx (dec (count cols)))]))

(defn- rev-ref-plan
  [db last-step index {:keys [type attr tgt]} new-key new-steps]
  (let [[step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- val-eq-plan
  [db last-step index {:keys [type attrs tgt]} new-key new-steps]
  (let [attr           (attrs tgt)
        [step n-index] (link-step type last-step index attr tgt new-key)]
    (if (= 1 (count new-steps))
      [step]
      [step (merge-scan-step db step n-index new-key new-steps)])))

(defn- hash-join-plan
  [_db {:keys [steps cost size]} link-e link new-key
   new-base-plan result-size]
  (let [last-step       (peek steps)
        in              (:out last-step)
        out             (if (set? in) (set new-key) new-key)
        lcols           (:cols last-step)
        lstrata         (:strata last-step)
        lseen           (:seen-or-joins last-step)
        tgt-steps       (:steps new-base-plan)
        in-size         (or size 0)
        tgt-size        (or (:size new-base-plan) 0)
        tgt-cols        (:cols (peek tgt-steps))
        [cols new-vars] (merge-join-cols lcols tgt-cols)
        step            (mk-hash-join-step link link-e in out lcols cols
                                           (conj lstrata new-vars) lseen
                                           tgt-steps in-size tgt-size)
        base-cost       (or (:cost new-base-plan) 0)
        join-cost       (estimate-hash-join-cost in-size tgt-size)]
    (make-plan [step]
               (+ ^long cost ^long base-cost ^long join-cost)
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- or-join-plan*
  [db sources rules last-step
   {:keys [clause bound-var free-vars tgt tgt-attr]} new-key new-base]
  (let [in        (:out last-step)
        out       (if (set? in) (set new-key) new-key)
        lcols     (:cols last-step)
        lstrata   (:strata last-step)
        lseen     (:seen-or-joins last-step)
        bound-idx (find-index bound-var lcols)
        or-cols   (-> lcols (into free-vars) (conj tgt))
        or-seen   (conj lseen clause)
        or-step   (mk-or-join-step clause
                                   bound-var
                                   bound-idx
                                   free-vars
                                   tgt
                                   tgt-attr
                                   sources
                                   rules
                                   in out or-cols
                                   (conj lstrata #{tgt})
                                   or-seen)
        tgt-idx   (dec (count or-cols))]
    (if new-base
      (let [new-steps (:steps new-base)]
        [or-step (merge-scan-step db or-step tgt-idx new-key new-steps)])
      [or-step])))

(defn- count-init-follows
  [^DB db tuples attr index]
  (let [store (.-store db)]
    (rd/fold
      +
      (rd/map #(av-size store attr (aget ^objects % index))
              (p/remove-end-scan tuples)))))

(defn- count-init-follows-stats
  [^DB db tuples attr index]
  (let [store    (.-store db)
        ^List ts (p/remove-end-scan tuples)
        n        (.size ts)]
    (loop [i     0
           sum   0.0
           sumsq 0.0
           mx    0.0]
      (if (< i n)
        (let [^objects t (.get ts i)
              f          (double (av-size store attr (aget t index)))]
          (recur (u/long-inc i)
                 (+ sum f)
                 (+ sumsq (* f f))
                 (if (> f mx) f mx)))
        {:n       n
         :sum     sum
         :sumsq   sumsq
         :max-val mx}))))

(defn- link-ratio-key
  [link-e {:keys [type attr attrs tgt]}]
  (case type
    :val-eq [type (attrs link-e) (attrs tgt)]
    :_ref   [type attr]
    [type attr]))

(defn- estimate-link-size
  [db link-e {:keys [type attr attrs tgt var]} ^ConcurrentHashMap ratios
   prev-size prev-plan index]
  (let [prev-steps              (:steps prev-plan)
        attr                    (or attr (attrs tgt))
        ratio-key               (link-ratio-key link-e {:type  type
                                                        :attr  attr
                                                        :var   var
                                                        :attrs attrs
                                                        :tgt   tgt})
        {:keys [result sample]} (peek prev-steps)
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [{:keys [^long n ^double sum ^double sumsq ^double max-val]}
              (count-init-follows-stats db sample attr index)

              mean       (if (pos? n) (/ sum (double n)) 0.0)
              variance   (if (pos? n)
                           (max 0.0 (- (/ sumsq (double n)) (* mean mean)))
                           0.0)
              cv2        (if (pos? mean) (/ variance (* mean mean)) 0.0)
              base-ratio (double (db/-default-ratio db attr))
              k-eff      (* (double c/link-estimate-prior-size)
                            (+ 1.0 (* (double c/link-estimate-var-alpha) cv2)))
              blended    (if (pos? (+ (double n) k-eff))
                           (/ (+ sum (* k-eff base-ratio))
                              (+ (double n) k-eff))
                           base-ratio)
              ub         (if (pos? n)
                           (min (+ mean (/ (- max-val mean)
                                           (Math/sqrt (double n))))
                                (* mean (double c/link-estimate-max-multi)))
                           base-ratio)
              ratio      (max base-ratio blended ub (double c/magic-link-ratio))]
          (.put ratios ratio-key ratio)
          (* (double prev-size) ratio))

        (< 0 rsize)
        (let [^long size (count-init-follows db result attr index)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (let [ratio (db/-default-ratio db attr)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ^double ratio))))))

(defn- count-or-join-follows
  "Execute or-join on input tuples and count output size."
  [db sources rules tuples {:keys [clause bound-var free-vars tgt-attr]}
   bound-idx]
  (let [result (or-join-execute-link db sources rules tuples clause bound-var
                                     bound-idx free-vars tgt-attr)]
    (.size ^List result)))

(defn- estimate-or-join-size
  [db sources rules ^ConcurrentHashMap ratios prev-plan link]
  (let [prev-size               (:size prev-plan)
        prev-steps              (:steps prev-plan)
        last-step               (peek prev-steps)
        bound-idx               (find-index (:bound-var link) (:cols last-step))
        ratio-key               [:or-join (:bound-var link) (:tgt link)]
        {:keys [result sample]} last-step
        ^long ssize             (if sample (.size ^List sample) 0)
        ^long rsize             (if result (.size ^List result) 0)]
    (estimate-round
      (cond
        (< 0 ssize)
        (let [^long size (count-or-join-follows db sources rules sample link
                                                bound-idx)
              ratio      (max (double (/ size ssize))
                              ^double c/magic-or-join-ratio)]
          (.put ratios ratio-key ratio)
          (* ^long prev-size ratio))

        (< 0 rsize)
        (let [^long size (count-or-join-follows db sources rules result link
                                                bound-idx)
              ratio      (/ size rsize)]
          (.put ratios ratio-key ratio)
          size)

        (.containsKey ratios ratio-key)
        (* ^long prev-size ^double (.get ratios ratio-key))

        :else
        (do (.put ratios ratio-key c/magic-or-join-ratio)
            (* ^long prev-size ^double c/magic-or-join-ratio))))))

(defn- estimate-join-size
  [db sources rules link-e link ratios prev-plan index new-base-plan]
  (let [prev-size (:size prev-plan)
        steps     (:steps new-base-plan)]
    (case (:type link)
      :ref     [nil (estimate-scan-v-size prev-size steps)]
      :or-join (let [or-size (estimate-or-join-size db sources rules ratios
                                                    prev-plan link)]
                 ;; or-join doesn't have new-base-plan steps to merge
                 [or-size or-size])
      ;; :_ref and :val-eq
      (let [e-size (estimate-link-size db link-e link ratios prev-size
                                       prev-plan index)]
        [e-size (estimate-scan-v-size e-size steps)]))))

(defn- estimate-link-cost
  [^long outer-size ^long result-size]
  (estimate-round
    (+ (* outer-size ^double c/magic-cost-link-probe)
       (* result-size ^double c/magic-cost-link-retrieval))))

(defn- estimate-hash-join-cost
  [^long left-size ^long right-size]
  (estimate-round (* ^double c/magic-cost-hash-join
                     (+ left-size right-size))))

(defn- estimate-e-plan-cost
  [prev-size e-size cur-steps]
  (let [step1 (first cur-steps)]
    (if (= 1 (count cur-steps))
      (if (identical? (-type step1) :merge)
        (estimate-scan-v-cost step1 prev-size)
        (estimate-link-cost prev-size e-size))
      (+ ^long (estimate-link-cost prev-size e-size)
         ^long (estimate-scan-v-cost (peek cur-steps) e-size)))))

(defn- e-plan
  [db {:keys [steps cost size]} index link-e link new-key new-base-plan e-size
   result-size]
  (let [new-steps (:steps new-base-plan)
        last-step (peek steps)
        cur-steps
        (case (:type link)
          :ref    [(merge-scan-step db last-step index new-key new-steps)]
          :_ref   (rev-ref-plan db last-step index link new-key new-steps)
          :val-eq (val-eq-plan db last-step index link new-key new-steps))]
    (make-plan cur-steps
               (+ ^long cost ^long (estimate-e-plan-cost size e-size cur-steps))
               result-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- compare-plans
  "Compare two plans. Prefer lower cost, then lower size as tiebreaker."
  [p1 p2]
  (let [c1 ^long (:cost p1)
        c2 ^long (:cost p2)]
    (if (= c1 c2)
      (if (< ^long (:size p2) ^long (:size p1)) p2 p1)
      (if (< ^long c2 ^long c1) p2 p1))))

(defn- or-join-plan
  [base-plans new-e db sources rules ratios prev-plan link last-step new-key
   link-e]
  (let [new-base  (base-plans [new-e])
        or-size   (estimate-or-join-size db sources rules ratios prev-plan link)
        cur-steps (or-join-plan* db sources rules last-step link new-key
                                 new-base)
        or-cost   (estimate-e-plan-cost (:size prev-plan) or-size cur-steps)]
    (make-plan cur-steps
               (+ ^long (:cost prev-plan) ^long or-cost)
               or-size
               (- ^long (find-index link-e (:strata last-step))))))

(defn- binary-plan*
  [db sources rules base-plans ratios prev-plan link-e new-e link new-key]
  (let [last-step (peek (:steps prev-plan))
        index     (index-by-link (:cols last-step) link-e link)
        link-type (:type link)]
    (if (identical? :or-join link-type)
      (or-join-plan base-plans new-e db sources rules ratios prev-plan link
                    last-step new-key link-e)
      (let [new-base (base-plans [new-e])
            [e-size result-size]
            (estimate-join-size db sources rules link-e link ratios prev-plan
                                index new-base)]
        (if (and (#{:_ref :val-eq} link-type) new-base)
          (let [link-plan (e-plan db prev-plan index link-e link new-key
                                  new-base e-size result-size)]
            (if (< ^long (:size prev-plan) ^long c/hash-join-min-input-size)
              link-plan
              (let [hash-plan (hash-join-plan db prev-plan link-e link new-key
                                              new-base result-size)]
                (compare-plans link-plan hash-plan))))
          (e-plan db prev-plan index link-e link new-key new-base e-size
                  result-size))))))

(defn- binary-plan
  [db sources rules nodes base-plans ratios prev-plan link-e new-e new-key]
  (let [last-step     (peek (:steps prev-plan))
        seen-or-joins (or (:seen-or-joins last-step) #{})
        links         (get-in nodes [link-e :links])
        filtered-links
        (into []
              (comp
                (filter #(= new-e (:tgt %)))
                (filter #(or (not= :or-join (:type %))
                             (not (contains? seen-or-joins (:clause %))))))
              links)
        candidates
        (mapv #(binary-plan* db sources rules base-plans ratios prev-plan
                             link-e new-e % new-key)
              filtered-links)]
    (when (seq candidates)
      (apply u/min-key-comp (juxt :recency :cost :size) candidates))))

(defn- plans
  [db sources rules nodes pairs base-plans prev-plans ratios]
  (apply
    u/merge-with compare-plans
    (mapv
      (fn [[prev-key prev-plan]]
        (let [prev-key-set (set prev-key)]
          (persistent!
            (reduce
              (fn [t [link-e new-e]]
                (if (and (prev-key-set link-e) (not (prev-key-set new-e)))
                  (let [new-key  (conj prev-key new-e)
                        cur-plan (t new-key)
                        new-plan
                        (binary-plan db sources rules nodes base-plans
                                     ratios prev-plan link-e new-e new-key)]
                    (if new-plan
                      (if (or (nil? cur-plan)
                              (identical? new-plan
                                          (compare-plans cur-plan new-plan)))
                        (assoc! t new-key new-plan)
                        t)
                      t))
                  t))
              (transient {}) pairs))))
      prev-plans)))

(def ^:private connected-pairs qog/connected-pairs)

(defn- shrink-space
  [plans]
  (persistent!
    (reduce-kv
      (fn [m k ps]
        (assoc! m k (-> (peek (apply min-key (fn [p] (:cost (peek p))) ps))
                        (update :steps (fn [ss]
                                         (if (= 1 (count ss))
                                           [(update (first ss) :out set)]
                                           [(first ss)
                                            (update (peek ss) :out set)]))))))
      (transient {}) (group-by (fn [p] (set (nth p 0))) plans))))

(defn- trace-steps
  [^List tables ^long n-1]
  (let [final-plans (vals (.get tables n-1))]
    (reduce
      (fn [plans i]
        (cons ((.get tables i) (:in (first (:steps (first plans))))) plans))
      [(apply min-key :cost final-plans)]
      (range (dec n-1) -1 -1))))

(defn- plan-component
  [db sources rules nodes component]
  (let [n (count component)]
    (if (= n 1)
      [(base-plan db nodes (first component) true)]
      (let [base-plans (build-base-plans db nodes component)]
        (if (some nil? (vals base-plans))
          [nil]
          (let [pairs  (connected-pairs nodes component)
                tables (FastList. n)
                ratios (ConcurrentHashMap.)
                n-1    (dec n)
                pn     ^long (min (long c/plan-search-max)
                                  (long (u/n-permutations n 2)))]
            (.add tables base-plans)
            (dotimes [i n-1]
              (let [plans (plans db sources rules nodes pairs base-plans
                                 (.get tables i) ratios)]
                (if (< pn (count plans))
                  (.add tables (shrink-space plans))
                  (.add tables plans))))
            (trace-steps tables n-1)))))))

(def ^:private connected-components qog/connected-components)

(defn- build-plan*
  [db sources rules nodes]
  (let [cc (connected-components nodes)]
    (if (= 1 (count cc))
      [(plan-component db sources rules nodes (first cc))]
      (map+ (bound-fn [component]
              (plan-component db sources rules nodes component))
            cc))))

(defn- strip-step-result
  [step]
  (let [step (if (contains? step :tgt-steps)
               (update step :tgt-steps (fn [steps]
                                         (mapv strip-step-result steps)))
               step)]
    (assoc step :result nil :sample nil)))

(defn- strip-result
  [plans]
  (mapv (fn [plan-vec]
          (mapv #(update % :steps (fn [steps]
                                    (mapv strip-step-result steps)))
                plan-vec))
        plans))

(defn build-plan
  "Generate a query plan that looks like this:

  [{:op :init :attr :name :val \"Tom\" :out #{?e} :vars [?e]
    :cols [?e]}
   {:op :merge-scan  :attrs [:age :friend] :preds [(< ?a 20) nil]
    :vars [?a ?f] :in #{?e} :index 0 :out #{?e} :cols [?e :age :friend]}
   {:op :link :attr :friend :var ?e1 :in #{?e} :index 2
    :out #{?e ?e1} :cols [?e :age :friend ?e1]}
   {:op :merge-scan :attrs [:name] :preds [nil] :vars [?n] :index 3
    :in #{?e ?e1} :out #{?e ?e1} :cols [?e :age :friend ?e1 :name]}]

  :op here means step type.
  :result-set will be #{} if there is any clause that matches nothing."
  [deps {:keys [graph sources rules] :as context}]
  (binding [*optimizer-deps* deps]
    (if graph
      (unreduced
        (reduce-kv
          (fn [c src nodes]
            (let [^DB db (sources src)
                  k      [(.-store db) nodes]]
              (if-let [cached (.get ^LRUCache (plan-cache) k)]
                (assoc-in c [:plan src] cached)
                (let [nodes (update-nodes db nodes)
                      plans (if (< 1 (count nodes))
                              (build-plan* db sources rules nodes)
                              [[(base-plan db nodes (ffirst nodes) true)]])]
                  (if (some #(some nil? %) plans)
                    (reduced (assoc c :result-set #{}))
                    (do (.put ^LRUCache (plan-cache) k (strip-result plans))
                        (assoc-in c [:plan src] plans)))))))
          context graph))
      context)))

(defn- component-binds-vars?
  [plans vars]
  (when-let [step (some-> plans last :steps last)]
    (let [cols (:cols step)]
      (every? #(some? (find-index % cols)) vars))))

(defn- add-not-join-step
  [plans clause sources rules]
  (let [plans     (vec plans)
        plan-idx  (dec (count plans))
        last-plan (plans plan-idx)
        last-step (some-> last-plan :steps peek)
        vars      (get-not-join-vars clause)
        nstep     (mk-not-join-step clause vars sources rules
                                    (:out last-step) (:out last-step)
                                    (:cols last-step) (:strata last-step)
                                    (:seen-or-joins last-step))]
    (assoc plans plan-idx (update last-plan :steps conj nstep))))

(defn plan-not-joins
  "Attach optimizable not-join clauses to source plans when all join vars are
   bound by a single component. Unlinked clauses remain in :late-clauses."
  [deps {:keys [plan sources rules optimizable-not-joins] :as context}]
  (binding [*optimizer-deps* deps]
    (if (seq optimizable-not-joins)
      (let [plan'                (into {} (map (fn [[src comps]]
                                                 [src (mapv vec comps)]))
                                      plan)
            [planned unlinked]
            (reduce
              (fn [[p u] clause]
                (let [src        (get-not-join-source clause)
                      vars       (get-not-join-vars clause)
                      components (get p src)]
                  (if (and (seq vars) (seq components))
                    (let [idxs (keep-indexed
                                 (fn [i comp]
                                   (when (component-binds-vars? comp vars) i))
                                 components)]
                      (if (= 1 (count idxs))
                        (let [idx (first idxs)]
                          [(assoc-in p [src idx]
                                     (add-not-join-step
                                       (nth components idx) clause sources rules))
                           u])
                        [p (conj u clause)]))
                    [p (conj u clause)])))
              [plan' []]
              optimizable-not-joins)]
        (-> context
            (assoc :plan planned)
            (update :late-clauses into unlinked)
            (assoc :optimizable-not-joins [])))
      context)))
