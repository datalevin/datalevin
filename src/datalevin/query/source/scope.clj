(ns ^:no-doc datalevin.query.source.scope
  "Typed interfaces at captured rule, union and attribute boundaries. Bodies
  remain shared engine regions; this pass never evaluates them or reads rows."
  (:require [clojure.set :as set]
            [datalevin.relation :as relation]
            [datalevin.rules.relation :as rule-relation]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey]
           [java.util AbstractList List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :planning)))

(def ^:private object-array-class (Class/forName "[Ljava.lang.Object;"))

(defn- unsupported! [] (fail! :query-source/unsupported :planning))

(defn- columns! [columns check!]
  (ensure! (vector? columns))
  (when (seq columns) (contract/binding-columns! columns check!))
  columns)

(defn- by-variable [columns] (into {} (map (juxt :variable identity)) columns))
(defn- clean [columns] (mapv #(select-keys % [:variable :type :nullable?]) columns))

(defn- canonical [variables check!]
  (vec (sort #(access/compare-keys %1 %2 check!) variables)))

(defn- projected [columns variables empty? check!]
  (let [columns (by-variable columns)]
    (mapv (fn [v]
            (check!)
            (or (get columns v)
                (if empty? {:variable v :type :data :nullable? true} (unsupported!)))) variables)))

(defn- intersect-column [a b]
  ;; Equality to a typed fact can narrow :data after the join. For other
  ;; incomparable types keep a conservative envelope; the shared argument
  ;; unifier still tests equality. No mismatch establishes an empty proof here.
  (cond
    (nil? a) b
    (or (= (:type a) (:type b)) (= :data (:type a)) (= :data (:type b)))
    (assoc b :type (if (= :data (:type b)) (:type a) (:type b))
             :nullable? (and (:nullable? a) (:nullable? b)))
    :else (assoc b :type :data :nullable? (and (:nullable? a) (:nullable? b)))))

(defn- union-columns [branches variables check!]
  (let [live (filterv (complement :empty?) branches)]
    (mapv (fn [v]
            (check!)
            (let [columns (mapv #(get (by-variable (:outputs %)) v) live)]
              (ensure! (every? some? columns))
              {:variable v :type (if (and (seq columns) (apply = (map :type columns)))
                                   (:type (first columns)) :data)
               :nullable? (or (empty? columns) (boolean (some :nullable? columns)))})) variables)))

(defn- incompatible-constraints? [region constraints check!]
  (boolean
    (some (fn [{:keys [variable type]}]
            (check!)
            (ensure! (and (contains? (:literal-vars region) variable)
                          (contains? (:bindings region) variable)))
            (value/disjoint-constant? type (get (:bindings region) variable) check!)) constraints)))

(defn- rule-branch [site branch region body-columns constraints parent check!]
  (let [head (:head-vars branch)
        arguments (:arguments branch)
        head-columns (projected body-columns head (not (:live? region)) check!)
        head-map (by-variable head-columns)
        constants (select-keys (:bindings region) head)
        ;; Literal values retain their representation through fact filters and
        ;; pure operators. Captured scalar inputs still use typed join keys.
        inputs (reduce (fn [columns v]
                         (check!)
                         (let [[tag actual] (get arguments v)]
                           (cond
                             (contains? constants v) (conj columns (get head-map v))
                             (and (= :var tag) (contains? (:input-vars region) v) (get parent actual))
                             (conj columns (assoc (get parent actual) :variable v))
                             :else columns))) [] head)
        available (set (map :variable inputs))
        _ (when-not (set/subset? (:required-inputs branch) available) (unsupported!))
        incompatible-constant?
        (some (fn [[v x]]
                (check!)
                (let [column (get head-map v)]
                  (when-not (if (nil? x) (:nullable? column)
                                (value/matches-type? (:type column) x check!))
                    ;; Captured scalar inputs still need a representation-aware
                    ;; key edge for overlapping types. Literal fact filters
                    ;; have already been separated from their owned seeds.
                    (when-not (value/disjoint-constant? (:type column) x check!)
                      (unsupported!))
                    true))) constants)
        empty? (or (not (:live? region)) (boolean incompatible-constant?)
                   (incompatible-constraints? region constraints check!))
        call-columns (when-not empty?
                       (reduce (fn [m column]
                                 (check!)
                                 (let [[tag actual] (get arguments (:variable column))]
                                   (if (= :var tag)
                                     (let [column (assoc column :variable actual)]
                                       (assoc m actual (intersect-column (get m actual) column))) m)))
                               {} head-columns))
        variables (canonical (:exports site) check!)]
    {:region-id (:region-id region) :empty? empty?
     :inputs inputs :bindings constants
     ;; Projection is before call-argument unification, including `_` positions.
     :projection head :arguments arguments
     :columns head-columns
     :outputs (if empty? (projected [] variables true check!)
                  (projected (vec (vals call-columns)) variables false check!))}))

(defn- ordinary-branch [site region body-columns constraints parent check!]
  (let [restore (if (= :attribute (:kind site))
                  (get-in region [:patterns 0 :restore]) {})
        variables (canonical (:exports site) check!)
        inputs (->> (vals parent)
                    (filter #(contains? (:input-vars region) (:variable %)))
                    (map (fn [column]
                           (if (contains? restore (:variable column))
                             ;; Attribute dispatch is an exact core equality
                             ;; partition, so the routed keyword is known here.
                             (assoc column :type :keyword :nullable? false) column)))
                    (sort #(access/compare-keys (:variable %1) (:variable %2) check!)) vec)
        restored (mapv (fn [v] {:variable v :type :keyword :nullable? false}) (keys restore))
        body (reduce (fn [m column]
                       (check!)
                       (when-let [before (get m (:variable column))]
                         (ensure! (#{:keyword :data} (:type before))))
                       (assoc m (:variable column) (intersect-column (get m (:variable column)) column)))
                     (by-variable body-columns) restored)
        empty? (or (not (:live? region)) (incompatible-constraints? region constraints check!))
        output (projected (vec (vals body)) variables empty? check!)]
    {:region-id (:region-id region) :empty? empty? :inputs inputs
     :bindings (select-keys (:bindings region) (set (map :variable inputs)))
     :restore restore :projection variables :columns output :outputs output}))

(defn interfaces!
  "Compile one captured site's boundary from complete shared branch schemas.
  parent-columns describes actual upstream bindings, never possible outputs.
  branch-outputs is a vector of {:region-id id :columns typed-columns}; it may
  arrive in any order, but must cover exactly this site's child regions.
  Optional :constant-constraints retains producer types for captured literal
  seeds without changing their output representation or accepting estimates.

  Keep every branch's precise schema and full rule-head set key. Combined
  heterogeneous outputs use :data without changing their values or identity.
  Required inputs are translated only through captured argument/export edges.
  The result is private compiler state, not a provider envelope or executor."
  [runtime compiled site-index parent-columns branch-outputs]
  (let [ctl (:control runtime) captured-owner (volatile! nil)
        check! #(do (control/check-active! ctl)
                    (when-let [owner @captured-owner] (ensure! (not @(:closed? owner)))))
        tokens (volatile! []) success? (volatile! false)
        own! #(do (vswap! tokens conj %) %)]
    (try
      (check!)
      (own! (rt/charge-metadata! runtime [site-index parent-columns branch-outputs] :planning))
      (ensure! (and (integer? site-index)
                    (not (value/at-most? site-index -1))
                    (value/at-most? site-index (dec (count (:sites compiled))))
                    (vector? branch-outputs)))
      (let [site (nth (:sites compiled) site-index)
            parent-region (nth (:regions compiled) (:parent-region site))
            owner (:owner parent-region)]
        (ensure! (and (identical? ctl (:control owner)) (= (:query-id runtime) (:query-id owner))
                      (not @(:closed? owner)) (#{:rule :union :attribute :negation} (:kind site))))
        (vreset! captured-owner owner)
        (columns! parent-columns check!)
        (ensure! (set/subset? (set (map :variable parent-columns)) (:available-vars parent-region)))
        (doseq [{:keys [region-id columns constant-constraints] :as output} branch-outputs]
          (check!)
          (ensure! (and (value/plain-map? output)
                        (contains? output :region-id) (contains? output :columns)
                        (set/subset? (set (keys output)) #{:region-id :columns :constant-constraints})
                        (integer? region-id) (not (value/at-most? region-id -1))))
          (columns! columns check!)
          (when constant-constraints
            (ensure! (vector? constant-constraints))
            ;; Several scalar producers may constrain the same literal.
            (doseq [column constant-constraints] (columns! [column] check!))))
        (own! (control/reserve! ctl
                {:retained-bytes
                 (value/add-charge (+ 4096 (* 2048 (count branch-outputs))
                                      (* 512 (count parent-columns)))
                   (value/multiply-charge 1024
                     (reduce (fn [^long n output] (+ n (count (:columns output))))
                             0 branch-outputs)))}))
        (let [branches (:branches site)
              child-id #(if (map? %) (:region-id %) %)
              expected (set (map child-id branches))
              by-id (into {} (map (juxt :region-id (comp clean :columns))) branch-outputs)
              constraints (into {} (map (juxt :region-id :constant-constraints)) branch-outputs)]
          (ensure! (and (= expected (set (keys by-id))) (= (count by-id) (count branch-outputs))))
          (doseq [[id columns] by-id]
            (check!)
            (ensure! (and (value/at-most? id (dec (count (:regions compiled))))
                          (set/subset? (set (map :variable columns))
                                       (get-in compiled [:regions id :available-vars])))))
          (doseq [[id columns] constraints]
            (check!)
            (ensure! (set/subset? (set (map :variable columns))
                                 (get-in compiled [:regions id :literal-vars]))))
          (let [parent-columns (clean parent-columns)
                parent (by-variable parent-columns)
                required (if (= :rule (:kind site)) #{} (:required-inputs site))
                _ (when-not (set/subset? required (set (keys parent))) (unsupported!))
                outputs (mapv (fn [branch]
                                (check!)
                                (let [id (child-id branch) region (nth (:regions compiled) id)]
                                  (if (= :rule (:kind site))
                                    (rule-branch site branch region (by-id id) (constraints id) parent check!)
                                    (ordinary-branch site region (by-id id) (constraints id) parent check!)))) branches)
                variables (canonical (:exports site) check!)
                columns (union-columns outputs variables check!)
                call-args (when (= :rule (:kind site))
                            (mapv (fn [v]
                                    (check!)
                                    (let [[tag x] (get-in (first outputs) [:arguments v])]
                                      (if (= :ignore tag)
                                        (loop []
                                          (check!)
                                          (let [candidate (gensym "?ignored__")]
                                            (if (some (fn [region]
                                                        (check!)
                                                        (contains? (:available-vars region) candidate))
                                                      (:regions compiled))
                                              (recur) candidate))) x)))
                                  (:projection (first outputs))))
                ;; Caller variable spellings never become rule-body inputs.
                consumed (if (= :rule (:kind site))
                           (into #{} (mapcat (fn [branch]
                                              (keep (fn [column]
                                                      (let [[tag v] (get (:arguments branch) (:variable column))]
                                                        (when (and (= :var tag) (contains? parent v)
                                                                   (not (contains? (:bindings branch)
                                                                                   (:variable column)))) v)))
                                                    (:inputs branch)))) outputs)
                           (into #{} (mapcat #(map :variable (:inputs %))) outputs))
                closed? (atom false)
                output {:owner owner :closed? closed? :kind (:kind site) :site-index site-index
                        :parent-region (:parent-region site) :path (:path site)
                        :inputs (filterv #(contains? consumed (:variable %)) parent-columns)
                        :columns columns :branches outputs
                        :call-args call-args :public-projection variables
                        :empty? (every? :empty? outputs)
                        :combine (if (= :attribute (:kind site)) :bag-union :set-union)}
                _ (own! (rt/charge-copy! runtime (dissoc output :owner :closed?)))
                output (value/owned-copy (dissoc output :owner :closed?) check!)
                ;; owner contains engine objects; copy only public typed data.
                held @tokens
                close (rt/own! runtime #(when (compare-and-set! closed? false true)
                                         (doseq [token held] (control/release! token))))]
            (vreset! success? true)
            (assoc output :owner owner :closed? closed? :close close))))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))

(defn check!
  "Reject a foreign or closed boundary before the driver uses its interfaces."
  [runtime boundary]
  (control/check-active! (:control runtime))
  (ensure! (and (identical? (:control runtime) (get-in boundary [:owner :control]))
                (= (:query-id runtime) (get-in boundary [:owner :query-id]))
                (not @(:closed? boundary)) (not @(get-in boundary [:owner :closed?]))))
  boundary)

(defn refine-regions!
  "Propagate captured boundary no-match proofs through the region tree before
  provider requests are built. A conjunction is empty when any of its sites
  has no live positive branch; an empty negative body keeps its parent live.
  An empty ancestor suppresses all descendant access work. Authorization and descriptor validation remain query-wide."
  [runtime compiled boundaries]
  (let [active! #(control/check-active! (:control runtime))
        regions (:regions compiled) sites (:sites compiled)]
    (ensure! (vector? boundaries))
    (let [token (control/reserve! (:control runtime)
                  {:retained-bytes (+ 2048 (* 2048 (count regions)) (* 1024 (count boundaries)))})]
      (try
        (let [proved
              (reduce (fn [ids boundary]
                        (check! runtime boundary)
                        (let [index (:site-index boundary)
                              _ (ensure! (and (integer? index)
                                              (not (value/at-most? index -1))
                                              (value/at-most? index (dec (count sites)))))
                              site (nth sites index)
                              owner (get-in regions [(:parent-region site) :owner])
                              child-id #(if (map? %) (:region-id %) %)]
                          (ensure! (and (identical? owner (:owner boundary))
                                        (= (select-keys site [:kind :parent-region :path])
                                           (select-keys boundary [:kind :parent-region :path]))
                                        (= (mapv child-id (:branches site))
                                           (mapv :region-id (:branches boundary)))))
                          (into ids (comp (filter :empty?) (map :region-id)) (:branches boundary))))
                      #{} boundaries)
              children (group-by :parent-region (filter #(seq (:branches %)) sites))
              empty-ids
              (reduce (fn [ids r]
                        (active!)
                        (let [id (:region-id r)]
                          (if (or (:empty? r) (contains? ids id)
                                  (some (fn [site]
                                          (and (not= :negation (:kind site))
                                               (every? #(contains? ids (if (map? %) (:region-id %) %))
                                                       (:branches site)))) (get children id)))
                            (conj ids id) ids))) proved (reverse regions))
              refined
              (reduce (fn [result r]
                        (active!)
                        (let [empty? (contains? empty-ids (:region-id r))
                              parent (:parent-region r)]
                          (conj result (assoc r :empty? empty?
                                               :live? (and (not empty?)
                                                           (or (nil? parent) (:live? (nth result parent))))))))
                      [] regions)]
          (rt/own! runtime #(control/release! token))
          (assoc compiled :regions refined))
        (catch Throwable e (control/release! token) (throw e))))))

(defn- admit-container! [runtime tokens ^long rows ^long width]
  ;; Shared projection/union helpers allocate their sized lists, key indexes,
  ;; wrappers and tuple arrays eagerly. Admit a conservative complete envelope
  ;; before invoking them. Value leaves have separate owned handoff charges.
  (when (> rows Integer/MAX_VALUE) (fail! :query-source/limit :execution))
  (let [per-row (value/add-charge 384 (value/multiply-charge 32 width))
        metadata (value/add-charge 2048 (value/multiply-charge 128 width))
        bytes (value/add-charge metadata (value/multiply-charge rows per-row))
        token (control/reserve! (:control runtime) {:retained-bytes bytes})]
    (vswap! tokens conj token)))

(defn- capture-body! [runtime tokens branch body check!]
  (let [ctl (:control runtime)
        columns (:columns branch) attrs (:attrs body) ^List tuples (:tuples body)
        restore (:restore branch) width (count columns)]
    (ensure! (and (instance? datalevin.relation.Relation body) (value/plain-map? attrs)
                  (instance? List tuples)
                  (every? (fn [column]
                            (or (contains? attrs (:variable column))
                                (contains? restore (:variable column))
                                (zero? (.size tuples)))) columns)))
    (ensure! (and (not (neg? (.size tuples)))
                  (every? (fn [column]
                            (let [v (:variable column)]
                              (or (not (contains? attrs v))
                                  (and (integer? (get attrs v))
                                       (not (value/at-most? (get attrs v) -1)))))) columns)))
    (check!)
    (admit-container! runtime tokens 0 width)
    (let [output (FastList.)]
      (dotimes [i (.size tuples)]
        (check!)
        (let [tuple (.get tuples i)
              scratch (control/reserve! ctl {:retained-bytes (+ 512 (* 64 width))})]
          (try
            (ensure! (and (instance? object-array-class tuple)
                          (every? (fn [column]
                                    (let [v (:variable column) index (get attrs v)]
                                      (or (not (contains? attrs v))
                                          (and (integer? index)
                                               (not (value/at-most? index -1))
                                               (value/at-most? index (dec (alength ^objects tuple)))))))
                                  columns)))
            (let [row (mapv (fn [column]
                              (let [v (:variable column)]
                                (if (contains? attrs v) (aget ^objects tuple (long (get attrs v)))
                                    (get restore v)))) columns)
                  token (rt/charge-copy! runtime row)]
              (vswap! tokens conj token)
              (doseq [[column x] (map vector columns row)]
                (check!)
                (ensure! (if (nil? x) (:nullable? column)
                             (value/matches-type? (:type column) x check!)))
                (when (contains? restore (:variable column))
                  (ensure! (QueryKey/equals x (get restore (:variable column))))))
              (.add output (object-array (value/owned-copy row check!))))
            (finally (control/release! scratch)))))
      (relation/relation! (zipmap (map :variable columns) (range)) output))))

(defn- checked-op
  "Give shared loops a cancellable borrowed list view, then remove that view
  if an identity/empty fast path returns it. Runtime callbacks never escape in
  the returned relation. Native callers keep their existing unwrapped lists."
  [active! f & relations]
  (let [views (mapv (fn [rel]
                      (let [^List tuples (:tuples rel)
                            guarded (proxy [AbstractList] []
                                      (size [] (active!) (.size tuples))
                                      (get [index] (active!) (.get tuples (int index))))]
                        {:relation (assoc rel :tuples guarded) :guarded guarded :tuples tuples})) relations)
        result (apply f (mapv :relation views))]
    (active!)
    (if-let [original (some #(when (identical? (:tuples result) (:guarded %)) %) views)]
      (assoc result :tuples (:tuples original)) result)))

(defn finish!
  "Finish already evaluated, completed shared branch relations at their
  captured scope boundary. branch-results contains {:region-id id :relation
  rel} for exactly the live branches. It performs no evaluation, joins to outer
  witnesses, provider callbacks or replay; those stay with the existing driver.

  Owned handoff copies only the declared head/export columns. Shared operators
  perform full rule-head projection/set union, argument unification and public
  projection; attribute expansion uses bag union. Retaining containers are
  admitted before allocation, and returned values survive producer close.
  Producers must already enforce decoding/arithmetic/intermediate limits."
  [runtime boundary branch-results]
  (check! runtime boundary)
  (let [ctl (:control runtime) active! #(do (control/check-active! ctl) (check! runtime boundary))
        tokens (volatile! []) success? (volatile! false)]
    (try
      (ensure! (vector? branch-results))
      (admit-container! runtime tokens (count branch-results) 0)
      ;; Exported attribute names can outlive the compiler/boundary owner, even
      ;; for empty results. Keep an independent metadata lease through cleanup.
      (vswap! tokens conj (rt/charge-copy! runtime
                            (select-keys boundary [:columns :branches :call-args :public-projection])))
      (active!)
      (doseq [result branch-results]
        (active!)
        (ensure! (and (value/plain-map? result) (= #{:region-id :relation} (set (keys result))))))
      (let [branches (filterv (complement :empty?) (:branches boundary))
            results (into {} (map (juxt :region-id :relation)) branch-results)
            _ (ensure! (and (= (count results) (count branch-results))
                            (= (set (map :region-id branches)) (set (keys results)))))
            canonical-head (:projection (first (:branches boundary)))
            union
            (reduce
              (fn [acc branch]
                (active!)
                (let [body (capture-body! runtime tokens branch (get results (:region-id branch)) active!)
                      rows (.size ^List (:tuples body))
                      _ (admit-container! runtime tokens rows (count (:projection branch)))
                      projected (checked-op active!
                                  #(if (= :attribute (:kind boundary))
                                     (rule-relation/project-rule-result % (:projection branch))
                                     (rule-relation/project-rule-result-distinct % (:projection branch))) body)
                      projected (if (= :rule (:kind boundary))
                                  (rule-relation/rename-rel-attrs projected canonical-head) projected)
                      rows (+ (long (.size ^List (:tuples acc))) (long (.size ^List (:tuples projected))))]
                  (admit-container! runtime tokens rows (count (:attrs projected)))
                  (let [combined (checked-op active! (if (= :bag-union (:combine boundary))
                                                       relation/sum-rel relation/sum-rel-dedupe) acc projected)]
                    (active!)
                    combined)))
              (relation/relation! (zipmap (if (= :rule (:kind boundary)) canonical-head
                                             (:public-projection boundary)) (range)) (FastList.)) branches)
            output
            (if (= :rule (:kind boundary))
              (do
                (admit-container! runtime tokens (.size ^List (:tuples union)) (count canonical-head))
                (let [mapped (checked-op active!
                               #(rule-relation/map-rule-result % canonical-head (:call-args boundary)) union)]
                  (admit-container! runtime tokens (.size ^List (:tuples mapped))
                                    (count (:public-projection boundary)))
                  (checked-op active! #(relation/project-distinct % (:public-projection boundary)) mapped)))
              union)
            held @tokens]
        (active!)
        (rt/own! runtime #(doseq [token held] (control/release! token)))
        (vreset! success? true)
        output)
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))
