(ns ^:no-doc datalevin.query.source.region
  "Region annotations on the existing parsed query/rule graph. This pass
  consumes engine-owned parsed clauses and captured descriptors, never expands
  a rule through its set boundary, and performs no provider or native reads."
  (:require [clojure.set :as set]
            [datalevin.parser :as dp]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.lowering :as lowering]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.parser And Constant DefaultSrc Function Not Or Pattern Predicate Rule RuleExpr Variable]))

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :planning)))

(defn- variable? [x]
  (and (symbol? x) (.startsWith (name x) "?")))

(defn- vars [node check! reserve!]
  ;; Scope analysis inspects syntax, not nested constant data.
  (let [result (volatile! #{})]
    (letfn [(visit [x depth]
              (check!)
              (when (> (long depth) 128) (fail! :query-source/limit :planning))
              (cond
                (instance? Constant x) nil
                (instance? Variable x)
                (do (reserve! 256) (vswap! result conj (with-meta (:symbol x) nil)))
                (instance? Or x) (visit (:rule-vars x) (inc (long depth)))
                (instance? Not x) (visit (:vars x) (inc (long depth)))
                (dp/-traversable? x)
                (do (reserve! 256) (doseq [v (vals x)] (visit v (inc (long depth)))))
                (sequential? x) (doseq [v x] (visit v (inc (long depth))))))]
      (visit node 0)
      @result)))

(defn- source-symbol [node inherited]
  (let [source (:source node)]
    (if (or (nil? source) (instance? DefaultSrc source)) inherited (:symbol source))))

(defn- term [x]
  (cond (instance? Variable x) [:var (with-meta (:symbol x) nil)]
        (instance? Constant x) [:const (:value x)]
        (instance? datalevin.parser.Placeholder x) [:ignore]
        :else (fail! :query-source/unsupported :clauses)))

(defn- head-vars [branch]
  (mapv :symbol (concat (get-in branch [:vars :required]) (get-in branch [:vars :free]))))

(defn annotate!
  "Attach stable regions/occurrence IDs and explicit rule/union/attribute
  edges to an engine-owned parsed query. entries contain already validated v2
  descriptors or existing native/collection sources. rules are parser Rule
  records. bindings contains only captured scalar constants, never outer rows.

  The returned parsed query/rules remain private engine nodes. Provider request
  construction uses request!, not serialization of these annotations. Typed
  upstream exports are supplied by the shared planner when forming requests."
  ([runtime parsed entries rules bindings input-vars]
   (annotate! runtime parsed entries rules bindings input-vars nil))
  ([runtime parsed entries rules bindings input-vars compiler-context]
    (let [ctl (:control runtime) check! #(control/check-active! ctl)
          tokens (volatile! []) children (volatile! []) success? (volatile! false)
          own! #(do (vswap! tokens conj %) %)
          regions (atom []) sites (atom []) occurrence (volatile! 0)
          next-occurrence! (fn [] (check!)
                             (let [id @occurrence]
                               (when (= Long/MAX_VALUE id) (fail! :query-source/limit :planning))
                               (vswap! occurrence (fn [n] (inc (long n)))) id))
          reserve-node! #(own! (control/reserve! ctl {:retained-bytes %}))]
      (try
        (ensure! (and (instance? datalevin.parser.Query parsed) (vector? entries) (vector? rules)
                      (every? #(instance? Rule %) rules) (value/plain-map? bindings) (set? input-vars)
                      (every? variable? input-vars)
                      (set/subset? (set (keys bindings)) input-vars)))
        (own! (rt/charge-copy! runtime [bindings input-vars]))
        (reserve-node! (+ 4096 (* 1024 (count entries)) (* 1024 (count rules))))
        (let [entries-by-source (into {} (map (juxt :symbol identity)) entries)
              rules-by-name (into {} (map #(vector (get-in % [:name :symbol]) %)) rules)
              bindings (value/owned-copy bindings check!)
              node-vars #(vars % check! reserve-node!)]
          (ensure! (and (= (count entries-by-source) (count entries))
                        (= (count rules-by-name) (count rules))))
          (when compiler-context
            (ensure! (and (value/plain-map? compiler-context)
                          (set/subset? #{:default-source :rule-relations} (set (keys compiler-context)))
                          (set/subset? (set (keys compiler-context)) #{:default-source :rule-relations :literal-vars})
                          (symbol? (:default-source compiler-context))
                          (.startsWith (name (:default-source compiler-context)) "$")
                          (or (nil? (entries-by-source (:default-source compiler-context)))
                              (:native (entries-by-source (:default-source compiler-context))))
                          (value/plain-map? (:rule-relations compiler-context))
                          (or (nil? (:literal-vars compiler-context))
                              (and (set? (:literal-vars compiler-context))
                                   (set/subset? (:literal-vars compiler-context) (set (keys bindings)))))))
            (doseq [[key {head :head-vars :keys [required-count input-positions observable-positions] :as header}] (:rule-relations compiler-context)
                    :let [branch (first (:branches (rules-by-name (second key))))]]
              (check!)
              (ensure! (and (vector? key) (= 2 (count key))
                            (:native (entries-by-source (first key)))
                            (rules-by-name (second key))
                            (set/subset? #{:head-vars :required-count :input-positions} (set (keys header)))
                            (set/subset? (set (keys header)) #{:head-vars :required-count :input-positions :observable-positions})
                            (vector? head) (integer? required-count)
                            (= head (head-vars branch))
                            (= required-count (count (get-in branch [:vars :required])))
                            (vector? input-positions)
                            (every? #(and (integer? %)
                                          (not (value/at-most? % -1))
                                          (value/at-most? % (dec (count head)))) input-positions)
                            (= input-positions (vec (sort (set input-positions))))
                            (set/subset? (set (range required-count)) (set input-positions))
                            (or (nil? observable-positions)
                                (and (vector? observable-positions)
                                     (every? #(and (integer? %)
                                                   (not (value/at-most? % -1))
                                                   (value/at-most? % (dec (count head)))) observable-positions)
                                     (= observable-positions (vec (sort (set observable-positions))))))))))
          (doseq [entry entries]
            (own! (rt/charge-metadata! runtime (:symbol entry) :inputs))
            (ensure! (and (symbol? (:symbol entry)) (.startsWith (name (:symbol entry)) "$"))))
          (letfn [(external? [node inherited seen depth]
                    (check!)
                    (when (> (long depth) 128) (fail! :query-source/limit :planning))
                    (reserve-node! 256)
                    (let [source (source-symbol node inherited)]
                      (cond
                        (instance? Pattern node)
                        (do (ensure! (entries-by-source source)) (boolean (:descriptor (entries-by-source source))))
                        (instance? RuleExpr node)
                        (let [name (get-in node [:name :symbol]) key [source name]]
                          (ensure! (rules-by-name name))
                          ;; Explicit body references already belong to the
                          ;; caller's validated native graph. Their definitions
                          ;; supply headers here; bodies compile separately.
                          (when-not (or (contains? (:rule-relations compiler-context) key) (contains? @seen key))
                            (vswap! seen conj key)
                            (some #(external? % source seen (inc (long depth)))
                                  (mapcat :clauses (:branches (rules-by-name name))))))
                        (or (instance? Or node) (instance? And node) (instance? Not node))
                        (some #(external? % source seen (inc (long depth))) (:clauses node))
                        :else false)))
                  (native-core? [node inherited stack depth]
                    ;; Positive acyclic native calls can use the same bounded
                    ;; scoped operators as mapped calls. Recursive/negative
                    ;; native programs retain their existing core rule node for
                    ;; the native evaluator's separate controlled-emission gate.
                    (check!)
                    (when (> (long depth) 128) (fail! :query-source/limit :planning))
                    (reserve-node! 256)
                    (let [source (source-symbol node inherited)]
                      (cond
                        (instance? Not node) true
                        (instance? RuleExpr node)
                        (let [name (get-in node [:name :symbol]) key [source name]
                              rule (rules-by-name name)]
                          (ensure! rule)
                          (or (contains? stack key)
                              (some #(native-core? % source (conj stack key) (inc (long depth)))
                                    (mapcat :clauses (:branches rule)))))
                        (or (instance? Or node) (instance? And node))
                        (some #(native-core? % source stack (inc (long depth))) (:clauses node))
                        :else false)))
                  (region! [path input-vars live bindings literals inherited]
                    (check!)
                    (reserve-node! (+ 2048 (* 128 (+ (count input-vars) (count live) (count bindings)))))
                    (let [id (count @regions)]
                      (swap! regions conj {:region-id id :path path :input-vars input-vars :required-vars live
                                           :bindings (select-keys bindings input-vars)
                                           :literal-vars (set/intersection literals input-vars) :default-source inherited
                                           :available-vars input-vars :patterns [] :core-clauses [] :empty? false})
                      id))
                  (add-core! [id node path]
                    (reserve-node! 512)
                    (swap! regions update-in [id :core-clauses] conj
                           {:path path :node node :literal-terms (::literal-terms (meta node))}))
                  (site! [site]
                    (reserve-node! 2048)
                    (swap! sites conj site))
                  (pattern! [id node path inherited outside bindings literals]
                    (let [;; A literal argument constrains facts by ordinary
                          ;; equality while its original value remains available
                          ;; to scalar operators and the complete rule head.
                          literal-terms (into {} (keep-indexed
                                                  (fn [i x]
                                                    (when (and (instance? Variable x) (contains? literals (:symbol x)))
                                                      [(:symbol x) i]))) (:pattern node))
                          node (update node :pattern
                                       (fn [terms]
                                         (mapv (fn [x]
                                                 (if (and (instance? Variable x) (contains? literals (:symbol x)))
                                                   (dp/->Constant (get bindings (:symbol x))) x)) terms)))
                          node (vary-meta node assoc ::literal-terms literal-terms)
                          source (source-symbol node inherited)
                          entry (entries-by-source source)]
                      (ensure! entry)
                      (if-let [descriptor (:descriptor entry)]
                        (let [terms (mapv term (:pattern node))
                              clause {:id (next-occurrence!) :kind :pattern :terms terms}
                              lowered (lowering/lower! runtime descriptor clause bindings outside)
                              _ (vswap! children conj (:close lowered))
                              branches (:branches lowered)
                              append! (fn [region branch]
                                        (reserve-node! 1024)
                                        (swap! regions update-in [region :available-vars]
                                               into (contract/variables (:clause branch)))
                                        (swap! regions update-in [region :patterns] conj
                                               (assoc branch :source-symbol source :origin path)))]
                          (cond
                            (:empty-proof lowered)
                            (do (swap! regions assoc-in [id :empty?] true)
                                (site! {:kind :empty :path path :parent-region id :proof (:empty-proof lowered)}))
                            (= :fixed (:mode lowered)) (append! id (first branches))
                            :else
                            (let [child-ids
                                  (mapv (fn [i branch]
                                          (check!)
                                          (let [restored (set (keys (:restore branch)))
                                                child (region! (conj path :attribute i) outside
                                                               (set/difference (node-vars node) restored)
                                                               bindings literals inherited)]
                                            (append! child (assoc-in branch [:clause :id] (next-occurrence!)))
                                            child)) (range) branches)]
                              (site! {:kind :attribute :path path :node node :parent-region id
                                      :mode (:mode lowered) :attribute-variable (:attribute-variable lowered)
                                      :exports (node-vars node) :required-inputs (if (= :dispatch (:mode lowered))
                                                                            #{(:attribute-variable lowered)} #{})
                                      :branches child-ids}))))
                        (add-core! id node path))))
                  (body! [clauses path inherited input-vars live bindings literals stack depth]
                    (check!)
                    (when (> (long depth) 128) (fail! :query-source/limit :planning))
                    (reserve-node! (+ 1024 (* 256 (count clauses))))
                    (let [id (region! path input-vars live bindings literals inherited)
                          variables (mapv node-vars clauses)
                          producers (mapv (fn [node vs]
                                            (cond (or (instance? Pattern node) (instance? RuleExpr node)
                                                      (instance? Or node)) vs
                                                  (instance? Function node) (node-vars (:binding node))
                                                  :else #{})) clauses variables)]
                      (swap! regions update-in [id :available-vars] #(reduce set/union % variables))
                      (doseq [[i node] (map-indexed vector clauses)]
                        (check!)
                        (reserve-node! 1024)
                        (let [location (conj path i)
                              source (source-symbol node inherited)
                              outside (reduce-kv (fn [s j vs] (if (= i j) s (set/union s vs))) input-vars variables)]
                          (cond
                            (instance? Pattern node) (pattern! id node location inherited outside bindings literals)
                            (instance? And node)
                            ;; And is grouping, so its clauses belong to this same
                            ;; conjunction. The parser only emits And under Or.
                            (fail! :query-source/unsupported :clauses)
                            (instance? Or node)
                            (let [exports (node-vars node)
                                  required (into #{} (map :symbol) (get-in node [:rule-vars :required]))
                                  inputs (set/intersection outside exports)
                                  branch-ids
                                  (mapv (fn [j branch]
                                          (body! (if (instance? And branch) (:clauses branch) [branch])
                                                 (conj location :branch j) source inputs exports
                                                 (select-keys bindings inputs) (set/intersection literals inputs)
                                                 stack (inc (long depth))))
                                        (range) (:clauses node))]
                              (site! {:kind :union :path location :node node :parent-region id :exports exports
                                      :required-inputs required :branches branch-ids})
                              (when (every? #(:empty? (nth @regions %)) branch-ids)
                                (swap! regions assoc-in [id :empty?] true)))
                            (instance? RuleExpr node)
                            (let [name (get-in node [:name :symbol]) rule (rules-by-name name)
                                  key [source name]]
                              (ensure! rule)
                              (if (or (contains? (:rule-relations compiler-context) key)
                                      (and (not (external? node inherited (volatile! #{}) 0))
                                           (native-core? node inherited #{} 0)))
                                (add-core! id node location)
                                (do
                                  (when (contains? stack key) (fail! :query-source/unsupported :planning))
                                  (let [args (mapv term (:args node))
                                        branches
                                        (mapv (fn [j branch]
                                                (let [head (head-vars branch)
                                                      _ (ensure! (= (count head) (count args)))
                                                      mapping (zipmap head args)
                                                      inputs (into #{} (keep (fn [[v [tag x]]]
                                                                             (when (or (= :const tag)
                                                                                       (and (= :var tag)
                                                                                            (contains? outside x))) v))) mapping)
                                                      required (into #{} (map :symbol) (get-in branch [:vars :required]))
                                                      constants (into {} (keep (fn [[v [tag x]]]
                                                                                  (cond (= :const tag) [v x]
                                                                                        (and (= :var tag) (contains? bindings x))
                                                                                        [v (get bindings x)]))) mapping)
                                                      literal-vars (into #{} (keep (fn [[v [tag x]]]
                                                                                    (when (or (= :const tag)
                                                                                              (and (= :var tag) (contains? literals x))) v))) mapping)
                                                      child (body! (:clauses branch) (conj location :rule-branch j)
                                                                   source inputs (set head) constants literal-vars
                                                                   (conj stack key) (inc (long depth)))]
                                                  {:region-id child :head-vars head :arguments mapping
                                                   :required-inputs required :input-vars inputs}))
                                              (range) (:branches rule))]
                                    (site! {:kind :rule :path location :node node :rule rule :parent-region id
                                            :exports (node-vars node) :branches branches})
                                    (when (every? #(:empty? (nth @regions (:region-id %))) branches)
                                      (swap! regions assoc-in [id :empty?] true))))))
                            (instance? Not node)
                            (do
                              (when (external? node inherited (volatile! #{}) 0)
                                (fail! :query-source/unsupported :clauses))
                              (let [form (dp/source node)
                                    form (if (instance? DefaultSrc (:source node)) form (rest form))
                                    explicit? (= "not-join" (some-> (first form) name))
                                    positive (reduce-kv (fn [s j vs] (if (= i j) s (set/union s vs))) input-vars producers)
                                    inputs (if explicit? (node-vars node) (set/intersection positive (node-vars node)))]
                                (when (empty? inputs) (fail! :query-source/unsupported :planning))
                                (add-core! id (vary-meta node assoc ::negation-inputs inputs
                                                           ::negation-literals (set/intersection literals inputs)) location)))
                            :else (add-core! id node location))))
                      id))]
            (let [live (node-vars [(:qfind parsed) (:qwith parsed)])
                  root (body! (:qwhere parsed) [] (or (:default-source compiler-context) '$)
                              input-vars live bindings (or (:literal-vars compiler-context) #{}) #{} 0)
                  _ (reserve-node! (+ 2048 (* 512 (count @regions))))
                  parents (into {} (mapcat (fn [site]
                                            (map (fn [branch]
                                                   [(if (map? branch) (:region-id branch) branch)
                                                    (:parent-region site)]) (:branches site)))) @sites)
                  resolved (reduce (fn [result r]
                                     (check!)
                                     (let [parent (get parents (:region-id r))]
                                       (conj result (assoc r :parent-region parent
                                                           :live? (and (not (:empty? r))
                                                                       (or (nil? parent)
                                                                           (:live? (nth result parent))))))))
                                   [] @regions)
                  held @tokens closes @children closed? (atom false)
                  owner {:control ctl :query-id (:query-id runtime) :closed? closed?
                         :descriptors (into {} (map (juxt :symbol :descriptor)) entries)}
                  close (rt/own! runtime #(do (reset! closed? true)
                                             (doseq [f (reverse closes)] (f))
                                             (doseq [token held] (control/release! token))))]
              (vreset! success? true)
              {:parsed parsed :root-region root :regions (mapv #(assoc % :owner owner) resolved)
               :sites @sites :rules rules :rule-relations (:rule-relations compiler-context) :close close})))
        (finally
          (when-not @success?
            (doseq [f (reverse @children)] (f))
            (doseq [token @tokens] (control/release! token))))))))

(defn predicate-clauses
  "Export only binary scalar predicates within this conjunction and visible
  fact variables. Captured scalar bindings may become constants; potential
  producers and values from another scope never become predicate constants."
  [region variables]
  (let [bindings (:bindings region)
        base (inc (reduce max -1 (map (comp :id :clause) (:patterns region))))]
    (into []
      (keep-indexed
        (fn [i {:keys [node]}]
          (let [operator ({'= := 'not= :not= '< :< '<= :<= '> :> '>= :>=} (get-in node [:fn :symbol]))
                args (:args node)]
            (when (and (instance? Predicate node) operator (= 2 (count args))
                       (every? #(or (instance? Variable %) (instance? Constant %)) args))
              (let [args (mapv #(if (and (instance? Variable %) (contains? bindings (:symbol %)))
                                 [:const (get bindings (:symbol %))] (term %)) args)
                    used (into #{} (keep #(when (= :var (first %)) (second %))) args)]
                (when (and (seq used) (set/subset? used variables))
                  (let [id (+' base i)]
                    (when (> id Long/MAX_VALUE) (fail! :query-source/limit :planning))
                    {:id id :kind :predicate :operator operator :semantics-version 1 :args args})))))))
      (:core-clauses region))))

(defn request!
  "Build one source's flat planning envelope from a region annotation and
  explicit typed exports supplied by the existing planner. Core nodes, rule
  bodies, original attribute patterns and other sources never enter it."
  [runtime region entry context available-inputs max-alternatives]
  (let [ctl (:control runtime) descriptor (:descriptor entry)
        owner (:owner region)
        _ (ensure! (and (identical? ctl (:control owner)) (= (:query-id runtime) (:query-id owner))
                        (not @(:closed? owner))
                        (identical? descriptor (get-in owner [:descriptors (:symbol entry)]))))
        scratch (control/reserve! ctl {:retained-bytes (+ 1024 (* 256 (count (:patterns region))))})]
    (try
      (let [patterns (filterv #(= (:source-symbol %) (:symbol entry)) (:patterns region))
            check! #(control/check-active! ctl)]
        (ensure! (and descriptor (seq patterns) (:live? region)
                      (identical? ctl (:control context)) (= (:query-id runtime) (:query-id context))
                      (every? #(contains? context %) [:spi-version :features :query-id :authorization
                                                      :consistency :control :limits])
                      (= (control/limits ctl) (:limits context))
                      (= 2 (:spi-version context)) (integer? max-alternatives) (<= 1 max-alternatives Long/MAX_VALUE)))
        (let [token (rt/charge-metadata! runtime available-inputs :planning)]
          (try
            (when (seq available-inputs) (contract/binding-columns! available-inputs check!))
            (ensure! (vector? available-inputs))
            (let [visible (:available-vars region)]
              (ensure! (every? #(contains? visible (:variable %)) available-inputs)))
            (finally (control/release! token))))
        (let [patterns (mapv :clause patterns)
              variables (into #{} (mapcat contract/variables) patterns)
              clauses (into patterns (predicate-clauses region variables))
              inputs (set (map :variable available-inputs))
              bindings (select-keys (:bindings region) (set/union variables inputs))
              output {:source-symbol (:symbol entry) :source-descriptor descriptor
                      :region-id (:region-id region) :scope-id (get-in descriptor [:scope :id])
                      :clauses clauses :bindings bindings :available-inputs available-inputs
                      ;; Keep normalized E/V variables through joins/residuals.
                      ;; Shared unused-variable rewrites may remove them before
                      ;; this pass; estimates cannot justify removing them.
                      :required-vars variables :max-alternatives max-alternatives
                      :demand {:quality :exact :ordering [] :offset 0 :limit nil}}
              token (rt/charge-copy! runtime (dissoc output :source-descriptor))]
          (try
            (let [owned (value/owned-copy (dissoc output :source-descriptor) check!)]
              (rt/own! runtime #(control/release! token))
              (merge (select-keys context [:spi-version :features :query-id :authorization :consistency :control :limits])
                     owned {:source-descriptor descriptor}))
            (catch Throwable e (control/release! token) (throw e)))))
      (finally (control/release! scratch)))))
