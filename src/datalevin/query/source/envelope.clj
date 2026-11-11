(ns ^:no-doc datalevin.query.source.envelope
  "Revised flat EAV planning envelopes. Only explicit :plan handles are opaque.
  Public source execution admits captured envelopes through this boundary."
  (:require [clojure.set :as set]
            [datalevin.query.access.dependency :as dependency]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.descriptor :as descriptor]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [datalevin.utl QueryKey]))

(def ^:private common-fields
  #{:spi-version :features :query-id :authorization :consistency :control :limits})
(def ^:private request-fields
  (into common-fields [:source-symbol :source-descriptor :region-id :scope-id :clauses
                       :bindings :available-inputs :required-vars :demand :max-alternatives]))
(def ^:private scan-numeric
  #{:startup-ns :run-ns :rows :row-bytes :requests-per-open :transfer-bytes :peak-retained-bytes})
(def ^:private lookup-numeric
  #{:startup-ns :per-binding-ns :rows-per-binding :row-bytes :requests-per-open
    :requests-per-binding :transfer-bytes-per-binding :peak-retained-bytes})

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :planning)))
(defn- fields! [m required optional]
  (ensure! (and (value/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (required %) (optional %) (qualified-keyword? %)) (keys m)))))
(defn- variable? [x] (and (symbol? x) (.startsWith (name x) "?")))
(defn- id? [x] (or (and (keyword? x) (seq (name x))) (and (string? x) (seq x))))
(defn- nonnegative? [x] (and (integer? x) (<= 0 x Long/MAX_VALUE)))
(defn- positive? [x] (and (nonnegative? x) (pos? (long x))))
(defn- columns! [columns check!]
  (ensure! (vector? columns))
  (when (seq columns) (contract/binding-columns! columns check!))
  columns)
(defn- by-variable [columns] (into {} (map (juxt :variable identity)) columns))

(defn- predicate-term! [term check!]
  (check!)
  (ensure! (and (vector? term) (= 2 (count term))))
  (case (first term)
    :var (ensure! (variable? (second term)))
    :const (value/value-charge (second term) (fn [_] (check!)))
    (fail! :query-source/invalid :planning)))

(defn- pattern-types [request clauses check!]
  (reduce (fn [types clause]
            (if (= :pattern (:kind clause))
              (reduce (fn [types [term column]]
                        (if (= :var (first term))
                          (let [variable (second term)]
                            (when-let [previous (get types variable)]
                              (ensure! (= (:type previous) (:type column))))
                            (assoc types variable
                                   (assoc column :variable variable
                                          :nullable? (and (:nullable? column)
                                                          (get-in types [variable :nullable?] true)))))
                          types)) types
                      (map vector (:terms clause) (contract/pattern-columns (contract/pattern-source-info request clause) clause check!)))
              types)) {} clauses))

(defn- request*!
  [runtime request descriptor!]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        {:keys [source-symbol source-descriptor region-id scope-id clauses bindings available-inputs
                required-vars demand max-alternatives]} request]
    (check!)
    (fields! request request-fields #{})
    (ensure! (and (= 2 (:spi-version request)) (identical? ctl (:control request))
                  (= (:query-id runtime) (:query-id request)) (= (control/limits ctl) (:limits request))
                  (#{:snapshot :statement :best-effort} (:consistency request))
                  (set? (:features request)) (every? keyword? (:features request))
                  (symbol? source-symbol) (.startsWith (name source-symbol) "$")
                  (nonnegative? region-id) (= scope-id (get-in source-descriptor [:scope :id]))
                  (vector? clauses) (seq clauses) (value/plain-map? bindings)
                  (every? variable? (keys bindings)) (set? required-vars) (every? variable? required-vars)
                  (positive? max-alternatives)
                  (= {:quality :exact :ordering [] :offset 0 :limit nil} demand)))
    (descriptor! source-descriptor check!)
    (ensure! (and (set/subset? (:features request) (:features source-descriptor))
                  (set/subset? (:required-features source-descriptor) (:features request))))
    (when-let [sources (:datalevin.query.source/region-sources request)]
      (ensure! (and (vector? sources) (<= 2 (count sources) 32)
                    (= (count sources) (count (set (map :symbol sources))))))
      (doseq [{:keys [symbol descriptor] :as entry} sources]
        (fields! entry #{:symbol :descriptor} #{})
        (ensure! (and (symbol? symbol) (.startsWith (name symbol) "$")))
        (descriptor! descriptor check!)
        (ensure! (= (select-keys source-descriptor [:source-instance-id :compute-context-id])
                    (select-keys descriptor [:source-instance-id :compute-context-id]))))
      (ensure! (identical? source-descriptor
                          (some #(when (= source-symbol (:symbol %)) (:descriptor %)) sources))))
    (columns! available-inputs check!)
    (ensure! (and (every? #(nonnegative? (:id %)) clauses)
                  (= (count clauses) (count (set (map :id clauses))))))
    (doseq [clause clauses]
      (check!)
      (case (:kind clause)
        :pattern
        (do (fields! clause #{:id :kind :terms} (if (:datalevin.query.source/region-sources request) #{:source-symbol} #{}))
            (doseq [[term column] (map vector (:terms clause)
                                      (contract/pattern-columns (contract/pattern-source-info request clause) clause check!))]
              (when (= :const (first term))
                ;; A constant is a predicate operand, not a provider output.
                ;; Numeric/sequence representations can compare equal without
                ;; satisfying the declared representation of returned facts.
                (ensure! (not (value/disjoint-constant? (:type column) (second term) check!))))))
        :predicate
        (do (fields! clause #{:id :kind :operator :semantics-version :args} #{})
            (when-not (and (#{:= :not= :< :<= :> :>= :like :not-like :like-any :in :not-in} (:operator clause))
                           (= 1 (:semantics-version clause)))
              (fail! :query-source/unsupported :planning))
            (ensure! (and (vector? (:args clause)) (= 2 (count (:args clause)))))
            (when (= :like-any (:operator clause))
              (ensure! (contains? (:datalevin.query.source/predicate-prefilters source-descriptor) :like-any)))
            (doseq [term (:args clause)] (predicate-term! term check!)))
        (fail! :query-source/invalid :planning)))
    (doseq [x (vals bindings)] (value/value-charge x (fn [_] (check!))))
    (let [visible (into (set (map :variable available-inputs)) (mapcat contract/variables)
                        (filter #(= :pattern (:kind %)) clauses))]
      (ensure! (and (some #(= :pattern (:kind %)) clauses)
                    (set/subset? (set (keys bindings)) visible)
                    (set/subset? required-vars visible)))
      (doseq [clause clauses] (ensure! (set/subset? (contract/variables clause) visible))))
    request))

(defn request!
  "Validate an admitted request and its descriptors at the public boundary."
  [runtime request]
  (request*! runtime request descriptor/source-info!))

(defn captured-request!
  "Validate a core-built request using descriptors already owned and validated
  by capture!. The supplied entries must be the compilation/session entries;
  require object identity, so a replacement descriptor cannot bypass checks."
  [runtime request entries]
  (request*! runtime request
    (fn [info check!]
      (check!)
      (ensure! (some #(identical? info (:descriptor %)) entries)))))

(defn estimate!
  "Validate the two revised estimate shapes without replacing unknowns or
  converting them into native optimizer scores. Staleness is advisory."
  [estimate dependent? check!]
  (let [numeric (if dependent? lookup-numeric scan-numeric)]
    (fields! estimate (into numeric [:confidence :provenance]) #{})
    (ensure! (#{:unknown :low :medium :high} (:confidence estimate)))
    (doseq [key numeric]
      (check!)
      (let [n (get estimate key)]
        (ensure! (or (nil? n)
                     (and (number? n)
                          (cond (instance? Double n) (Double/isFinite (double n))
                                (instance? Float n) (Float/isFinite (float n))
                                :else true)
                          (not (neg? (compare n 0))))))))
    (when (every? #(nil? (get estimate %)) numeric)
      (ensure! (= :unknown (:confidence estimate))))
    (let [provenance (:provenance estimate)]
      (fields! provenance #{:kind :id :observed-at-ms :valid-for-ms} #{})
      (ensure! (and (#{:exact :statistics :heuristic :unknown} (:kind provenance))
                    (or (nil? (:id provenance)) (string? (:id provenance)))
                    (every? #(or (nil? %) (nonnegative? %))
                            [(:observed-at-ms provenance) (:valid-for-ms provenance)]))))
    estimate))

(defn- alternative! [request alternative consumption check!]
  (fields! alternative #{:id :plan :covers :prefilters :requires :columns :properties :estimate :explain
                         :scope-id :completion :constraints :access} #{})
  (let [{:keys [covers prefilters requires columns properties constraints access]} alternative
        clauses (into {} (map (juxt :id identity)) (:clauses request))
        ids (set (keys clauses))]
    (ensure! (and (id? (:id alternative)) (some? (:plan alternative))
                  (= (:scope-id request) (:scope-id alternative)) (= :all-bindings (:completion alternative))
                  (set? covers) (set? prefilters) (set/subset? covers ids) (set/subset? prefilters ids)
                  (empty? (set/intersection covers prefilters))
                  (every? #(= :predicate (:kind (clauses %))) prefilters)))
    (columns! requires check!)
    (columns! columns check!)
    (fields! properties #{:replay :ordering :blocking-state} #{:consumption-group})
    (ensure! (and (#{:identical :unstable} (:replay properties))
                  (= [] (:ordering properties)) (= :none (:blocking-state properties))))
    (fields! constraints #{:max-binding-rows :max-binding-bytes :max-request-binding-rows} #{})
    (ensure! (every? positive? (map constraints [:max-binding-rows :max-binding-bytes :max-request-binding-rows])))
    (fields! access #{:kind :anchor-clause} #{})
    (let [patterns (filterv #(= :pattern (:kind %)) (map clauses covers))
          pattern-ids (set (map :id patterns))
          anchor (clauses (:anchor-clause access))
          available (by-variable (:available-inputs request))
          required (by-variable requires) output (by-variable columns)
          types (pattern-types request patterns check!)
          supplied (into (into (set (keys (:bindings request))) (keys required)) (keys output))
          covered-vars (into #{} (mapcat contract/variables) (map clauses covers))
          residual-vars (into #{} (mapcat contract/variables)
                              (remove #(contains? covers (:id %)) (:clauses request)))]
      (ensure! (and (seq patterns) (contains? pattern-ids (:anchor-clause access))
                    (#{:entity-lookup :value-lookup :attribute-enumeration :join} (:kind access))))
      (when (= :join (:kind access))
        (ensure! (and (:datalevin.query.source/region-sources request) (seq requires)
                      (> (count (set (map :source-symbol patterns))) 1))))
      (when (and (not= :join (:kind access)) (> (count patterns) 1))
        (let [[tag x] (first (:terms anchor))
              entity-type (:type (first (contract/pattern-columns (contract/pattern-source-info request anchor) anchor check!)))]
          (ensure! (and (#{:var :const} tag)
                        (every? (fn [pattern]
                                  (let [[other-tag y] (first (:terms pattern))]
                                    (and (= tag other-tag)
                                         (= entity-type (:type (first (contract/pattern-columns
                                                                        (contract/pattern-source-info request pattern) pattern check!))))
                                         (if (= :const tag) (QueryKey/equiv x y) (= x y))))) patterns)))))
      (doseq [[variable column] required]
        (check!)
        (ensure! (dependency/supplies? (get available variable) column)))
      (doseq [[variable column] output]
        (check!)
        (let [expected (or (get types variable) (get required variable))]
          (ensure! (and expected (= (:type expected) (:type column))
                        (= (:nullable? expected) (:nullable? column))))))
      (ensure! (set/subset? (set/intersection covered-vars (set/union (:required-vars request) residual-vars)) supplied))
      (doseq [id (set/union covers prefilters) :let [clause (clauses id)] :when (= :predicate (:kind clause))]
        (ensure! (set/subset? (contract/variables clause) supplied)))
      (when-let [position (case (:kind access) :entity-lookup 0 :value-lookup 2 nil)]
        (let [[tag x] (nth (:terms anchor) position)]
          (ensure! (or (= :const tag) (and (= :var tag)
                                          (or (contains? (:bindings request) x) (contains? required x))))))))
    (permissions/planning-permission consumption alternative)
    (estimate! (:estimate alternative) (boolean (seq requires)) check!)
    (fields! (:explain alternative) #{:label} #{})
    (ensure! (and (string? (get-in alternative [:explain :label])) (seq (get-in alternative [:explain :label]))))
    alternative))

(defrecord ^:private CapturedResponse [control query-id state])

(defn capture-response!
  "Admit and validate every candidate, including discarded vector backing.
  Capture detached envelopes while leaving :plan handles opaque and borrowed.
  No cursor, request permit or one-shot consumption is acquired here."
  [runtime request response consumption]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        transient (volatile! []) copies (volatile! []) container (volatile! nil) success? (volatile! false)
        admit! (fn [metadata overhead]
                 (vswap! transient conj (rt/charge-metadata! runtime metadata :planning overhead)))]
    (try
      (request! runtime request)
      (fields! response #{:alternatives} #{})
      (let [alternatives (:alternatives response)]
        (ensure! (and (vector? alternatives) (<= (count alternatives) (long (:max-alternatives request)))))
        (admit! (dissoc response :alternatives) 512)
        ;; A subvector's hidden alternatives remain reachable until capture.
        ;; Inspect every retained parent; only each explicit plan slot is opaque.
        (loop [items alternatives]
          (admit! (meta items) (+ 320 (* 64 (count items))))
          (doseq [alternative items]
            (ensure! (value/plain-map? alternative))
            (admit! (dissoc alternative :plan) 512))
          (when-let [parent (value/vector-parent items)] (recur parent)))
        (ensure! (= (count alternatives) (count (set (map :id alternatives)))))
        (doseq [alternative alternatives]
          (check!)
          (alternative! request alternative consumption check!))
        (vreset! container (control/reserve! ctl {:retained-bytes (+ 1024 (* 128 (count alternatives)))}))
        (let [owned (mapv (fn [alternative]
                            (let [metadata (dissoc alternative :plan)
                                  token (rt/charge-copy! runtime metadata)]
                              (vswap! copies conj token)
                              (assoc (value/owned-copy metadata check!) :plan (:plan alternative)))) alternatives)
              state (atom {:phase :captured :alternatives owned :container @container
                           :charges (zipmap (map :id owned) @copies)})
              captured (->CapturedResponse ctl (:query-id runtime) state)]
          (rt/own! runtime #(let [previous (first (swap-vals! state (constantly {:phase :closed})))]
                             (when-let [token (:container previous)] (control/release! token))
                             (doseq [token (vals (:charges previous))] (control/release! token))))
          (vreset! success? true)
          captured))
      (finally
        (doseq [token @transient] (control/release! token))
        (when-not @success?
          (when-let [token @container] (control/release! token))
          (doseq [token @copies] (control/release! token)))))))

(defn alternatives
  "Borrow captured envelopes until selection or runtime close."
  [runtime captured]
  (control/check-active! (:control runtime))
  (ensure! (and (instance? CapturedResponse captured) (identical? (:control runtime) (:control captured))
                (= (:query-id runtime) (:query-id captured)) (not= :closed (:phase @(:state captured)))))
  (:alternatives @(:state captured)))

(defn select!
  "Retain only selected envelopes and release discarded candidates. Selection
  is final; all response validation and complete-query audits precede this call."
  [runtime captured selected-ids]
  (let [candidates (alternatives runtime captured) state (:state captured)
        ids (set selected-ids) by-id (into {} (map (juxt :id identity)) candidates)]
    (ensure! (and (vector? selected-ids) (= (count selected-ids) (count ids))
                  (set/subset? ids (set (keys by-id)))))
    (let [before @state
          selected (mapv by-id selected-ids)
          container (if (= (count selected) (count candidates)) (:container before)
                        (do (control/release! (:container before))
                            (control/reserve! (:control runtime)
                              {:retained-bytes (+ 1024 (* 128 (count selected)))})))
          after {:phase :selected :alternatives selected :container container
                 :charges (select-keys (:charges before) ids)}]
      (try
        (ensure! (and (= :captured (:phase before)) (compare-and-set! state before after)))
        (catch Throwable e (control/release! container) (throw e)))
      (doseq [[id token] (:charges before) :when (not (contains? ids id))] (control/release! token))
      selected)))
