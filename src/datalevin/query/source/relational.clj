(ns ^:no-doc datalevin.query.source.relational
  "Optional whole-conjunction delegation with ordinary owned cursor batches."
  (:refer-clojure :exclude [run!])
  (:require [clojure.set :as set]
            [datalevin.parser :as parser]
            [datalevin.query.execute :as execute]
            [datalevin.query.source :as source]
            [datalevin.query.source.batch :as batch]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.diagnostics :as diagnostics]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]]
            [datalevin.spill :as spill])
  (:import [datalevin.parser Aggregate FindColl FindRel FindScalar FindTuple Pattern Predicate Variable]
           [java.io Closeable]
           [java.util List]))

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :planning)))
(defn- retain! [runtime token] (rt/own! runtime #(control/release! token)))

(defn- projection [element]
  (cond
    (instance? Variable element) {:kind :variable :variable (:symbol element)}
    (and (instance? Aggregate element) (= 1 (count (:args element)))
         (instance? Variable (first (:args element)))
         (#{'count 'count-distinct 'sum 'min 'max} (get-in element [:fn :symbol])))
    {:kind :aggregate :operator (keyword (name (get-in element [:fn :symbol])))
     :variable (:symbol (first (:args element)))}))

(defn- request [compilation input]
  (let [{:keys [entries compiled prepared]} compilation
        parsed (:parsed compiled) [root] (:regions compiled)
        elements (mapv projection (parser/find-elements (:qfind parsed)))
        sources (into {} (map (juxt :symbol :descriptor)) entries)
        patterns (mapv #(assoc (:clause %) :source-symbol (:source-symbol %)) (:patterns root))
        variables (into #{} (mapcat contract/variables) patterns)
        groups (set (map #(vector (:source-symbol %) (first (:terms %))) patterns))
        types (reduce (fn [out {:keys [source-symbol terms] :as clause}]
                        (reduce (fn [out [term column]]
                                  (if (= :var (first term))
                                    (update out (second term) #(if (or (nil? %) (= % (:type column)))
                                                               (:type column) ::mixed)) out))
                                out (map vector terms (contract/pattern-columns (sources source-symbol) clause))))
                      {} patterns)]
    (when (and (= 1 (count (:groups prepared))) (every? :descriptor entries)
               (= 1 (count (:regions compiled))) (empty? (:sites compiled))
               (empty? (:rules compiled)) (empty? (:qhaving parsed))
               (not (and (:qreturn-map parsed) (seq (:qorder parsed))))
               (nil? *math-context*)
               (every? #(or (instance? Pattern %) (instance? Predicate %)) (:qwhere parsed))
               (= 1 (.size ^List (get-in input [:relation :tuples])))
               (set/subset? (set (map :variable (:columns input))) (set (keys (:bindings input))))
               (seq patterns) (every? some? elements)
               (every? variables (concat (map :variable elements) (map :symbol (:qwith parsed))))
               (not-any? #{::mixed} (vals types))
               (or (> (count groups) 1) (some #(= :aggregate (:kind %)) elements)))
      ;; Every scalar operator must be represented. A function, ternary
      ;; predicate, scoped body, or unknown operator keeps the core path.
      (let [predicates (region/predicate-clauses root variables)]
        (when (= (count predicates) (count (:core-clauses root)))
          (merge (:request (first (:groups prepared)))
                 {:mode :relational :semantics-version 1
                  :sources (mapv #(select-keys % [:symbol :descriptor]) entries)
                  :clauses (into patterns predicates) :bindings (:bindings input)
                  :find elements :with (mapv :symbol (:qwith parsed))
                  :columns (mapv (fn [i {:keys [kind operator variable]}]
                                   {:variable (symbol (str "?relational_result_" i))
                                    :type (if (= kind :variable) (types variable)
                                              (if (#{:count :count-distinct} operator) :long :data))
                                    :nullable? (boolean (#{:min :max} operator))}) (range) elements)}))))))

(defn plan!
  "Try a pure, owned relational plan before ordinary acquisition. Declining
  incurs no reads and leaves the existing fact-access compiler untouched."
  [runtime compilation input]
  (let [provider (get-in compilation [:entries 0 :source])]
    (when (satisfies? source/IRelationalQuerySource provider)
      (when-let [request (request compilation input)]
        (let [request-token (rt/charge-copy! runtime (dissoc request :control))
              _ (retain! runtime request-token)
              response (rt/invoke runtime :planning #(source/-plan-relational provider request))]
          (when-not response (control/release! request-token))
          (when response
          (ensure! (and (value/plain-map? response) (= #{:plan :columns} (set (keys response)))
                        (value/plain-map? (:plan response)) (vector? (:columns response))
                        (= (:columns request) (:columns response))))
          (let [borrowed (control/reserve! (:control runtime) {:retained-bytes (value/value-charge response)})
                response (try
                           (retain! runtime (rt/charge-copy! runtime response))
                           (value/owned-copy response #(control/check-active! (:control runtime)))
                           (finally (control/release! borrowed)))
                groups (session/acquire! runtime (:prepared compilation))
                group (first groups)]
            (ensure! (satisfies? source/IRelationalQuerySession (:session group)))
            (when (:diagnostics runtime)
              (diagnostics/sources! runtime
                (mapv (fn [description]
                        (let [descriptor (:descriptor (first (filter #(= (:symbol %) (:source description))
                                                                     (:entries compilation))))]
                          (assoc description :definition-id (:definition-id descriptor)
                                             :definition-fingerprint (:definition-fingerprint descriptor)
                                             :scope-id (get-in descriptor [:scope :id]))))
                      (session/explain! runtime groups)))
              (diagnostics/region! runtime
                {:program-id 0 :region-id 0 :policy :relational-v1 :selection :provider
                 :steps [{:source-symbol (get-in compilation [:entries 0 :symbol])
                          :alternative-id "relational" :covers (set (map :id (:clauses request)))
                          :access {:kind :relational} :columns (:columns request)}]
                 :operators []}))
            {:group group :response response :request request :parsed (get-in input [:parsed])})))))))

(defn- result-query [parsed columns]
  (let [elements (mapv #(parser/->Variable (:variable %)) columns)
        positions (reduce (fn [m [i v]] (if (contains? m v) m (assoc m v i)))
                          {} (map-indexed vector (parser/find-vars (:qfind parsed))))
        find (:qfind parsed)
        find (if (or (instance? FindRel find) (instance? FindTuple find))
               (assoc find :elements elements) (assoc find :element (first elements)))]
    (assoc parsed :qfind find :qwith nil :qhaving nil
                  :qorder (when (:qorder parsed)
                            (mapv #(if (symbol? %) (positions %) %) (:qorder parsed))))))

(defn run!
  "Consume an exact relational cursor, enforce batch/result quotas, and use
  the shared result processor for public shapes, maps, and result windows."
  [runtime {:keys [group response request parsed]}]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        columns (:columns response) vars (mapv :variable columns)
        descriptor (get-in request [:sources 0 :descriptor])
        open (merge (:context group)
                    {:source-symbol (get-in request [:sources 0 :symbol]) :region-id 0
                     :scope-id (get-in descriptor [:scope :id]) :alternative-id "relational"
                     :definition-fingerprint (:definition-fingerprint descriptor)
                     :plan (:plan response) :columns columns})
        alternative {:id "relational" :scope-id (:scope-id open)
                     :properties {:replay :identical :ordering [] :blocking-state :none}}
        trace (diagnostics/cursor! runtime open false)
        permission (permissions/reserve-open! (:consumption group) alternative open)
        close-permission (rt/own! runtime #(.close ^Closeable permission))
        primary (volatile! nil) close-cursor (volatile! nil)]
    (try
      (let [cursor (rt/invoke runtime :open #(source/-open-relational-cursor (:session group) open))]
        (ensure! (satisfies? source/IQuerySourceCursor cursor))
        (vreset! close-cursor (rt/own! runtime #(source/-close-cursor! cursor)))
        (let [collected (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE})
              fetch {:control ctl :max-rows (get-in request [:limits :batch-rows])
                     :max-bytes (get-in request [:limits :batch-bytes])}]
          (retain! runtime (control/reserve! ctl {:retained-bytes 4096}))
          (loop []
            (let [batch (batch/accept! runtime columns false fetch
                          (rt/invoke runtime :fetch #(source/-next-batch! cursor fetch)) identity)]
              (try
                (doseq [row (:rows batch)]
                  (check!)
                  (retain! runtime (rt/charge-copy! runtime row))
                  (retain! runtime (control/reserve! ctl {:retained-bytes 128}))
                  (.cons ^clojure.lang.IPersistentCollection collected (value/owned-copy row check!)))
                (diagnostics/batch! runtime trace (:row-count batch) (:byte-count batch))
                (finally (control/release! (:reservation batch))))
              (when-not (:exhausted? batch) (recur))))
          (when (:diagnostics runtime)
            (diagnostics/metrics! runtime trace (rt/invoke runtime :metrics #(source/-cursor-metrics cursor))))
          (let [query (result-query parsed columns)
                raw ((execute/result-processor query)
                     query {:result-set collected :terminal-collected-symbols vars})
                own! (fn [row]
                       (retain! runtime (rt/charge-result! runtime row))
                       (retain! runtime (rt/charge-copy! runtime row))
                       (value/owned-copy row check!))
                find (:qfind parsed)]
            (check!)
            (cond
              (instance? FindScalar find) (when (seq collected) (first (own! [raw])))
              (instance? FindTuple find) (when (some? raw) (own! raw))
              (instance? FindColl find) (mapv #(first (own! [%])) raw)
              (set? raw) (into (spill/new-spillable-set nil {:spill-threshold Long/MAX_VALUE}) (map own!) raw)
              :else (mapv own! raw)))))
      (catch Throwable e (vreset! primary e) (throw e))
      (finally
        (try
          (when-let [close @close-cursor] (close))
          (catch Throwable e (if-let [error @primary] (.addSuppressed ^Throwable error e) (throw e)))
          (finally (close-permission)))))))
