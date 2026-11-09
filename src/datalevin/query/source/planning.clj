(ns ^:no-doc datalevin.query.source.planning
  "Owned capture and planning lifecycle for the revised contract. The shared
  compiler supplies actual typed region interfaces before acquisition."
  (:require [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.access :as access]
            [datalevin.query.source.binding :as binding]
            [datalevin.query.source.collection :as collection]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.cursor :as cursor]
            [datalevin.query.source.descriptor :as descriptor]
            [datalevin.query.source.envelope :as envelope]
            [datalevin.query.source.estimation :as estimation]
            [datalevin.query.source.region :as region]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.scope :as scope]
            [datalevin.query.source.session :as session]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [java.util IdentityHashMap]))

(defrecord ^:private Compilation [runtime entries compiled prepared state close])
(defrecord ^:private Planned [compilation groups schedules])

(defn- ensure! [ok] (when-not ok (fail! :query-source/invalid :planning)))

(defn- own-syntax! [runtime syntax]
  ;; Parser nodes and their :source metadata can both retain caller-owned
  ;; constants. Own them before the first descriptor/acquisition callback.
  (let [tokens (volatile! []) success? (volatile! false)
        check! #(control/check-active! (:control runtime))
        own! #(do (vswap! tokens conj %) %)]
    (try
      (letfn [(copy [x depth]
                (check!)
                (when (> (long depth) 128) (fail! :query-source/limit :planning))
                (cond
                  (parser/-traversable? x)
                  (do
                    (own! (control/reserve! (:control runtime) {:retained-bytes 1024}))
                    (let [node (reduce-kv (fn [node key value] (assoc node key (copy value (inc (long depth))))) x x)
                          metadata (meta x)]
                      (own! (rt/charge-copy! runtime metadata))
                      (with-meta node (value/owned-copy metadata check!))))
                  (and (vector? x) (some parser/-traversable? x))
                  (do (own! (control/reserve! (:control runtime) {:retained-bytes (+ 256 (* 64 (count x)))}))
                      (mapv #(copy % (inc (long depth))) x))
                  :else (do (own! (rt/charge-copy! runtime x)) (value/owned-copy x check!))))]
        (let [owned (copy syntax 0) held @tokens]
          (rt/own! runtime #(doseq [token held] (control/release! token)))
          (vreset! success? true)
          owned))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))

(defn- close-after-error! [runtime close error]
  (let [primary (rt/redacted-error runtime (or (:phase (ex-data error)) :planning) error)]
    (control/cancel! (:control runtime) (select-keys (ex-data primary) [:error :phase]))
    (try (close) (catch Throwable secondary (.addSuppressed ^Throwable primary secondary)))
    (throw primary)))

(defn capture!
  "Capture external descriptors once per provider object, validate all alias
  requirements, and annotate parsed clauses/rules before any acquisition.
  entries contain :symbol and exactly one of :source (external) or :native
  (DB/local collection). Local facts are owned before descriptor callbacks.
  The returned compilation owns its resources independently of other work."
  ([runtime entries parsed rules bindings input-vars options]
   (capture! runtime entries parsed rules bindings input-vars options nil))
  ([runtime entries parsed rules bindings input-vars options compiler-context]
    (let [child (assoc runtime :cleanup (atom [])) state (atom :capturing)
          close (rt/own! runtime #(do (when (= :planning @state)
                                       (control/cancel! (:control child)
                                         {:error :query-source/cancelled :phase :planning}))
                                     (reset! state :closed)
                                     (let [primary (volatile! nil)]
                                       (doseq [cleanup (reverse @(:cleanup child))]
                                         (try (cleanup)
                                              (catch Throwable e
                                                (if-let [error @primary]
                                                  (.addSuppressed ^Throwable error e)
                                                  (vreset! primary e)))))
                                       (reset! (:cleanup child) [])
                                       (when-let [error @primary] (throw error)))))]
      (try
        (control/check-active! (:control runtime))
        (ensure! (vector? entries))
        (let [parsed (own-syntax! child parsed)
              rules (own-syntax! child rules)
              compiler-context
              (when compiler-context
                (let [token (rt/charge-metadata! child compiler-context :planning)]
                  (rt/own! child #(control/release! token))
                  (value/owned-copy compiler-context #(control/check-active! (:control child)))))
              token (control/reserve! (:control runtime) {:retained-bytes (+ 2048 (* 1024 (count entries)))})
              _ (rt/own! child #(control/release! token))
              _ (doseq [entry entries]
                  (ensure! (and (value/plain-map? entry)
                                (= #{:symbol (if (contains? entry :source) :source :native)} (set (keys entry)))
                                (symbol? (:symbol entry)) (.startsWith (name (:symbol entry)) "$"))))
              entries (collection/capture-entries! child entries)
              seen (IdentityHashMap.)
              entries
              (mapv (fn [entry]
                      (ensure! (and (value/plain-map? entry)
                                    (= #{:symbol (if (contains? entry :source) :source :native)} (set (keys entry)))
                                    (symbol? (:symbol entry)) (.startsWith (name (:symbol entry)) "$")))
                      (rt/with-context child {:source-symbol (:symbol entry)}
                        (fn [runtime]
                          (if (contains? entry :source)
                            (let [provider (:source entry)
                                  info (or (.get seen provider)
                                           (let [info (descriptor/capture! runtime provider)]
                                             (.put seen provider info) info))]
                              (assoc entry :descriptor info))
                            (do (ensure! (some? (:native entry))) entry))))) entries)
              prepared (session/prepare! child entries options)
              compiled (region/annotate! child parsed entries rules bindings input-vars compiler-context)]
          (reset! state :captured)
          (->Compilation child entries compiled prepared state close))
        (catch Throwable e (close-after-error! child close e))))))

(defn refine!
  "Apply engine-owned typed boundary proofs while capture is still pure. The
  returned compilation shares the original lifecycle; requests and acquisition
  have not started. No source estimates or provider alternatives prove empty."
  [runtime compilation boundaries]
  (ensure! (and (instance? Compilation compilation)
                (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))
                (= :captured @(:state compilation))))
  (let [{:keys [runtime compiled close]} compilation]
    (try
      (assoc compilation :compiled (scope/refine-regions! runtime compiled boundaries))
      (catch Throwable e (close-after-error! runtime close e)))))

(defn plan!
  "Plan every live source/region pair once, then audit selected coverage and
  consumption for the whole query before returning executable AccessPlans.
  interfaces! is an engine compiler callback (runtime, compiled -> region map).
  Every live region must publish actual :inputs, captured :operators, and a
  :core map from each private clause/site path to its implementing operator ID.
  Only explicit typed exports enter requests. It must perform no source I/O.
  Branch evaluation and the emission of these shared interfaces remain the
  compiler's responsibility. On failure all compilation resources close now."
  [runtime compilation interfaces!]
  (ensure! (and (instance? Compilation compilation)
                (identical? (:control runtime) (get-in compilation [:runtime :control]))
                (= (:query-id runtime) (get-in compilation [:runtime :query-id]))))
  (let [{:keys [runtime entries compiled prepared state close]} compilation]
    (control/check-active! (:control runtime))
    (ensure! (compare-and-set! state :captured :planning))
    (try
      (let [live (filterv :live? (:regions compiled))
            interfaces (interfaces! runtime compiled)
            _ (ensure! (and (value/plain-map? interfaces)
                            (= (set (map :region-id live)) (set (keys interfaces)))))
            ;; Build and validate all requests while acquisition is still pure.
            prepared-groups (into {} (mapcat (fn [group] (map #(vector % group) (:aliases group)))) (:groups prepared))
            regions
            (mapv
              (fn [r]
                (let [{:keys [inputs operators core] :as interface} (interfaces (:region-id r))
                      _ (ensure! (and (value/plain-map? interface)
                                      (= #{:inputs :operators :core} (set (keys interface)))
                                      (value/plain-map? core) (vector? operators)))
                      token (rt/charge-metadata! runtime core :planning)
                      _ (rt/own! runtime #(control/release! token))
                      paths (into (set (map :path (:core-clauses r)))
                                  (comp (filter #(= (:region-id r) (:parent-region %))) (map :path)) (:sites compiled))
                      _ (when-not (= paths (set (keys core))) (fail! :query-source/unsupported :planning))
                      _ (ensure! (every? (set (map :id operators)) (vals core)))
                      available (access/available-inputs! runtime r inputs operators)
                      sources (set (map :source-symbol (:patterns r)))
                      requests (into []
                                     (keep (fn [entry]
                                             (when (and (:descriptor entry) (contains? sources (:symbol entry)))
                                               (let [context (:request (prepared-groups (:symbol entry)))
                                                     request (region/request! runtime r entry context available
                                                                (get-in prepared [:planning :max-alternatives]))]
                                                 (envelope/request! runtime request))))) entries)]
                  {:region r :inputs inputs :operators operators :requests requests})) live)
            groups (session/acquire! runtime prepared)
            by-alias (into {} (mapcat (fn [group] (map #(vector % group) (:aliases group)))) groups)
            estimate-work (estimation/prepare! runtime regions by-alias (:planning prepared))
            ids (delay (binding/id-space! runtime))
            ;; Capture every returned candidate before running any selection.
            responses
            (mapv
              (fn [{:keys [requests] :as r}]
                (assoc r :responses
                       (mapv (fn [request]
                               (let [alias (:source-symbol request) group (by-alias alias)
                                     entry (some #(when (= alias (:symbol %)) %) (:entries group))]
                                 (rt/with-context runtime {:source-symbol alias :region-id (:region-id request)}
                                   (fn [runtime]
                                     {:request request :group group
                                      :response (envelope/capture-response! runtime request
                                                  (rt/invoke runtime :planning
                                                    #(source/-plan-source (:source entry) (:session group) request))
                                                  (:consumption group))})))) requests))) regions)
            all-candidates (volatile! [])
            schedules
            (mapv (fn [{:keys [region inputs operators requests responses]}]
                    (let [candidates
                          (into [] (mapcat (fn [{:keys [request group response]}]
                                            (let [method (cursor/method runtime group ids)]
                                              (mapv #(access/candidate! runtime request % (:consumption group) method)
                                                    (envelope/alternatives runtime response))))) responses)]
                      (vswap! all-candidates into candidates)
                      (assoc (access/schedule! runtime (:region-id region) inputs requests candidates operators
                                               (:planning prepared))
                             :estimation (get estimate-work (:region-id region))))) responses)]
        (access/validate-query! runtime schedules)
        (let [identity-key #(select-keys (get-in % [:path :options]) [:source-symbol :region-id :alternative-id])
              selected (into #{} (map identity-key) (mapcat :plans schedules))]
          (doseq [candidate @all-candidates :when (not (contains? selected (identity-key candidate)))]
            (access/release-discarded! candidate)))
        (doseq [[{:keys [responses]} schedule] (map vector responses schedules)
                {:keys [request response]} responses]
          (envelope/select! runtime response
            (into [] (comp (filter #(= (:source-symbol request) (get-in % [:expr :source])))
                           (map #(get-in % [:path :options :alternative-id]))) (:plans schedule))))
        (reset! state :planned)
        (->Planned compilation groups schedules))
      (catch Throwable e (close-after-error! runtime close e)))))

(defn schedules
  "Borrow validated shared schedules while their compilation remains open."
  [runtime planned]
  (ensure! (and (instance? Planned planned)
                (identical? (:control runtime) (get-in planned [:compilation :runtime :control]))
                (= (:query-id runtime) (get-in planned [:compilation :runtime :query-id]))
                (= :planned @(get-in planned [:compilation :state]))))
  (control/check-active! (:control runtime))
  (:schedules planned))
