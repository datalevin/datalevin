(ns ^:no-doc datalevin.query.source.session
  "Captured alias requirements and grouped acquisition for the revised contract.
  Public source execution validates captured descriptors before acquisition.
  Native read contexts remain engine-owned."
  (:require [clojure.set :as set]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.descriptor :as descriptor]
            [datalevin.query.source.permissions :as permissions]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]])
  (:import [java.io Closeable]
           [java.util IdentityHashMap]))

(def ^:private rank {:best-effort 0 :statement 1 :snapshot 2})
(def ^:private common-keys
  [:spi-version :features :query-id :authorization :consistency :control :limits])

(defrecord Prepared [control query-id groups requirements native-requirements planning state])

(defn- ensure! [ok phase]
  (when-not ok (fail! :query-source/invalid phase)))

(defn- fields! [m required optional phase]
  (ensure! (and (value/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (required %) (optional %) (qualified-keyword? %)) (keys m))) phase))

(defn- source-symbol? [x]
  (and (symbol? x) (.startsWith (name x) "$")))

(defn- nonnegative? [x]
  (and (integer? x) (<= 0 x Long/MAX_VALUE)))

(defn- positive? [x]
  (and (nonnegative? x) (pos? (long x))))

(defn- at-least? [actual minimum]
  (>= (long (rank actual)) (long (rank minimum))))

(defn- supports? [capabilities minimum]
  (boolean (some #(at-least? % minimum) capabilities)))

(defn- strongest [requirements]
  (reduce #(if (at-least? %1 %2) %1 %2) :best-effort requirements))

(defn- options! [ctl options aliases]
  (fields! options #{} #{:limits :authorization :consistency :sources :planning} :context)
  (let [default (get options :consistency :snapshot)
        overrides (get options :sources {})
        planning (get options :planning {})]
    (ensure! (and (contains? rank default) (value/plain-map? overrides)
                  (set/subset? (set (keys overrides)) aliases)) :context)
    (doseq [[_ override] overrides]
      (fields! override #{:consistency} #{} :context)
      (ensure! (contains? rank (:consistency override)) :context))
    (fields! planning #{} #{:max-alternatives :estimate-max-age-ms :estimate-budget-ms :estimate-max-requests} :context)
    (ensure! (and (positive? (get planning :max-alternatives 32))
                  (nonnegative? (get planning :estimate-max-age-ms 300000))
                  (nonnegative? (get planning :estimate-budget-ms 100))
                  (nonnegative? (get planning :estimate-max-requests 8))) :context)
    (when (some? (:limits options))
      (let [limits (:limits options)]
        (ensure! (value/plain-map? limits) :context)
        (doseq [[k v] limits]
          (ensure! (and (contains? control/default-limits k) (nonnegative? v)
                        (or (not (#{:batch-rows :batch-bytes :binding-rows :binding-bytes
                                    :in-flight-batches :concurrent-requests} k)) (positive? v))
                        ;; The caller creates the runtime first. A stronger parent
                        ;; cap is fine; silently ignoring a requested cap is not.
                        (<= (long (get (control/limits ctl) k)) (long v))) :context))))
    {:requirements (into {} (map (fn [alias] [alias (get-in overrides [alias :consistency] default)])) aliases)
     :authorization (get options :authorization {:principal-id :application})
     :planning {:max-alternatives (get planning :max-alternatives 32)
                :estimate-max-age-ms (get planning :estimate-max-age-ms 300000)
                :estimate-budget-ms (get planning :estimate-budget-ms 100)
                :estimate-max-requests (get planning :estimate-max-requests 8)}}))

(defn validate-options!
  "Check already admitted invocation options against declared source aliases
  before consuming inputs or invoking descriptor callbacks. Acquisition repeats
  this pure check against the actual captured entries."
  [runtime options aliases]
  (options! (:control runtime) options aliases)
  nil)

(defn- logical-view [descriptor]
  ;; Ignore inert envelope annotations without erasing complete nominal types
  ;; or accepted scope parameters. Different logical views are kept separately.
  (let [{:keys [pattern-contract logical-schema scope completion]} descriptor]
    {:pattern-contract
     {:kind (:kind pattern-contract)
      :attributes (into {} (map (fn [[a t]] [a (select-keys t [:entity-type :value-type])]))
                        (:attributes pattern-contract))}
     :logical-schema
     {:domains (into {} (map (fn [[id d]] [id (select-keys d [:entity-type])])) (:domains logical-schema))
      :attributes (into {} (map (fn [[a d]] [a (select-keys d [:entity-domain :cardinality :absence
                                                              :reference-domain])]))
                        (:attributes logical-schema))}
     :scope (select-keys scope [:id :kind :parameters])
     :completion (select-keys completion [:domain :extent])}))

(defn- compatible-views! [runtime entries]
  (reduce (fn [accepted {:keys [descriptor]}]
            (let [id (:definition-id descriptor)
                  scope (:scope descriptor)
                  scope-view (select-keys scope [:kind :parameters])]
              (when-let [previous (get-in accepted [:scopes (:id scope)])]
                (ensure! (= previous scope-view) :descriptor))
              (if-let [previous (get-in accepted [:definitions id])]
                (do
                  (when-not (= (:definition-fingerprint previous) (:definition-fingerprint descriptor))
                    (fail! :query-source/stale-definition :descriptor))
                  (when-not (identical? previous descriptor)
                    (let [nodes (reduce + (for [d [previous descriptor]]
                                            (+ (count (get-in d [:pattern-contract :attributes]))
                                               (count (get-in d [:logical-schema :domains]))
                                               (count (get-in d [:logical-schema :attributes])))))
                          token (control/reserve! (:control runtime)
                                                 {:retained-bytes (+ 4096 (* 512 (long nodes)))})]
                      (try (ensure! (= (logical-view previous) (logical-view descriptor)) :descriptor)
                           (finally (control/release! token)))))
                  (assoc-in accepted [:scopes (:id scope)] scope-view))
                (-> accepted
                    (assoc-in [:definitions id] descriptor)
                    (assoc-in [:scopes (:id scope)] scope-view)))))
          {:definitions {} :scopes {}} entries))

(defn prepare!
  "Capture all alias minima and check every physical group before acquisition.
  Captured descriptors pass full v2 schema/type validation here. Descriptor and
  option metadata are admitted here; source/native objects are borrowed. The
  shared authorization is preserved, and overrides accept consistency only."
  [runtime entries options]
  (let [ctl (:control runtime) tokens (volatile! []) retained? (volatile! false)
        own-token! (fn [token] (vswap! tokens conj token) token)]
    (try
      (control/check-active! ctl)
      (ensure! (vector? entries) :inputs)
      (own-token! (control/reserve! ctl {:retained-bytes (+ 2048 (* 2048 (count entries)))}))
      (let [options (if (nil? options) {} options)
            transient (own-token! (rt/charge-metadata! runtime options :context 1024))
            raw-aliases (mapv :symbol entries)
            normalized (try
                         (ensure! (and (every? source-symbol? raw-aliases)
                                       (= (count raw-aliases) (count (set raw-aliases)))) :inputs)
                         (doseq [alias raw-aliases]
                           (own-token! (rt/charge-metadata! runtime alias :inputs)))
                         (options! ctl options (into #{} (map #(with-meta % nil)) raw-aliases))
                         (finally (control/release! transient)))
            _ (own-token! (rt/charge-copy! runtime normalized))
            {:keys [requirements authorization planning]}
            (value/owned-copy normalized #(control/check-active! ctl))
            descriptors (IdentityHashMap.)
            entries
            (mapv (fn [{:keys [symbol source descriptor native]}]
                    (control/check-active! ctl)
                    (let [symbol (with-meta symbol nil)]
                      (if descriptor
                        (do
                          (ensure! (and (nil? native) (satisfies? source/IQuerySource source)) :inputs)
                          (when-not (.containsKey descriptors descriptor)
                            (own-token! (rt/charge-metadata! runtime descriptor :descriptor))
                            (descriptor/source-info! descriptor #(control/check-active! ctl))
                            (.put descriptors descriptor true))
                          {:symbol symbol :source source :descriptor descriptor})
                        (do (ensure! (some? native) :inputs) {:symbol symbol :native native})))) entries)
            physical (group-by #(select-keys (:descriptor %) [:source-instance-id :compute-context-id])
                               (filter :descriptor entries))
            group-keys (into [] (distinct)
                             (map #(select-keys (:descriptor %) [:source-instance-id :compute-context-id])
                                  (filter :descriptor entries)))
            common {:spi-version 2 :features #{} :query-id (:query-id runtime) :authorization authorization
                    :control ctl :limits (control/limits ctl)}
            groups
            (mapv (fn [members]
                    (control/check-active! ctl)
                    (let [minimum (strongest (map #(requirements (:symbol %)) members))
                          features (reduce set/intersection #{} (map #(get-in % [:descriptor :features]) members))
                          _ (compatible-views! runtime members)]
                      (doseq [{:keys [descriptor]} members]
                        (when-not (and (supports? (:read-capabilities descriptor) minimum)
                                       (set/subset? (:required-features descriptor) features))
                          (fail! :query-source/unsupported :acquisition)))
                      {:aliases (mapv :symbol members) :entries members
                       :request (assoc common :features features :consistency minimum
                                              :source-descriptors (into [] (distinct) (map :descriptor members))
                                              :read-requirements (select-keys requirements (map :symbol members)))}))
                  ;; Preserve query input order, including for >32 groups. Hash
                  ;; iteration order must not determine acquisition or cleanup.
                  (mapv physical group-keys))
            prepared (->Prepared ctl (:query-id runtime) groups requirements
                                 (select-keys requirements (map :symbol (filter :native entries))) planning
                                 (atom :prepared))
            held @tokens]
        (rt/own! runtime #(do (reset! (:state prepared) :closed)
                             (doseq [token held] (control/release! token))))
        (vreset! retained? true)
        prepared)
      (finally (when-not @retained? (doseq [token @tokens] (control/release! token)))))))

(defn- session-info! [request info]
  (fields! info #{:spi-version :features :session-id :source-instance-id :compute-context-id
                  :authorization-token :consistency :acquired-at-ms :data-version :request-constraints}
           #{:consumption :consumption-groups} :session)
  (ensure! (and (= 2 (:spi-version info)) (= (:features request) (:features info))
                (some? (:session-id info)) (some? (:authorization-token info))
                (nonnegative? (:acquired-at-ms info)) (contains? rank (:consistency info))) :session)
  (let [actual (:consistency info)]
    (when-not (and (at-least? actual (:consistency request))
                   (every? #(supports? (:read-capabilities %) actual) (:source-descriptors request)))
      (fail! :query-source/unsupported :session))
    (ensure! (or (= :snapshot actual) (nil? (:data-version info))) :session))
  (doseq [descriptor (:source-descriptors request)]
    (ensure! (= (select-keys descriptor [:source-instance-id :compute-context-id])
                (select-keys info [:source-instance-id :compute-context-id])) :session))
  (permissions/request-constraints! (:request-constraints info) #(control/check-active! (:control request)))
  info)

(defn- close-after-error! [runtime primary closers]
  (control/cancel! (:control runtime) (select-keys (ex-data primary) [:error :phase]))
  (doseq [close (reverse closers)]
    (try (close)
         (catch Throwable e
           (.addSuppressed ^Throwable primary (rt/redacted-error runtime :close e)))))
  (throw primary))

(defn acquire!
  "Acquire and validate every prepared group eagerly, once. Own a returned
  session before checking cancellation, retain cancellation through close, and
  immediately unwind all acquired groups on failure. Providers own setup I/O
  request admission; this helper creates no duplicate backend limiter."
  [runtime prepared]
  (let [ctl (:control runtime) closers (volatile! [])
        own! (fn [runtime action]
               (let [close (rt/own! runtime action)] (vswap! closers conj close) close))
        snapshot! (fn [runtime identifier]
                    (let [token (rt/charge-copy! runtime identifier)]
                      (own! runtime #(control/release! token))
                      (value/diagnostic-copy identifier #(control/check-active! ctl))))]
    (control/check-active! ctl)
    (ensure! (and (instance? Prepared prepared) (identical? ctl (:control prepared))
                  (= (:query-id runtime) (:query-id prepared))
                  (compare-and-set! (:state prepared) :prepared :acquiring)) :acquisition)
    (try
      (let [groups
            (mapv
              (fn [{:keys [aliases entries request] :as group}]
                (rt/with-context runtime {:source-symbol (first aliases)}
                  (fn [runtime]
                    (let [session (rt/invoke runtime :acquisition
                                    #(source/-open-session (:source (first entries)) request))]
                      (ensure! (satisfies? source/IQuerySourceSession session) :session)
                      (let [close (own! runtime #(source/-close-session! session))
                            registration (source/register-cancel! ctl #(source/-cancel-session! session %))]
                        (own! runtime #(try (close)
                                            (finally (source/unregister-cancel! registration)))))
                      (let [info (rt/invoke runtime :session #(source/-session-info session))
                            token (rt/charge-metadata! runtime info :session 2048)]
                        (try
                          (session-info! request info)
                          (let [consumption (permissions/consumption ctl info)
                                _ (own! runtime #(.close ^Closeable consumption))
                                retained (control/reserve! ctl {:retained-bytes 2048})
                                _ (own! runtime #(control/release! retained))
                                constraints (:request-constraints info)]
                            (assoc group :session session :consumption consumption
                                         :context (select-keys request common-keys)
                                         :actual-consistency (:consistency info)
                                         :session-id (snapshot! runtime (:session-id info))
                                         :authorization-token (snapshot! runtime (:authorization-token info))
                                         :data-version (snapshot! runtime (:data-version info))
                                         :request-constraints
                                         {:max-concurrent-requests (:max-concurrent-requests constraints)
                                          :rate-limit (when-some [rate (:rate-limit constraints)]
                                                        {:requests (:requests rate) :per-ms (:per-ms rate)})}))
                          (finally (control/release! token))))))))
              (:groups prepared))]
        (control/check-active! ctl)
        (reset! (:state prepared) :acquired)
        groups)
      (catch Throwable e
        (reset! (:state prepared) :failed)
        (close-after-error! runtime (rt/redacted-error runtime (or (:phase (ex-data e)) :acquisition) e) @closers)))))

(defn explain!
  "Copy per-alias requested/actual guarantees and the physical group minimum.
  Authorization, effective tokens, backend metadata, and source values stay
  private. Native read guarantees are reported by the existing native engine."
  [runtime groups]
  (let [n (reduce + (map #(count (:aliases %)) groups))
        token (control/reserve! (:control runtime)
                {:retained-bytes (value/add-charge 1024 (value/multiply-charge 1024 n))})]
    (try
      (let [result (into []
                         (mapcat (fn [{:keys [aliases request actual-consistency data-version]}]
                                   (map (fn [alias]
                                          (rt/diagnostic-copy! runtime
                                            {:source alias :requested-consistency (get-in request [:read-requirements alias])
                                             :effective-consistency (:consistency request)
                                             :consistency actual-consistency :data-version data-version})) aliases))) groups)]
        (rt/own! runtime #(control/release! token))
        result)
      (catch Throwable e (control/release! token) (throw e)))))
