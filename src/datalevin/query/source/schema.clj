(ns ^:no-doc datalevin.query.source.schema
  "Pure v2 EAV descriptor checks, independent of protocols and execution."
  (:require [clojure.set :as set]
            [datalevin.query.source.contract :as contract]
            [datalevin.query.source.value :as value :refer [fail!]]))

(defn- ensure! [ok]
  (when-not ok (fail! :query-source/invalid :descriptor)))

(defn- fields! [m required]
  (ensure! (and (value/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (contains? required %) (qualified-keyword? %)) (keys m)))))

(defn- identifier? [x check!]
  (and (some? x) (value/portable? x check!)))

(defn logical-schema!
  "Validate complete attribute coverage, domains and full entity/reference
  types. The caller has already validated/admitted the immutable descriptor."
  [pattern-contract schema check!]
  (fields! schema #{:domains :attributes})
  (let [domains (:domains schema) attributes (:attributes schema)
        pattern-attributes (:attributes pattern-contract)]
    (ensure! (and (value/plain-map? domains) (seq domains) (value/plain-map? attributes)
                  (= (set (keys pattern-attributes)) (set (keys attributes)))))
    (doseq [[id domain] domains]
      (check!)
      (ensure! (identifier? id check!))
      (fields! domain #{:entity-type})
      (contract/type! (:entity-type domain) check!))
    (doseq [[attribute guarantees] attributes]
      (check!)
      (fields! guarantees #{:entity-domain :reference-domain :cardinality :absence})
      (let [{:keys [entity-domain reference-domain cardinality absence]} guarantees
            {:keys [entity-type value-type]} (get pattern-attributes attribute)]
        (ensure! (and (contains? domains entity-domain)
                      (= entity-type (get-in domains [entity-domain :entity-type]))
                      (#{:one :many :unknown} cardinality) (= :no-fact absence)
                      (or (nil? reference-domain)
                          (and (contains? domains reference-domain)
                               (= value-type (get-in domains [reference-domain :entity-type])))))))))
  schema)

(defn admitted-source-info!
  "Check descriptor structure after metadata-charge has admitted every value,
  annotation, and retained vector parent. No second portable-value walk."
  [info check!]
   (check!)
   (ensure! (and (value/plain-map? info) (contains? info :spi-versions)
                 (set? (:spi-versions info)) (seq (:spi-versions info))
                 (every? pos-int? (:spi-versions info))))
   (when-not (= #{2} (:spi-versions info)) (fail! :query-source/version :descriptor))
   (fields! info #{:spi-versions :source-instance-id :compute-context-id :definition-id
                   :definition-fingerprint :features :required-features :pattern-contract
                   :logical-schema :read-capabilities :scope :completion})
   (ensure! (and (every? #(identifier? (get info %) check!)
                         [:source-instance-id :compute-context-id :definition-id])
                 (string? (:definition-fingerprint info)) (seq (:definition-fingerprint info))
                 (set? (:features info)) (every? keyword? (:features info))
                 (set? (:required-features info)) (every? keyword? (:required-features info))
                 (set/subset? (:required-features info) (:features info))
                 (set? (:read-capabilities info)) (seq (:read-capabilities info))
                 (every? #{:best-effort :statement :snapshot} (:read-capabilities info))))
   (when-not (= :eav (get-in info [:pattern-contract :kind]))
     (fail! :query-source/unsupported :descriptor))
   (contract/pattern-contract! (:pattern-contract info) check!)
   (logical-schema! (:pattern-contract info) (:logical-schema info) check!)
   (let [{:keys [scope completion]} info]
     (fields! scope #{:id :kind :parameters})
     (ensure! (and (string? (:id scope)) (seq (:id scope)) (#{:full :restricted} (:kind scope))
                   (value/plain-map? (:parameters scope))
                   (or (= :restricted (:kind scope)) (empty? (:parameters scope)))))
     (fields! completion #{:domain :extent})
     (ensure! (and (= :finite (:domain completion)) (#{:all-matches :declared-view} (:extent completion)))))
   info)

(defn source-info!
  "Validate a revised EAV descriptor, including all portable annotations.
  Call admitted-source-info! only after bounded metadata admission."
  ([info] (source-info! info (fn [])))
  ([info check!]
   (ensure! (value/portable? info check!))
   (admitted-source-info! info check!)))

