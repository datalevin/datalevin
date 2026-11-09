(ns ^:no-doc datalevin.query.source.contract
  "Pure v2 type/pattern helpers and captured cursor request construction."
  (:require [datalevin.query.source.value :as v :refer [fail!]]))

(defn- ensure! [ok phase]
  (when-not ok (fail! :query-source/invalid phase)))

(defn- fields! [m required optional phase]
  (ensure! (and (v/plain-map? m) (every? #(contains? m %) required)
                (every? #(or (contains? required %) (contains? optional %)
                             (qualified-keyword? %)) (keys m))) phase))

(defn- nonempty-string? [x] (and (string? x) (pos? (count x))))
(defn- variable? [x] (and (symbol? x) (.startsWith (name x) "?")))

(defn cursor-request
  "Build the v2 open envelope from captured context and a selected plan.
  Planning inputs belong to the session's handle, not the cursor request."
  [context source-symbol definition-fingerprint alternative]
  (assoc (select-keys context [:spi-version :features :query-id :authorization
                               :consistency :control :limits])
         :source-symbol source-symbol
         :definition-fingerprint definition-fingerprint
         :scope-id (:scope-id alternative)
         :region-id (:region-id context)
         :alternative-id (:id alternative)
         :plan (:plan alternative)))

(defn type!
  ([t] (type! t nil))
  ([t check!]
   (ensure! (v/portable? t check!) :descriptor)
   (if (keyword? t)
     (ensure! (v/scalar-types t) :descriptor)
     (case (:kind t)
       :tuple (do (fields! t #{:kind :elements} #{} :descriptor)
                  (ensure! (and (vector? (:elements t)) (seq (:elements t))) :descriptor)
                  (run! #(type! % check!) (:elements t)))
       :custom (do (fields! t #{:kind :name :version :fingerprint} #{:parameters} :descriptor)
                   (ensure! (and (qualified-keyword? (:name t))
                                 (pos-int? (:version t))
                                 (nonempty-string? (:fingerprint t))
                                 (or (not (contains? t :parameters))
                                     (and (v/plain-map? (:parameters t))
                                          (v/portable? (:parameters t) check!)))) :descriptor))
       (fail! :query-source/invalid :descriptor)))
   t))

(defn- column! [column check!]
  (fields! column #{:type :nullable? :variable} #{} :descriptor)
  (type! (:type column) check!)
  (ensure! (and (boolean? (:nullable? column))
                (variable? (:variable column))) :descriptor))

(defn binding-columns!
  "Validate the nonempty ordered typed inputs of a dependent access. This pure
  helper does not admit a source descriptor or negotiate an SPI revision."
  ([columns] (binding-columns! columns nil))
  ([columns check!]
   (ensure! (and (vector? columns) (seq columns) (v/portable? columns check!)) :batch)
   (doseq [column columns] (column! column check!))
   (ensure! (= (count columns) (count (set (map :variable columns)))) :batch)
   columns))

(defn pattern-contract!
  ([contract] (pattern-contract! contract nil))
  ([contract check!]
   (case (:kind contract)
     :eav (do (fields! contract #{:kind :attributes} #{} :descriptor)
              (ensure! (and (v/plain-map? (:attributes contract))
                            (seq (:attributes contract))) :descriptor)
              (doseq [[attribute types] (:attributes contract)]
                (ensure! (keyword? attribute) :descriptor)
                (fields! types #{:entity-type :value-type} #{} :descriptor)
                (type! (:entity-type types) check!)
                (type! (:value-type types) check!)))
     (fail! :query-source/invalid :descriptor))
   contract))

(defn- term! [term check!]
  (when check! (check!))
  (ensure! (and (vector? term)
                (case (first term)
                  :var (and (= 2 (count term)) (variable? (second term)))
                  :const (and (= 2 (count term))
                              (try (v/value-charge (second term) (when check! (fn [_] (check!)))) true
                                   (catch clojure.lang.ExceptionInfo _
                                     (when check! (check!))
                                     false)))
                  :ignore (= 1 (count term))
                  false)) :clauses))

(defn pattern-columns
  ([descriptor clause] (pattern-columns descriptor clause nil))
  ([descriptor clause check!]
   (let [contract (:pattern-contract descriptor)
         terms (:terms clause)]
     (ensure! (vector? terms) :clauses)
     (run! #(term! % check!) terms)
     (case (:kind contract)
       :eav (do
              (ensure! (= 3 (count terms)) :clauses)
              (let [[tag attribute] (nth terms 1)
                    types (get (:attributes contract) attribute)]
                (when-not (and (= :const tag) types)
                  (fail! :query-source/unsupported :clauses {:clause-id (:id clause)}))
                [{:type (:entity-type types) :nullable? false}
                 {:type :keyword :nullable? false}
                 {:type (:value-type types) :nullable? false}]))
       (fail! :query-source/unsupported :clauses)))))

(defn variables [clause]
  (into #{} (keep #(when (= :var (first %)) (second %)))
        (or (:terms clause) (:args clause))))

(defn pattern-source-info
  "The captured descriptor for a normal or regional pattern."
  [request pattern]
  (if-let [sources (:datalevin.query.source/region-sources request)]
    (or (some #(when (= (:symbol %) (:source-symbol pattern)) (:descriptor %)) sources)
        (fail! :query-source/invalid :planning))
    (:source-descriptor request)))
