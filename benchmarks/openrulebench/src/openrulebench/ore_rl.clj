(ns openrulebench.ore-rl
  "OWL 2 RL benchmark using a stratified subset of the ORE 2015 corpus.
   This benchmark performs:
   1) OWL 2 RL materialization (via Datalog rules)
   2) Generic fixed queries over the materialized closure

   Notes:
   - ORE 2015 ontologies are preprocessed to OWL/XML; we load with OWLAPI,
     render to RDF/XML, then parse RDF triples for rule evaluation.
   - The rule set implemented here is an OWL RL core (RDFS + property/class
     hierarchy, sameAs, inverse, symmetric, transitive, functional)."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str]
   [openrulebench.core :as core]
   [datalevin.core :as d])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream File]
   [java.util Random UUID]
   [org.eclipse.rdf4j.rio Rio RDFFormat]
   [org.eclipse.rdf4j.model Statement IRI BNode Literal]
   [org.semanticweb.owlapi.apibinding OWLManager]
   [org.semanticweb.owlapi.formats RDFXMLDocumentFormat]
   [org.semanticweb.owlapi.model OWLOntology]
   [org.semanticweb.owlapi.profiles OWL2RLProfile]))

;; =============================================================================
;; Constants and Helpers
;; =============================================================================

(defn- iri [^String s]
  (str "iri|" s))

(def rdf-type (iri "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"))
(def rdfs-subclass (iri "http://www.w3.org/2000/01/rdf-schema#subClassOf"))
(def rdfs-subproperty (iri "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"))
(def rdfs-domain (iri "http://www.w3.org/2000/01/rdf-schema#domain"))
(def rdfs-range (iri "http://www.w3.org/2000/01/rdf-schema#range"))

(def owl-equivalent-class (iri "http://www.w3.org/2002/07/owl#equivalentClass"))
(def owl-equivalent-property (iri "http://www.w3.org/2002/07/owl#equivalentProperty"))
(def owl-sameas (iri "http://www.w3.org/2002/07/owl#sameAs"))
(def owl-inverse-of (iri "http://www.w3.org/2002/07/owl#inverseOf"))
(def owl-symmetric-property (iri "http://www.w3.org/2002/07/owl#SymmetricProperty"))
(def owl-transitive-property (iri "http://www.w3.org/2002/07/owl#TransitiveProperty"))
(def owl-functional-property (iri "http://www.w3.org/2002/07/owl#FunctionalProperty"))
(def owl-inverse-functional-property (iri "http://www.w3.org/2002/07/owl#InverseFunctionalProperty"))

(defn- escape-lit [^String s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "|" "\\|")))

(defn- value->string [v]
  (cond
    (instance? IRI v) (iri (.stringValue ^IRI v))
    (instance? BNode v) (str "bnode|" (.getID ^BNode v))
    (instance? Literal v)
    (let [lit ^Literal v
          dt  (some-> (.getDatatype lit) .stringValue)
          lang (when (.isPresent (.getLanguage lit))
                 (.getLanguage lit))]
      (str "lit|" (or dt "") "|" (or lang "") "|" (escape-lit (.getLabel lit))))
    :else (str v)))

(defn- statement->triple [^Statement st]
  [(value->string (.getSubject st))
   (value->string (.getPredicate st))
   (value->string (.getObject st))])

;; =============================================================================
;; OWLAPI + RDF parsing
;; =============================================================================

(defn- load-ontology
  "Load an ontology using OWLAPI."
  [^File f]
  (let [manager (OWLManager/createOWLOntologyManager)
        ontology (.loadOntologyFromOntologyDocument manager f)]
    {:manager manager
     :ontology ontology}))

(defn- ontology->triples
  "Render ontology to RDF/XML and parse triples."
  [{:keys [manager ^OWLOntology ontology]}]
  (let [out (ByteArrayOutputStream.)]
    (.saveOntology manager ontology (RDFXMLDocumentFormat.) out)
    (with-open [in (ByteArrayInputStream. (.toByteArray out))]
      (let [model (Rio/parse in "" RDFFormat/RDFXML)]
        (vec (map statement->triple model))))))

(defn- ontology-info
  "Return {:path ... :axioms n :rl? boolean}."
  [^File f]
  (try
    (let [{:keys [manager ontology]} (load-ontology f)
          axioms (.getAxiomCount ^OWLOntology ontology)
          report (.checkOntology (OWL2RLProfile.) ontology)
          rl? (.isInProfile report)]
      (.removeOntology manager ontology)
      {:path (.getPath f) :axioms axioms :rl? rl?})
    (catch Exception e
      {:path (.getPath f) :error (.getMessage e)})))

;; =============================================================================
;; ORE 2015 selection (stratified by axiom count)
;; =============================================================================

(def default-data-dir "data/ore2015")
(def default-manifest "data/ore2015/ore-rl-small.edn")
(def default-total 50)
(def default-seed 42)

(def bins
  [{:name :very-small :min 1 :max 99}
   {:name :small :min 100 :max 999}
   {:name :medium :min 1000 :max 9999}
   {:name :large :min 10000 :max 100000}
   {:name :very-large :min 100001 :max Long/MAX_VALUE}])

(defn- bin-for [axioms]
  (some (fn [{:keys [name min max]}]
          (when (and (<= min axioms) (<= axioms max)) name))
        bins))

(defn- shuffle-with [^Random rng coll]
  (let [arr (object-array coll)]
    (dotimes [i (dec (alength arr))]
      (let [j (+ i (.nextInt rng (- (alength arr) i)))]
        (let [tmp (aget arr i)]
          (aset arr i (aget arr j))
          (aset arr j tmp))))
    (vec arr)))

(defn- stratified-sample
  [items total seed]
  (let [rng (Random. seed)
        grouped (->> items
                     (group-by :bin)
                     (into {} (map (fn [[k v]] [k (shuffle-with rng v)]))))
        counts (into {} (map (fn [[k v]] [k (count v)]) grouped))
        total-available (reduce + (vals counts))
        desired (min total total-available)
        targets (reduce (fn [acc {:keys [name]}]
                          (let [n (get counts name 0)
                                share (if (pos? total-available)
                                        (int (Math/round (* desired (/ n total-available))))
                                        0)]
                            (assoc acc name (min n (max 0 share)))))
                        {}
                        bins)
        initial (mapcat (fn [[k v]]
                          (take (get targets k 0) v))
                        grouped)
        taken (count initial)
        remaining (- desired taken)
        remainder-pool (vec (mapcat (fn [[k v]]
                                      (drop (get targets k 0) v))
                                    grouped))
        remainder (take remaining (shuffle-with rng remainder-pool))]
    (vec (concat initial remainder))))

(defn- list-ontology-files [dir]
  (let [exts #{"owl" "rdf" "xml" "ttl" "nt" "n3" "owx"}]
    (->> (file-seq (io/file dir))
         (filter #(.isFile ^File %))
         (filter (fn [^File f]
                   (let [name (.getName f)
                         idx (.lastIndexOf name ".")]
                     (when (pos? idx)
                       (contains? exts (str/lower-case (subs name (inc idx)))))))))))

(defn- ensure-manifest
  [{:keys [data-dir manifest-path total seed refresh?]}]
  (let [manifest-file (io/file manifest-path)]
    (if (and (.exists manifest-file) (not refresh?))
      (edn/read-string (slurp manifest-file))
      (let [files (list-ontology-files data-dir)
            infos (->> files
                       (map ontology-info)
                       (filter :rl?)
                       (filter :axioms)
                       (map (fn [m]
                              (assoc m :bin (bin-for (:axioms m)))))
                       (filter :bin)
                       (sort-by :path))
            sample (stratified-sample infos total seed)
            rel (fn [path]
                  (let [base (.toPath (io/file data-dir))
                        p (.toPath (io/file path))]
                    (str (.relativize base p))))
            manifest {:source "ore2015"
                      :seed seed
                      :total total
                      :data-dir data-dir
                      :files (mapv (fn [m]
                                     (-> m
                                         (update :path rel)
                                         (select-keys [:path :axioms :bin])))
                                   sample)}]
        (io/make-parents manifest-path)
        (spit manifest-path (pr-str manifest))
        manifest))))

;; =============================================================================
;; Datalevin storage and rules
;; =============================================================================

(def triple-schema
  {:triple/s {:db/valueType :db.type/string}
   :triple/p {:db/valueType :db.type/string}
   :triple/o {:db/valueType :db.type/string}})

(defn- triples->datoms [triples]
  (mapv (fn [[idx [s p o]]]
          {:db/id (inc idx)
           :triple/s s
           :triple/p p
           :triple/o o})
        (map-indexed vector triples)))

(def owl-rl-rules
  `[[(triple ?s ?p ?o)
     [?t :triple/s ?s]
     [?t :triple/p ?p]
     [?t :triple/o ?o]]

    [(subClass ?c ?d)
     (triple ?c ~rdfs-subclass ?d)]
    [(subClass ?c ?e)
     (subClass ?c ?d)
     (subClass ?d ?e)]
    [(triple ?x ~rdf-type ?d)
     (triple ?x ~rdf-type ?c)
     (subClass ?c ?d)]

    [(subProperty ?p ?q)
     (triple ?p ~rdfs-subproperty ?q)]
    [(subProperty ?p ?r)
     (subProperty ?p ?q)
     (subProperty ?q ?r)]
    [(triple ?s ?q ?o)
     (triple ?s ?p ?o)
     (subProperty ?p ?q)]

    [(subClass ?c ?d)
     (triple ?c ~owl-equivalent-class ?d)]
    [(subClass ?d ?c)
     (triple ?c ~owl-equivalent-class ?d)]

    [(subProperty ?p ?q)
     (triple ?p ~owl-equivalent-property ?q)]
    [(subProperty ?q ?p)
     (triple ?p ~owl-equivalent-property ?q)]

    [(triple ?s ~rdf-type ?c)
     (triple ?p ~rdfs-domain ?c)
     (triple ?s ?p ?o)]
    [(triple ?o ~rdf-type ?c)
     (triple ?p ~rdfs-range ?c)
     (triple ?s ?p ?o)]

    [(inverseOf ?p ?q)
     (triple ?p ~owl-inverse-of ?q)]
    [(inverseOf ?q ?p)
     (inverseOf ?p ?q)]
    [(triple ?o ?q ?s)
     (inverseOf ?p ?q)
     (triple ?s ?p ?o)]

    [(triple ?o ?p ?s)
     (triple ?p ~rdf-type ~owl-symmetric-property)
     (triple ?s ?p ?o)]

    [(triple ?s ?p ?z)
     (triple ?p ~rdf-type ~owl-transitive-property)
     (triple ?s ?p ?o)
     (triple ?o ?p ?z)]

    [(sameAs ?x ?y)
     (triple ?x ~owl-sameas ?y)]
    [(sameAs ?y ?x)
     (sameAs ?x ?y)]
    [(sameAs ?x ?z)
     (sameAs ?x ?y)
     (sameAs ?y ?z)]

    [(triple ?y ?p ?o)
     (triple ?x ?p ?o)
     (sameAs ?x ?y)]
    [(triple ?s ?p ?y)
     (triple ?s ?p ?x)
     (sameAs ?x ?y)]
    [(triple ?s ?q ?o)
     (triple ?s ?p ?o)
     (sameAs ?p ?q)]

    [(sameAs ?o1 ?o2)
     (triple ?p ~rdf-type ~owl-functional-property)
     (triple ?s ?p ?o1)
     (triple ?s ?p ?o2)]

    [(sameAs ?s1 ?s2)
     (triple ?p ~rdf-type ~owl-inverse-functional-property)
     (triple ?s1 ?p ?o)
     (triple ?s2 ?p ?o)]])

(defn- infer-triples [db]
  (d/q '[:find ?s ?p ?o
         :in $ %
         :where (triple ?s ?p ?o)]
       db owl-rl-rules))

(def query-specs
  [{:name :total
    :query '[:find (count ?t) .
             :where [?t :triple/p _]]}
   {:name :type
    :query `[:find (count ?t) .
             :where [?t :triple/p ~rdf-type]]}
   {:name :subclass
    :query `[:find (count ?t) .
             :where [?t :triple/p ~rdfs-subclass]]}
   {:name :subproperty
    :query `[:find (count ?t) .
             :where [?t :triple/p ~rdfs-subproperty]]}
   {:name :sameas
    :query `[:find (count ?t) .
             :where [?t :triple/p ~owl-sameas]]}])

;; =============================================================================
;; Benchmark execution
;; =============================================================================

(defn- create-conn [triples]
  (let [dir (str "/tmp/ore-rl-" (UUID/randomUUID))
        conn (d/create-conn dir triple-schema)]
    (d/transact! conn (triples->datoms triples))
    {:conn conn :dir dir}))

(defn- cleanup-conn! [{:keys [conn dir]}]
  (when conn (d/close conn))
  (when dir (io/delete-file dir true)))

(defn- run-queries [db]
  (reduce (fn [acc {:keys [name query]}]
            (let [[result time-ms] (core/time-once (d/q query db))
                  count (if (number? result) result (first result))]
              (assoc acc name {:count count :time-ms time-ms})))
          {}
          query-specs))

(defn- run-ontology
  [{:keys [data-dir path axioms]}]
  (let [file (io/file data-dir path)
        {:keys [manager ontology]} (load-ontology file)
        triples (ontology->triples {:manager manager :ontology ontology})
        base-count (count triples)
        base-conn (create-conn triples)
        db (d/db (:conn base-conn))
        start (core/now-ms)
        inferred (infer-triples db)
        inferred-count (count inferred)
        inferred-conn (create-conn inferred)
        materialize-ms (- (core/now-ms) start)
        queries (run-queries (d/db (:conn inferred-conn)))
        query-ms (reduce + 0 (map (comp :time-ms val) queries))]
    (.removeOntology manager ontology)
    (cleanup-conn! base-conn)
    (cleanup-conn! inferred-conn)
    {:ontology path
     :axioms axioms
     :base-triples base-count
     :inferred-triples inferred-count
     :materialize-ms materialize-ms
     :query-ms query-ms
     :queries queries}))

(defn- write-results-csv [results path]
  (io/make-parents path)
  (with-open [w (io/writer path)]
    (.write w "ontology,axioms,base_triples,inferred_triples,materialize_ms,query_ms")
    (doseq [{:keys [name]} query-specs]
      (.write w (str "," (name name) "_count," (name name) "_ms")))
    (.write w "\n")
    (doseq [{:keys [ontology axioms base-triples inferred-triples materialize-ms query-ms queries]} results]
      (.write w (format "%s,%d,%d,%d,%.2f,%.2f"
                        ontology axioms base-triples inferred-triples materialize-ms query-ms))
      (doseq [{:keys [name]} query-specs]
        (let [{:keys [count time-ms]} (get queries name)]
          (.write w (format ",%d,%.2f" (long count) (double time-ms)))))
      (.write w "\n"))))

;; =============================================================================
;; CLI
;; =============================================================================

(defn- parse-args [args]
  (loop [remaining args
         opts {:data-dir default-data-dir
               :manifest-path default-manifest
               :total default-total
               :seed default-seed
               :refresh? false
               :limit nil
               :out "out/ore-rl-small.csv"}]
    (if (empty? remaining)
      opts
      (let [arg (first remaining)]
        (case arg
          "--data" (let [val (second remaining)]
                     (recur (nnext remaining)
                            (assoc opts
                                   :data-dir val
                                   :manifest-path (str val "/ore-rl-small.edn"))))
          "--manifest" (recur (nnext remaining) (assoc opts :manifest-path (second remaining)))
          "--limit" (recur (nnext remaining) (assoc opts :limit (parse-long (second remaining))))
          "--seed" (recur (nnext remaining) (assoc opts :seed (parse-long (second remaining))))
          "--refresh" (recur (rest remaining) (assoc opts :refresh? true))
          "--out" (recur (nnext remaining) (assoc opts :out (second remaining)))
          (recur (rest remaining) opts))))))

(defn -main [& args]
  (let [{:keys [data-dir manifest-path total seed refresh? limit out]} (parse-args args)
        manifest (ensure-manifest {:data-dir data-dir
                                   :manifest-path manifest-path
                                   :total total
                                   :seed seed
                                   :refresh? refresh?})
        files (:files manifest)
        selected (if limit (take limit files) files)]
    (println "ORE 2015 OWL RL benchmark")
    (println "Data dir:" data-dir)
    (println "Manifest:" manifest-path)
    (println "Ontologies:" (count selected))
    (println)
    (let [results (mapv (fn [entry]
                          (println "Running" (:path entry))
                          (run-ontology (assoc entry :data-dir data-dir)))
                        selected)]
      (write-results-csv results out)
      (println)
      (println "Wrote results:" out))))
