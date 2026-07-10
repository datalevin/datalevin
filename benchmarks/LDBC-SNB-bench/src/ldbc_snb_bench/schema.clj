(ns ldbc-snb-bench.schema
  "LDBC SNB schema definition for Datalevin.
   Maps the Social Network Benchmark data model to Datalevin datoms.")

;; Entity ID base allocation to avoid collisions
;; Following the pattern from JOB benchmark
(def eid-bases
  {:place        1000000
   :organization 2000000
   :tagclass     3000000
   :tag          4000000
   :person       10000000
   :forum        100000000
   :message      200000000})

(defn place-eid [id] (+ (:place eid-bases) id))
(defn organization-eid [id] (+ (:organization eid-bases) id))
(defn tagclass-eid [id] (+ (:tagclass eid-bases) id))
(defn tag-eid [id] (+ (:tag eid-bases) id))
(defn person-eid [id] (+ (:person eid-bases) id))
(defn forum-eid [id] (+ (:forum eid-bases) id))
(defn message-eid [id] (+ (:message eid-bases) id))

(def schema
  {;; ============================================================
   ;; Place entity (City, Country, Continent)
   ;; ============================================================
   :place/id        {:db/valueType :db.type/long
                     :db/unique    :db.unique/identity}
   :place/name      {:db/valueType :db.type/string}
   :place/url       {:db/valueType :db.type/string}
   :place/type      {:db/valueType :db.type/string}  ; "City", "Country", "Continent"
   :place/isPartOf  {:db/valueType :db.type/ref}     ; -> Place (parent)

   ;; ============================================================
   ;; Organization entity (Company, University)
   ;; ============================================================
   :organization/id          {:db/valueType :db.type/long
                              :db/unique    :db.unique/identity}
   :organization/name        {:db/valueType :db.type/string}
   :organization/url         {:db/valueType :db.type/string}
   :organization/type        {:db/valueType :db.type/string}  ; "Company", "University"
   :organization/isLocatedIn {:db/valueType :db.type/ref}     ; -> Place

   ;; ============================================================
   ;; TagClass entity
   ;; ============================================================
   :tagclass/id           {:db/valueType :db.type/long
                           :db/unique    :db.unique/identity}
   :tagclass/name         {:db/valueType :db.type/string}
   :tagclass/url          {:db/valueType :db.type/string}
   :tagclass/isSubclassOf {:db/valueType :db.type/ref}  ; -> TagClass (parent)

   ;; ============================================================
   ;; Tag entity
   ;; ============================================================
   :tag/id      {:db/valueType :db.type/long
                 :db/unique    :db.unique/identity}
   :tag/name    {:db/valueType :db.type/string}
   :tag/url     {:db/valueType :db.type/string}
   :tag/hasType {:db/valueType :db.type/ref}  ; -> TagClass

   ;; ============================================================
   ;; Person entity
   ;; ============================================================
   :person/id           {:db/valueType :db.type/long
                         :db/unique    :db.unique/identity}
   :person/firstName    {:db/valueType :db.type/string}
   :person/lastName     {:db/valueType :db.type/string}
   :person/gender       {:db/valueType :db.type/string}
   :person/birthday     {:db/valueType :db.type/instant}
   :person/creationDate {:db/valueType :db.type/instant}
   :person/locationIP   {:db/valueType :db.type/string}
   :person/browserUsed  {:db/valueType :db.type/string}
   :person/email        {:db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/many}
   :person/speaks       {:db/valueType   :db.type/string
                         :db/cardinality :db.cardinality/many}
   :person/isLocatedIn  {:db/valueType :db.type/ref}  ; -> Place (city)
   :person/hasInterest  {:db/valueType   :db.type/ref
                         :db/cardinality :db.cardinality/many}  ; -> Tag

   ;; ============================================================
   ;; Person relationships
   ;; ============================================================
   ;; knows: Person -> Person (with creationDate as edge property)
   ;; We model this as a separate entity for the edge properties
   :knows/person1      {:db/valueType :db.type/ref}   ; -> Person
   :knows/person2      {:db/valueType :db.type/ref}   ; -> Person
   :knows/creationDate {:db/valueType :db.type/instant}

   ;; studyAt: Person -> Organization (with classYear)
   :studyAt/person       {:db/valueType :db.type/ref}  ; -> Person
   :studyAt/organization {:db/valueType :db.type/ref}  ; -> Organization
   :studyAt/classYear    {:db/valueType :db.type/long}

   ;; workAt: Person -> Organization (with workFrom year)
   :workAt/person       {:db/valueType :db.type/ref}  ; -> Person
   :workAt/organization {:db/valueType :db.type/ref}  ; -> Organization
   :workAt/workFrom     {:db/valueType :db.type/long}

   ;; ============================================================
   ;; Forum entity
   ;; ============================================================
   :forum/id           {:db/valueType :db.type/long
                        :db/unique    :db.unique/identity}
   :forum/title        {:db/valueType :db.type/string}
   :forum/creationDate {:db/valueType :db.type/instant}
   :forum/hasModerator {:db/valueType :db.type/ref}  ; -> Person
   :forum/hasTag       {:db/valueType   :db.type/ref
                        :db/cardinality :db.cardinality/many}  ; -> Tag

   ;; Forum membership (with joinDate)
   :hasMember/forum    {:db/valueType :db.type/ref}  ; -> Forum
   :hasMember/person   {:db/valueType :db.type/ref}  ; -> Person
   :hasMember/joinDate {:db/valueType :db.type/instant}

   ;; ============================================================
   ;; Message entity (Post/Comment)
   ;; ============================================================
   :message/id           {:db/valueType :db.type/long
                          :db/unique    :db.unique/identity}
   :message/imageFile    {:db/valueType :db.type/string}
   :message/creationDate {:db/valueType :db.type/instant}
   :message/locationIP   {:db/valueType :db.type/string}
   :message/browserUsed  {:db/valueType :db.type/string}
   :message/language     {:db/valueType :db.type/string}
   :message/content      {:db/valueType :db.type/string}
   :message/length       {:db/valueType :db.type/long}
   :message/hasCreator   {:db/valueType :db.type/ref}  ; -> Person
   :message/hasTag       {:db/valueType   :db.type/ref
                          :db/cardinality :db.cardinality/many}  ; -> Tag
   :message/isLocatedIn  {:db/valueType :db.type/ref}  ; -> Place (country)
   :message/isContainedIn {:db/valueType :db.type/ref} ; Post -> Forum
   :message/replyOf      {:db/valueType :db.type/ref}  ; -> Message (comments only)

   ;; ============================================================
   ;; Likes relationships
   ;; ============================================================
   :likes/person       {:db/valueType :db.type/ref}
   :likes/message      {:db/valueType :db.type/ref}
   :likes/creationDate {:db/valueType :db.type/instant}})

;; Database options for optimal performance
(def db-opts {:kv-opts {:mapsize 50000}}); 50GB max for SF10
