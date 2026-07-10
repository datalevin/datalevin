(ns ldbc-snb-bench.loader
  "CSV data loader for LDBC SNB benchmark.
   Loads data from LDBC SNB Datagen Spark output into Datalevin."
  (:require [datalevin.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [datalevin.core :as d]
            [ldbc-snb-bench.schema :as schema])
  (:import [java.time Instant LocalDate ZoneOffset]
           [java.time.format DateTimeFormatter]
           [java.util Date]))

;; ============================================================
;; Parsing utilities
;; ============================================================

(def ^DateTimeFormatter datetime-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))

(def ^DateTimeFormatter date-formatter
  (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(defn parse-datetime [s]
  "Parse datetime string to java.util.Date (required by Datalevin's :db.type/instant)"
  (when-not (str/blank? s)
    (Date/from (Instant/from (.parse datetime-formatter s)))))

(defn parse-date [s]
  "Parse date string to java.util.Date (required by Datalevin's :db.type/instant)"
  (when-not (str/blank? s)
    (Date/from (.toInstant (.atStartOfDay (LocalDate/parse s date-formatter) ZoneOffset/UTC)))))

(defn parse-long*  [s]
  (when-not (str/blank? s)
    (Long/parseLong s)))

(defn not-blank [s]
  (when-not (str/blank? s) s))

(defn split-multi [s]
  "Split semicolon-separated values into a sequence."
  (when-not (str/blank? s)
    (str/split s #";")))

(defn explicitly-deleted?
  "Return true when explicitlyDeleted is set to true."
  [s]
  (let [v (not-blank s)]
    (and v (= "true" (str/lower-case v)))))

;; ============================================================
;; CSV reading utilities for Spark partitioned output
;; ============================================================

(defn list-part-files
  "List all part-*.csv files in a directory."
  [dir-path]
  (let [dir (io/file dir-path)]
    (when (.exists dir)
      (->> (.listFiles dir)
           (filter #(and (.isFile %)
                         (str/starts-with? (.getName %) "part-")
                         (str/ends-with? (.getName %) ".csv")))
           (map #(.getAbsolutePath %))
           sort))))

(defn read-csv-file
  "Read a CSV file with pipe separator (LDBC format).
   Returns a lazy sequence of rows (vectors), skipping header."
  [filepath]
  (let [reader (io/reader filepath)
        rows (csv/read-csv reader :separator \|)]
    (rest rows)))  ; Skip header

(defn read-all-parts
  "Read all part files from a directory, concatenating rows."
  [dir-path]
  (let [files (list-part-files dir-path)]
    (when (seq files)
      (mapcat read-csv-file files))))

;; ============================================================
;; Base data path
;; ============================================================

(def data-subpath "graphs/csv/raw/composite-merged-fk")

(defn static-path [data-dir entity]
  (str data-dir "/" data-subpath "/static/" entity))

(defn dynamic-path [data-dir entity]
  (str data-dir "/" data-subpath "/dynamic/" entity))

;; ============================================================
;; Entity loaders - Static entities
;; ============================================================

(defn load-places
  "Load Place entities.
   Columns: id|name|url|type|PartOfPlaceId"
  [data-dir]
  (println "  Loading places...")
  (let [rows (read-all-parts (static-path data-dir "Place"))]
    (mapcat (fn [[id name url type part-of-id]]
              (let [eid (schema/place-eid (parse-long*  id))]
                (cond-> [(d/datom eid :place/id (parse-long*  id))
                         (d/datom eid :place/name name)
                         (d/datom eid :place/url url)
                         (d/datom eid :place/type type)]
                  (not-blank part-of-id)
                  (conj (d/datom eid :place/isPartOf (schema/place-eid (parse-long*  part-of-id)))))))
            rows)))

(defn load-organizations
  "Load Organisation entities.
   Columns: id|type|name|url|LocationPlaceId"
  [data-dir]
  (println "  Loading organizations...")
  (let [rows (read-all-parts (static-path data-dir "Organisation"))]
    (mapcat (fn [[id type name url place-id]]
              (let [eid (schema/organization-eid (parse-long*  id))]
                [(d/datom eid :organization/id (parse-long*  id))
                 (d/datom eid :organization/type type)
                 (d/datom eid :organization/name name)
                 (d/datom eid :organization/url url)
                 (d/datom eid :organization/isLocatedIn (schema/place-eid (parse-long*  place-id)))]))
            rows)))

(defn load-tagclasses
  "Load TagClass entities.
   Columns: id|name|url|SubclassOfTagClassId"
  [data-dir]
  (println "  Loading tag classes...")
  (let [rows (read-all-parts (static-path data-dir "TagClass"))]
    (mapcat (fn [[id name url subclass-id]]
              (let [eid (schema/tagclass-eid (parse-long*  id))]
                (cond-> [(d/datom eid :tagclass/id (parse-long*  id))
                         (d/datom eid :tagclass/name name)
                         (d/datom eid :tagclass/url url)]
                  (not-blank subclass-id)
                  (conj (d/datom eid :tagclass/isSubclassOf (schema/tagclass-eid (parse-long*  subclass-id)))))))
            rows)))

(defn load-tags
  "Load Tag entities.
   Columns: id|name|url|TypeTagClassId"
  [data-dir]
  (println "  Loading tags...")
  (let [rows (read-all-parts (static-path data-dir "Tag"))]
    (mapcat (fn [[id name url type-id]]
              (let [eid (schema/tag-eid (parse-long*  id))]
                [(d/datom eid :tag/id (parse-long*  id))
                 (d/datom eid :tag/name name)
                 (d/datom eid :tag/url url)
                 (d/datom eid :tag/hasType (schema/tagclass-eid (parse-long*  type-id)))]))
            rows)))

;; ============================================================
;; Entity loaders - Dynamic entities
;; ============================================================

(defn load-persons
  "Load Person entities.
   Columns: creationDate|deletionDate|explicitlyDeleted|id|firstName|lastName|gender|birthday|locationIP|browserUsed|LocationCityId|language|email"
  [data-dir]
  (println "  Loading persons...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person"))]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted id first-name last-name
                  gender birthday location-ip browser-used city-id languages emails]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [eid (schema/person-eid (parse-long*  id))
                      base-datoms [(d/datom eid :person/id (parse-long*  id))
                                   (d/datom eid :person/firstName first-name)
                                   (d/datom eid :person/lastName last-name)
                                   (d/datom eid :person/gender gender)
                                   (d/datom eid :person/birthday (parse-date birthday))
                                   (d/datom eid :person/creationDate (parse-datetime creation-date))
                                   (d/datom eid :person/locationIP location-ip)
                                   (d/datom eid :person/browserUsed browser-used)
                                   (d/datom eid :person/isLocatedIn (schema/place-eid (parse-long*  city-id)))]
                      ;; Add multi-valued languages
                      lang-datoms (for [lang (split-multi languages)]
                                    (d/datom eid :person/speaks lang))
                      ;; Add multi-valued emails
                      email-datoms (for [email (split-multi emails)]
                                     (d/datom eid :person/email email))]
                  (concat base-datoms lang-datoms email-datoms))))
            rows)))

(defn load-person-interests
  "Load Person hasInterest Tag relationships.
   Columns: creationDate|deletionDate|PersonId|TagId"
  [data-dir]
  (println "  Loading person interests...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_hasInterest_Tag"))]
    (map (fn [[_creation-date _deletion-date person-id tag-id]]
           (d/datom (schema/person-eid (parse-long*  person-id))
                    :person/hasInterest
                    (schema/tag-eid (parse-long*  tag-id))))
         rows)))

(defn load-person-knows
  "Load Person knows Person relationships.
   Columns: creationDate|deletionDate|explicitlyDeleted|Person1Id|Person2Id"
  [data-dir]
  (println "  Loading person knows relationships...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_knows_Person"))
        counter (atom 0)]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted person1-id person2-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [p1 (parse-long* person1-id)
                      p2 (parse-long* person2-id)
                      creation (parse-datetime creation-date)]
                  (when (and p1 p2)
                    ;; Datagen outputs undirected edges once (person1 < person2).
                    ;; Expand to both directions so queries can use a single direction.
                    (let [edge-eid (+ 900000000 (swap! counter inc))
                          rev-eid (+ 900000000 (swap! counter inc))
                          base-datoms [(d/datom edge-eid :knows/person1 (schema/person-eid p1))
                                       (d/datom edge-eid :knows/person2 (schema/person-eid p2))
                                       (d/datom edge-eid :knows/creationDate creation)]]
                      (if (= p1 p2)
                        base-datoms
                        (concat base-datoms
                                [(d/datom rev-eid :knows/person1 (schema/person-eid p2))
                                 (d/datom rev-eid :knows/person2 (schema/person-eid p1))
                                 (d/datom rev-eid :knows/creationDate creation)])))))))
            rows)))

(defn load-person-study-at
  "Load Person studyAt University relationships.
   Columns: creationDate|deletionDate|PersonId|UniversityId|classYear"
  [data-dir]
  (println "  Loading person studyAt relationships...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_studyAt_University"))
        counter (atom 0)]
    (mapcat (fn [[_creation-date _deletion-date person-id org-id class-year]]
              (let [edge-eid (+ 910000000 (swap! counter inc))]
                [(d/datom edge-eid :studyAt/person (schema/person-eid (parse-long*  person-id)))
                 (d/datom edge-eid :studyAt/organization (schema/organization-eid (parse-long*  org-id)))
                 (d/datom edge-eid :studyAt/classYear (parse-long*  class-year))]))
            rows)))

(defn load-person-work-at
  "Load Person workAt Company relationships.
   Columns: creationDate|deletionDate|PersonId|CompanyId|workFrom"
  [data-dir]
  (println "  Loading person workAt relationships...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_workAt_Company"))
        counter (atom 0)]
    (mapcat (fn [[_creation-date _deletion-date person-id org-id work-from]]
              (let [edge-eid (+ 920000000 (swap! counter inc))]
                [(d/datom edge-eid :workAt/person (schema/person-eid (parse-long*  person-id)))
                 (d/datom edge-eid :workAt/organization (schema/organization-eid (parse-long*  org-id)))
                 (d/datom edge-eid :workAt/workFrom (parse-long*  work-from))]))
            rows)))

(defn load-forums
  "Load Forum entities.
   Columns: creationDate|deletionDate|explicitlyDeleted|id|title|ModeratorPersonId"
  [data-dir]
  (println "  Loading forums...")
  (let [rows (read-all-parts (dynamic-path data-dir "Forum"))]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted id title moderator-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [eid (schema/forum-eid (parse-long*  id))]
                  [(d/datom eid :forum/id (parse-long*  id))
                   (d/datom eid :forum/title title)
                   (d/datom eid :forum/creationDate (parse-datetime creation-date))
                   (d/datom eid :forum/hasModerator (schema/person-eid (parse-long*  moderator-id)))])))
            rows)))

(defn load-forum-tags
  "Load Forum hasTag Tag relationships.
   Columns: creationDate|deletionDate|ForumId|TagId"
  [data-dir]
  (println "  Loading forum tags...")
  (let [rows (read-all-parts (dynamic-path data-dir "Forum_hasTag_Tag"))]
    (map (fn [[_creation-date _deletion-date forum-id tag-id]]
           (d/datom (schema/forum-eid (parse-long*  forum-id))
                    :forum/hasTag
                    (schema/tag-eid (parse-long*  tag-id))))
         rows)))

(defn load-forum-members
  "Load Forum hasMember Person relationships.
   Columns: creationDate|deletionDate|explicitlyDeleted|ForumId|PersonId"
  [data-dir]
  (println "  Loading forum members...")
  (let [rows (read-all-parts (dynamic-path data-dir "Forum_hasMember_Person"))
        counter (atom 0)]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted forum-id person-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [edge-eid (+ 930000000 (swap! counter inc))]
                  [(d/datom edge-eid :hasMember/forum (schema/forum-eid (parse-long*  forum-id)))
                   (d/datom edge-eid :hasMember/person (schema/person-eid (parse-long*  person-id)))
                   (d/datom edge-eid :hasMember/joinDate (parse-datetime creation-date))])))
            rows)))

(defn load-posts
  "Load Post entities into Message attributes.
   Columns: creationDate|deletionDate|explicitlyDeleted|id|imageFile|locationIP|browserUsed|language|content|length|CreatorPersonId|ContainerForumId|LocationCountryId"
  [data-dir]
  (println "  Loading posts...")
  (let [rows (read-all-parts (dynamic-path data-dir "Post"))]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted id image-file location-ip
                  browser-used language content length creator-id forum-id country-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [eid (schema/message-eid (parse-long*  id))
                      forum-eid (schema/forum-eid (parse-long* forum-id))]
                  (cond-> [(d/datom eid :message/id (parse-long*  id))
                           (d/datom eid :message/creationDate (parse-datetime creation-date))
                           (d/datom eid :message/locationIP location-ip)
                           (d/datom eid :message/browserUsed browser-used)
                           (d/datom eid :message/length (parse-long*  length))
                           (d/datom eid :message/hasCreator (schema/person-eid (parse-long*  creator-id)))
                           (d/datom forum-eid :forum/containerOf eid)
                           (d/datom eid :message/isLocatedIn (schema/place-eid (parse-long*  country-id)))]
                    (not-blank image-file) (conj (d/datom eid :message/imageFile image-file))
                    (not-blank language)   (conj (d/datom eid :message/language language))
                    (not-blank content)    (conj (d/datom eid :message/content content))))))
            rows)))

(defn load-post-tags
  "Load Message hasTag Tag relationships (from Post data).
   Columns: creationDate|deletionDate|PostId|TagId"
  [data-dir]
  (println "  Loading post tags...")
  (let [rows (read-all-parts (dynamic-path data-dir "Post_hasTag_Tag"))]
    (map (fn [[_creation-date _deletion-date post-id tag-id]]
           (d/datom (schema/message-eid (parse-long*  post-id))
                    :message/hasTag
                    (schema/tag-eid (parse-long*  tag-id))))
         rows)))

(defn load-comments
  "Load Comment entities into Message attributes.
   Columns: creationDate|deletionDate|explicitlyDeleted|id|locationIP|browserUsed|content|length|CreatorPersonId|LocationCountryId|ParentPostId|ParentCommentId"
  [data-dir]
  (println "  Loading comments...")
  (let [rows (read-all-parts (dynamic-path data-dir "Comment"))]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted id location-ip browser-used
                  content length creator-id country-id parent-post-id parent-comment-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [eid (schema/message-eid (parse-long*  id))]
                  (cond-> [(d/datom eid :message/id (parse-long*  id))
                           (d/datom eid :message/creationDate (parse-datetime creation-date))
                           (d/datom eid :message/locationIP location-ip)
                           (d/datom eid :message/browserUsed browser-used)
                           (d/datom eid :message/content content)
                           (d/datom eid :message/length (parse-long*  length))
                           (d/datom eid :message/hasCreator (schema/person-eid (parse-long*  creator-id)))
                           (d/datom eid :message/isLocatedIn (schema/place-eid (parse-long*  country-id)))]
                    (not-blank parent-post-id)
                    (conj (d/datom eid :message/replyOf (schema/message-eid (parse-long*  parent-post-id))))
                    (not-blank parent-comment-id)
                    (conj (d/datom eid :message/replyOf (schema/message-eid (parse-long*  parent-comment-id))))))))
            rows)))

(defn load-comment-tags
  "Load Message hasTag Tag relationships (from Comment data).
   Columns: creationDate|deletionDate|CommentId|TagId"
  [data-dir]
  (println "  Loading comment tags...")
  (let [rows (read-all-parts (dynamic-path data-dir "Comment_hasTag_Tag"))]
    (map (fn [[_creation-date _deletion-date comment-id tag-id]]
           (d/datom (schema/message-eid (parse-long*  comment-id))
                    :message/hasTag
                    (schema/tag-eid (parse-long*  tag-id))))
         rows)))

(defn load-likes-post
  "Load Person likes Message relationships (from Post data).
   Columns: creationDate|deletionDate|explicitlyDeleted|PersonId|PostId"
  [data-dir]
  (println "  Loading post likes...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_likes_Post"))
        counter (atom 0)]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted person-id post-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [edge-eid (+ 940000000 (swap! counter inc))]
                  [(d/datom edge-eid :likes/person (schema/person-eid (parse-long*  person-id)))
                   (d/datom edge-eid :likes/message (schema/message-eid (parse-long*  post-id)))
                   (d/datom edge-eid :likes/creationDate (parse-datetime creation-date))])))
            rows)))

(defn load-likes-comment
  "Load Person likes Message relationships (from Comment data).
   Columns: creationDate|deletionDate|explicitlyDeleted|PersonId|CommentId"
  [data-dir]
  (println "  Loading comment likes...")
  (let [rows (read-all-parts (dynamic-path data-dir "Person_likes_Comment"))
        counter (atom 0)]
    (mapcat (fn [[creation-date _deletion-date explicitly-deleted person-id comment-id]]
              (when-not (explicitly-deleted? explicitly-deleted)
                (let [edge-eid (+ 950000000 (swap! counter inc))]
                  [(d/datom edge-eid :likes/person (schema/person-eid (parse-long*  person-id)))
                   (d/datom edge-eid :likes/message (schema/message-eid (parse-long*  comment-id)))
                   (d/datom edge-eid :likes/creationDate (parse-datetime creation-date))])))
            rows)))

;; ============================================================
;; Main loading function
;; ============================================================

(defn load-all-data
  "Load all LDBC SNB data from the specified directory into Datalevin.

   data-dir should point to the root data directory containing
   'graphs/csv/raw/composite-merged-fk/{static,dynamic}' subdirectories.

   Returns the final database value."
  [db-path data-dir]
  (let [start-time (System/currentTimeMillis)
        show (fn [db label]
               (println label)
               db)]
    (println "Starting LDBC SNB data load from:" data-dir)
    (println "================================================")

    ;; Use threading macro with fill-db, following JOB benchmark pattern
    (let [final-db
          (-> (d/empty-db db-path schema/schema schema/db-opts)
              (show "Created empty database")

              ;; 1. Static/reference entities first
              (d/fill-db (load-places data-dir))
              (d/fill-db (load-organizations data-dir))
              (d/fill-db (load-tagclasses data-dir))
              (d/fill-db (load-tags data-dir))
              (show "  Static entities loaded.")

              ;; 2. Person entities with embedded multi-valued attributes
              (d/fill-db (load-persons data-dir))
              (d/fill-db (load-person-interests data-dir))
              (show "  Person entities loaded.")

              ;; 3. Person relationships
              (d/fill-db (load-person-knows data-dir))
              (d/fill-db (load-person-study-at data-dir))
              (d/fill-db (load-person-work-at data-dir))
              (show "  Person relationships loaded.")

              ;; 4. Forums
              (d/fill-db (load-forums data-dir))
              (d/fill-db (load-forum-tags data-dir))
              (d/fill-db (load-forum-members data-dir))
              (show "  Forums loaded.")

              ;; 5. Posts
              (d/fill-db (load-posts data-dir))
              (d/fill-db (load-post-tags data-dir))
              (show "  Posts loaded.")

              ;; 6. Comments
              (d/fill-db (load-comments data-dir))
              (d/fill-db (load-comment-tags data-dir))
              (show "  Comments loaded.")

              ;; 7. Likes
              (d/fill-db (load-likes-post data-dir))
              (d/fill-db (load-likes-comment data-dir))
              (show "  Likes loaded."))]

      ;; Close the database to release resources
      (d/close-db final-db)

      (let [elapsed (/ (- (System/currentTimeMillis) start-time) 1000.0)]
        (println "================================================")
        (println (format "Data load complete in %.2f seconds" elapsed))))))
