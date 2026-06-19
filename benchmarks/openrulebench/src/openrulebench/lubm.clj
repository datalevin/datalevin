(ns openrulebench.lubm
  "LUBM (Lehigh University Benchmark) for OpenRuleBench.
   Implements a subset of LUBM queries on university domain data.

   LUBM tests ontology reasoning and rule inference on hierarchical data.
   See: http://swat.cse.lehigh.edu/projects/lubm/"
  (:import [java.util Random]))

;; =============================================================================
;; LUBM Instance Sizes
;; =============================================================================

(def lubm-instances
  "LUBM benchmark instances (number of universities).
   Each university generates approximately 100K triples."
  {:lubm-1  {:universities 1}    ;; ~100K triples
   :lubm-10 {:universities 10}   ;; ~1M triples
   :lubm-50 {:universities 50}}) ;; ~5M triples

;; =============================================================================
;; LUBM Schema (Datalevin)
;; =============================================================================

(def lubm-schema
  "Schema for LUBM benchmark.
   Models university entities: departments, professors, students, courses."
  {:type {:db/valueType :db.type/keyword}
   :name {:db/valueType :db.type/string}
   :email {:db/valueType :db.type/string}
   :memberOf {:db/valueType :db.type/ref}
   :worksFor {:db/valueType :db.type/ref}
   :teacherOf {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :advisor {:db/valueType :db.type/ref}
   :takesCourse {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :undergraduateDegreeFrom {:db/valueType :db.type/ref}
   :mastersDegreeFrom {:db/valueType :db.type/ref}
   :doctoralDegreeFrom {:db/valueType :db.type/ref}
   :subOrganizationOf {:db/valueType :db.type/ref}
   :publicationAuthor {:db/valueType :db.type/ref :db/cardinality :db.cardinality/many}
   :researchInterest {:db/valueType :db.type/string}})

;; =============================================================================
;; LUBM Type Hierarchy
;; =============================================================================

(def type-hierarchy
  "LUBM class hierarchy (simplified).
   Used for type inference in queries."
  {:GraduateStudent [:Student :Person]
   :UndergraduateStudent [:Student :Person]
   :Student [:Person]
   :Professor [:Faculty :Employee :Person]
   :AssociateProfessor [:Professor :Faculty :Employee :Person]
   :AssistantProfessor [:Professor :Faculty :Employee :Person]
   :FullProfessor [:Professor :Faculty :Employee :Person]
   :Lecturer [:Faculty :Employee :Person]
   :Faculty [:Employee :Person]
   :Employee [:Person]
   :TeachingAssistant [:Person]
   :ResearchAssistant [:Person]
   :Department [:Organization]
   :University [:Organization]
   :Course []
   :GraduateCourse [:Course]
   :Publication []
   :ResearchGroup [:Organization]})

;; =============================================================================
;; LUBM Rules (Type Inference)
;; =============================================================================

(def lubm-rules
  "Rules for LUBM type inference.
   Infers superclass membership from subclass relationships."
  '[;; GraduateStudent is-a Student
    [(is-a ?x :Student)
     [?x :type :GraduateStudent]]
    [(is-a ?x :Student)
     [?x :type :UndergraduateStudent]]
    ;; Student is-a Person
    [(is-a ?x :Person)
     (is-a ?x :Student)]
    ;; Professor hierarchy
    [(is-a ?x :Professor)
     [?x :type :FullProfessor]]
    [(is-a ?x :Professor)
     [?x :type :AssociateProfessor]]
    [(is-a ?x :Professor)
     [?x :type :AssistantProfessor]]
    [(is-a ?x :Faculty)
     (is-a ?x :Professor)]
    [(is-a ?x :Faculty)
     [?x :type :Lecturer]]
    [(is-a ?x :Employee)
     (is-a ?x :Faculty)]
    [(is-a ?x :Person)
     (is-a ?x :Employee)]
    ;; Direct types
    [(is-a ?x :Person)
     [?x :type :Person]]
    [(is-a ?x :GraduateStudent)
     [?x :type :GraduateStudent]]
    [(is-a ?x :UndergraduateStudent)
     [?x :type :UndergraduateStudent]]])

;; =============================================================================
;; LUBM Queries (Subset of 14 standard queries)
;; =============================================================================

(def lubm-queries
  "Standard LUBM queries (subset).
   Q1: Graduate students taking a specific course
   Q2: All graduate students (requires inference)
   Q4: Professors at a specific department
   Q9: Students with advisors"
  {:q1 '[:find ?x
         :in $ ?course
         :where
         [?x :type :GraduateStudent]
         [?x :takesCourse ?course]]

   :q2 '[:find ?x
         :in $ %
         :where
         (is-a ?x :GraduateStudent)]

   :q4 '[:find ?x ?name ?email
         :in $ ?dept
         :where
         [?x :type :Professor]
         [?x :worksFor ?dept]
         [?x :name ?name]
         [?x :email ?email]]

   :q9 '[:find ?x ?advisor
         :in $ %
         :where
         (is-a ?x :Student)
         [?x :advisor ?advisor]]

   :q14 '[:find ?x
          :where
          [?x :type :UndergraduateStudent]]})

;; =============================================================================
;; Synthetic LUBM Data Generation
;; =============================================================================

(def ^:private first-names
  ["James" "John" "Robert" "Michael" "William" "David" "Richard" "Joseph"
   "Thomas" "Mary" "Patricia" "Jennifer" "Linda" "Elizabeth" "Barbara"
   "Susan" "Jessica" "Sarah" "Karen" "Nancy" "Lisa" "Betty" "Margaret"])

(def ^:private last-names
  ["Smith" "Johnson" "Williams" "Brown" "Jones" "Garcia" "Miller" "Davis"
   "Rodriguez" "Martinez" "Hernandez" "Lopez" "Gonzalez" "Wilson" "Anderson"
   "Thomas" "Taylor" "Moore" "Jackson" "Martin" "Lee" "Perez" "Thompson"])

(def ^:private research-areas
  ["Artificial Intelligence" "Machine Learning" "Computer Vision"
   "Natural Language Processing" "Databases" "Distributed Systems"
   "Computer Networks" "Software Engineering" "Theory of Computation"
   "Computer Graphics" "Human-Computer Interaction" "Security"
   "Operating Systems" "Programming Languages" "Algorithms"])

(defn- gen-name [rng]
  (str (nth first-names (.nextInt rng (count first-names)))
       " "
       (nth last-names (.nextInt rng (count last-names)))))

(defn- gen-email [name dept-num]
  (-> name
      (clojure.string/lower-case)
      (clojure.string/replace " " ".")
      (str "@dept" dept-num ".edu")))

(defn generate-lubm-data
  "Generate synthetic LUBM data for a given number of universities.
   Each university has ~15 departments, each department has professors,
   graduate students, undergraduate students, and courses."
  [num-universities]
  (let [rng (Random. 42)
        depts-per-uni 15
        profs-per-dept 10
        grad-students-per-dept 30
        undergrad-students-per-dept 100
        courses-per-dept 20]

    (vec
      (for [uni-num (range num-universities)
            dept-num (range depts-per-uni)
            :let [uni-id (+ 1000000 uni-num)
                  dept-id (+ 2000000 (* uni-num 100) dept-num)
                  ;; University entity
                  uni-entity {:db/id uni-id
                              :type :University
                              :name (str "University" uni-num)}
                  ;; Department entity
                  dept-entity {:db/id dept-id
                               :type :Department
                               :name (str "Department" dept-num)
                               :subOrganizationOf uni-id}
                  ;; Professors
                  prof-entities
                  (for [p (range profs-per-dept)
                        :let [id (+ 3000000 (* uni-num 10000) (* dept-num 100) p)
                              name (gen-name rng)
                              type (case (mod p 3)
                                     0 :FullProfessor
                                     1 :AssociateProfessor
                                     2 :AssistantProfessor)]]
                    {:db/id id
                     :type type
                     :name name
                     :email (gen-email name dept-num)
                     :worksFor dept-id
                     :researchInterest (nth research-areas (.nextInt rng (count research-areas)))})
                  ;; Courses
                  course-entities
                  (for [c (range courses-per-dept)
                        :let [id (+ 4000000 (* uni-num 10000) (* dept-num 100) c)
                              is-grad? (< c 5)]]
                    {:db/id id
                     :type (if is-grad? :GraduateCourse :Course)
                     :name (str "Course" dept-num "-" c)})
                  course-ids (mapv :db/id course-entities)
                  prof-ids (mapv :db/id prof-entities)
                  ;; Graduate students
                  grad-entities
                  (for [s (range grad-students-per-dept)
                        :let [id (+ 5000000 (* uni-num 100000) (* dept-num 1000) s)
                              name (gen-name rng)
                              advisor-id (nth prof-ids (.nextInt rng (count prof-ids)))
                              courses (take 3 (shuffle course-ids))]]
                    {:db/id id
                     :type :GraduateStudent
                     :name name
                     :email (gen-email name dept-num)
                     :memberOf dept-id
                     :advisor advisor-id
                     :takesCourse courses})
                  ;; Undergraduate students
                  undergrad-entities
                  (for [s (range undergrad-students-per-dept)
                        :let [id (+ 6000000 (* uni-num 100000) (* dept-num 1000) s)
                              name (gen-name rng)
                              courses (take 5 (shuffle (drop 5 course-ids)))]]
                    {:db/id id
                     :type :UndergraduateStudent
                     :name name
                     :email (gen-email name dept-num)
                     :memberOf dept-id
                     :takesCourse courses})]]
        (concat [uni-entity dept-entity]
                prof-entities
                course-entities
                grad-entities
                undergrad-entities)))))

(defn flatten-lubm-data
  "Flatten nested LUBM data into a sequence of datoms."
  [data]
  (apply concat data))

;; =============================================================================
;; Instance Loading
;; =============================================================================

(defn load-lubm-instance
  "Load LUBM instance. Returns seq of datoms.
   Generates synthetic data based on number of universities."
  [instance-key]
  (let [{:keys [universities]} (get lubm-instances instance-key)]
    (when (nil? universities)
      (throw (ex-info (str "Unknown LUBM instance: " instance-key)
                      {:instance instance-key
                       :available (keys lubm-instances)})))
    (println "Generating LUBM data (" universities "universities)")
    (flatten-lubm-data (generate-lubm-data universities))))

(comment
  ;; Test data generation
  (def data (generate-lubm-data 1))
  (count (flatten-lubm-data data))

  ;; Sample entities
  (take 10 (flatten-lubm-data data))

  ;; Load instance
  (def instance (load-lubm-instance :lubm-1))
  (count instance)
  )
