(ns openrulebench.dblp
  "DBLP benchmark for OpenRuleBench.
   Parses DBLP XML data and converts to EAV format for Datalevin.

   The DBLP benchmark tests a 4-way self-join query on publication data.
   See: https://dblp.uni-trier.de/"
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util.zip GZIPInputStream]
           [java.io BufferedReader InputStreamReader]))

;; =============================================================================
;; Constants
;; =============================================================================

(def data-dir "data/dblp")
(def dblp-xml-file (str data-dir "/dblp.xml"))
(def dblp-gz-file (str data-dir "/dblp.xml.gz"))

;; DBLP record types we care about
(def record-types
  #{:article :inproceedings :proceedings :book :incollection
    :phdthesis :mastersthesis :www})

;; Attributes we extract for the benchmark
(def relevant-attrs
  #{:title :author :year :month :journal :booktitle :publisher :url})

;; =============================================================================
;; DBLP Instance Sizes (OpenRuleBench standard)
;; =============================================================================

(def dblp-instances
  "DBLP benchmark instances (number of papers to include)."
  {:small  {:papers 2000}
   :medium {:papers 8000}
   :large  {:papers 64000}})


;; =============================================================================
;; XML Parsing (simple line-based parser, no external dependencies)
;; =============================================================================

(defn- parse-dblp-xml
  "Simple line-based parser for DBLP XML.
   Extracts key attributes without full XML parsing."
  [filepath max-records]
  (let [input (if (str/ends-with? filepath ".gz")
                (-> filepath io/input-stream GZIPInputStream.)
                (io/input-stream filepath))
        reader (BufferedReader. (InputStreamReader. input "UTF-8"))]
    (loop [records []
           current-record nil
           line-num 0]
      (if (>= (count records) max-records)
        records
        (if-let [line (.readLine reader)]
          (let [trimmed (str/trim line)]
            (cond
              ;; Start of a record
              (and (nil? current-record)
                   (some #(str/starts-with? trimmed (str "<" (name %) " "))
                         record-types))
              (let [key-match (re-find #"key=\"([^\"]+)\"" trimmed)]
                (recur records
                       (when key-match {:key (second key-match) :attrs {}})
                       (inc line-num)))

              ;; End of a record
              (and current-record
                   (some #(= trimmed (str "</" (name %) ">"))
                         record-types))
              (let [attrs (:attrs current-record)
                    key (:key current-record)]
                (if (and key
                         (contains? attrs :title)
                         (contains? attrs :year)
                         (contains? attrs :author))
                  (recur (conj records (assoc attrs :key key))
                         nil
                         (inc line-num))
                  (recur records nil (inc line-num))))

              ;; Inside a record - extract attributes
              current-record
              (let [tag-match (re-find #"<(\w+)>([^<]+)</\1>" trimmed)
                    tag (when tag-match (keyword (second tag-match)))
                    value (when tag-match (nth tag-match 2))]
                (if (and tag (relevant-attrs tag))
                  (recur records
                         (update-in current-record [:attrs tag]
                                    (fnil conj []) value)
                         (inc line-num))
                  (recur records current-record (inc line-num))))

              :else
              (recur records current-record (inc line-num))))
          ;; EOF
          records)))))

;; =============================================================================
;; Synthetic DBLP Data Generation
;; =============================================================================

(defn generate-dblp-data
  "Generate synthetic DBLP-like data for benchmarking.
   Generates papers with title, author, year, and month attributes."
  [num-papers]
  (let [years (range 1990 2024)
        months ["January" "February" "March" "April" "May" "June"
                "July" "August" "September" "October" "November" "December"]
        authors (mapv #(str "Author" %) (range 1 1001))
        titles (mapv #(str "Paper on Topic " % " with Novel Approach") (range 1 10001))]
    (for [i (range num-papers)]
      {:key (str "journals/synthetic/paper" i)
       :title [(nth titles (mod i (count titles)))]
       :author [(nth authors (mod i (count authors)))
                (nth authors (mod (+ i 1) (count authors)))]
       :year [(str (nth years (mod i (count years))))]
       :month [(nth months (mod i (count months)))]})))

;; =============================================================================
;; EAV Conversion (for Datalevin)
;; =============================================================================

(defn records->eav
  "Convert DBLP records to EAV (Entity-Attribute-Value) datoms.
   Each record attribute becomes multiple datoms:
   {:db/id <key> :att/attribute <attr> :att/value <value>}"
  [records]
  (for [record records
        :let [id (:key record)]
        [attr values] (dissoc record :key)
        value (if (coll? values) values [values])]
    {:db/id id
     :att/attribute attr
     :att/value (str value)}))

;; =============================================================================
;; Instance Loading
;; =============================================================================

(defn load-dblp-instance
  "Load DBLP instance. Returns seq of EAV datoms.
   If DBLP XML file exists, parses it; otherwise generates synthetic data."
  [instance-key]
  (let [{:keys [papers]} (get dblp-instances instance-key)
        _ (when (nil? papers)
            (throw (ex-info (str "Unknown DBLP instance: " instance-key)
                            {:instance instance-key
                             :available (keys dblp-instances)})))
        xml-file (io/file dblp-xml-file)
        gz-file (io/file dblp-gz-file)
        records (cond
                  (.exists xml-file)
                  (do (println "Loading DBLP from" dblp-xml-file)
                      (parse-dblp-xml dblp-xml-file papers))

                  (.exists gz-file)
                  (do (println "Loading DBLP from" dblp-gz-file)
                      (parse-dblp-xml dblp-gz-file papers))

                  :else
                  (do (println "Generating synthetic DBLP data (" papers "papers)")
                      (generate-dblp-data papers)))]
    (vec (records->eav records))))

(comment
  ;; Test synthetic data generation
  (def data (generate-dblp-data 10))
  (count data)
  (first data)

  ;; Test EAV conversion
  (def eav (records->eav data))
  (count eav)
  (take 5 eav)

  ;; Load instance
  (def instance (load-dblp-instance :small))
  (count instance)
  )
