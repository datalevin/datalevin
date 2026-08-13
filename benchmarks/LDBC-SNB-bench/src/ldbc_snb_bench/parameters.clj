(ns ldbc-snb-bench.parameters
  "Loading and validating LDBC SNB Interactive v1 substitution parameters."
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [clojure.string :as str])
  (:import
   [java.io File]
   [java.time Instant]
   [java.util Date]))

(def query-parameter-keys
  {:ic1  [:person-id :first-name]
   :ic2  [:person-id :max-date]
   :ic3  [:person-id :country-x-name :country-y-name
          :start-date :duration-days]
   :ic4  [:person-id :start-date :duration-days]
   :ic5  [:person-id :min-date]
   :ic6  [:person-id :tag-name]
   :ic7  [:person-id]
   :ic8  [:person-id]
   :ic9  [:person-id :max-date]
   :ic10 [:person-id :month]
   :ic11 [:person-id :country-name :work-from-year]
   :ic12 [:person-id :tag-class-name]
   :ic13 [:person1-id :person2-id]
   :ic14 [:person1-id :person2-id]
   :is1  [:person-id]
   :is2  [:person-id]
   :is3  [:person-id]
   :is4  [:message-id]
   :is5  [:message-id]
   :is6  [:message-id]
   :is7  [:message-id]})

(def ^:private long-keys
  #{:person-id :message-id :person1-id :person2-id
    :duration-days :month :work-from-year})

(def ^:private date-keys
  #{:max-date :min-date :start-date})

(def official-specs
  {:ic1  {:file "interactive_1_param.txt"
          :columns [["personId" :person-id]
                    ["firstName" :first-name]]}
   :ic2  {:file "interactive_2_param.txt"
          :columns [["personId" :person-id]
                    ["maxDate" :max-date]]}
   :ic3  {:file "interactive_3_param.txt"
          :columns [["personId" :person-id]
                    ["startDate" :start-date]
                    ["durationDays" :duration-days]
                    ["countryXName" :country-x-name]
                    ["countryYName" :country-y-name]]}
   :ic4  {:file "interactive_4_param.txt"
          :columns [["personId" :person-id]
                    ["startDate" :start-date]
                    ["durationDays" :duration-days]]}
   :ic5  {:file "interactive_5_param.txt"
          :columns [["personId" :person-id]
                    ["minDate" :min-date]]}
   :ic6  {:file "interactive_6_param.txt"
          :columns [["personId" :person-id]
                    ["tagName" :tag-name]]}
   :ic7  {:file "interactive_7_param.txt"
          :columns [["personId" :person-id]]}
   :ic8  {:file "interactive_8_param.txt"
          :columns [["personId" :person-id]]}
   :ic9  {:file "interactive_9_param.txt"
          :columns [["personId" :person-id]
                    ["maxDate" :max-date]]}
   :ic10 {:file "interactive_10_param.txt"
          :columns [["personId" :person-id]
                    ["month" :month]]}
   :ic11 {:file "interactive_11_param.txt"
          :columns [["personId" :person-id]
                    ["countryName" :country-name]
                    ["workFromYear" :work-from-year]]}
   :ic12 {:file "interactive_12_param.txt"
          :columns [["personId" :person-id]
                    ["tagClassName" :tag-class-name]]}
   :ic13 {:file "interactive_13_param.txt"
          :columns [["person1Id" :person1-id]
                    ["person2Id" :person2-id]]}
   :ic14 {:file "interactive_14_param.txt"
          :columns [["person1Id" :person1-id]
                    ["person2Id" :person2-id]]}})

(def ^:private external-key-aliases
  (into {}
        (mapcat (fn [[_ {:keys [columns]}]]
                  (map (fn [[header parameter-key]]
                         [header parameter-key])
                       columns)))
        official-specs))

(defn query-key
  "Normalize an IC/IS query identifier to a lower-case keyword."
  [value]
  (let [normalized (-> (if (keyword? value) (name value) (str value))
                       str/trim
                       str/lower-case
                       keyword)]
    (when-not (contains? query-parameter-keys normalized)
      (throw (ex-info (str "Unknown query in parameter suite: " value)
                      {:query value})))
    normalized))

(defn to-date
  "Coerce an epoch-millisecond or ISO-8601 value to java.util.Date."
  [value]
  (cond
    (instance? Date value) value
    (instance? Instant value) (Date/from ^Instant value)
    (integer? value) (Date. (long value))
    (number? value) (Date. (long value))
    (string? value)
    (let [value (str/trim value)]
      (if (re-matches #"[-+]?\d+" value)
        (Date. (Long/parseLong value))
        (Date/from (Instant/parse value))))
    :else
    (throw (ex-info (str "Expected an epoch-millisecond or ISO-8601 date, got "
                         (pr-str value))
                    {:value value}))))

(defn- parameter-key
  [key]
  (cond
    (keyword? key) key
    (string? key) (or (get external-key-aliases key)
                      (keyword key))
    :else (keyword (str key))))

(defn- coerce-long
  [query parameter-key value]
  (try
    (cond
      (integer? value) (long value)
      (number? value) (long value)
      (string? value) (Long/parseLong (str/trim value))
      :else (throw (NumberFormatException.)))
    (catch NumberFormatException _
      (throw (ex-info (str "Expected an integer for " (name parameter-key)
                           " in " (str/upper-case (name query)) ", got "
                           (pr-str value))
                      {:query query :parameter parameter-key :value value})))))

(defn normalize-parameter-map
  "Normalize keys and scalar types, then require every parameter for query."
  [query value]
  (let [query      (query-key query)
        _          (when-not (map? value)
                     (throw (ex-info (str "Parameters for "
                                          (str/upper-case (name query))
                                          " must be a map")
                                     {:query query :value value})))
        normalized (into {}
                         (map (fn [[key v]] [(parameter-key key) v]))
                         value)
        unknown    (remove (set (get query-parameter-keys query))
                           (keys normalized))
        _          (when (seq unknown)
                     (throw (ex-info (str "Unknown parameters for "
                                          (str/upper-case (name query)) ": "
                                          (str/join ", " (map name unknown)))
                                     {:query query :unknown (vec unknown)})))
        result     (reduce-kv
                     (fn [m key v]
                       (assoc m key
                              (cond
                                (contains? long-keys key)
                                (coerce-long query key v)

                                (contains? date-keys key)
                                (try
                                  (to-date v)
                                  (catch Exception cause
                                    (throw (ex-info
                                             (str "Invalid date for " (name key)
                                                  " in "
                                                  (str/upper-case (name query)))
                                             {:query query
                                              :parameter key
                                              :value v}
                                             cause))))

                                :else
                                (let [text (str v)]
                                  (when (str/blank? text)
                                    (throw (ex-info
                                             (str "Expected a non-blank value for "
                                                  (name key) " in "
                                                  (str/upper-case (name query)))
                                             {:query query
                                              :parameter key
                                              :value v})))
                                  text))))
                     {}
                     normalized)
        missing    (remove #(contains? result %)
                           (get query-parameter-keys query))]
    (when (seq missing)
      (throw (ex-info (str "Missing parameters for "
                           (str/upper-case (name query)) ": "
                           (str/join ", " (map name missing)))
                      {:query query :missing (vec missing)})))
    result))

(defn- split-pipe
  [line]
  (str/split (str/replace line #"\r$" "") #"\|" -1))

(defn read-official-file
  "Read one official interactive_N_param.txt file."
  [query file]
  (let [query            (query-key query)
        {:keys [columns]} (get official-specs query)
        file             (io/file file)]
    (when-not columns
      (throw (ex-info (str "No official substitution file exists for "
                           (str/upper-case (name query)))
                      {:query query})))
    (when-not (.isFile ^File file)
      (throw (ex-info (str "Parameter file does not exist: " (.getPath file))
                      {:query query :file (.getPath file)})))
    (with-open [reader (io/reader file :encoding "UTF-8")]
      (let [lines            (vec (line-seq reader))
            _                (when (empty? lines)
                               (throw (ex-info
                                        (str "Parameter file is empty: "
                                             (.getPath file))
                                        {:query query :file (.getPath file)})))
            headers          (-> (first lines)
                                 (str/replace-first "\ufeff" "")
                                 split-pipe)
            expected-headers (mapv first columns)]
        (when-not (= expected-headers headers)
          (throw (ex-info (str "Unexpected header in " (.getPath file)
                               "; expected " (str/join "|" expected-headers)
                               ", got " (str/join "|" headers))
                          {:query query
                           :file (.getPath file)
                           :expected expected-headers
                           :actual headers})))
        (mapv
          (fn [[offset line]]
            (let [values (split-pipe line)
                  line-no (+ offset 2)]
              (when-not (= (count columns) (count values))
                (throw (ex-info (str "Wrong column count in " (.getPath file)
                                     " at line " line-no)
                                {:query query
                                 :file (.getPath file)
                                 :line line-no
                                 :expected (count columns)
                                 :actual (count values)})))
              (try
                (normalize-parameter-map
                  query
                  (into {}
                        (map (fn [[[_ key] value]] [key value]))
                        (map vector columns values)))
                (catch Exception cause
                  (throw (ex-info (str "Invalid parameter row in "
                                       (.getPath file) " at line " line-no)
                                  {:query query
                                   :file (.getPath file)
                                   :line line-no}
                                  cause))))))
          (map-indexed vector (remove str/blank? (rest lines))))))))

(defn read-official-directory
  "Read every interactive_N_param.txt found in a Datagen parameter directory."
  [path]
  (let [dir (io/file path)]
    (when-not (.isDirectory ^File dir)
      (throw (ex-info (str "Parameter directory does not exist: " path)
                      {:path path})))
    (let [present (into (sorted-map)
                        (keep (fn [[query {:keys [file]}]]
                                (let [parameter-file (io/file dir file)]
                                  (when (.isFile ^File parameter-file)
                                    [query (read-official-file
                                             query parameter-file)]))))
                        official-specs)]
      (when (empty? present)
        (throw (ex-info (str "No interactive_N_param.txt files found in " path)
                        {:path path})))
      {:kind :official-directory
       :path (.getCanonicalPath ^File dir)
       :parameters present
       :origins (into {}
                      (map (fn [[query {:keys [file]}]]
                             [query {:kind :official-file
                                     :path (.getCanonicalPath
                                             ^File (io/file dir file))}]))
                      (filter (fn [[query _]] (contains? present query))
                              official-specs))})))

(defn read-edn-suite
  "Read an EDN map of query keys to parameter maps or vectors of maps."
  [path]
  (let [file (io/file path)]
    (when-not (.isFile ^File file)
      (throw (ex-info (str "Parameter suite does not exist: " path)
                      {:path path})))
    (let [form (edn/read-string (slurp file :encoding "UTF-8"))
          form (if (and (map? form) (map? (:parameters form)))
                 (:parameters form)
                 form)]
      (when-not (map? form)
        (throw (ex-info "An EDN parameter suite must be a map"
                        {:path path})))
      (let [parameters
            (into (sorted-map)
                  (map (fn [[raw-query raw-rows]]
                         (let [query (query-key raw-query)
                               rows  (cond
                                       (map? raw-rows) [raw-rows]
                                       (sequential? raw-rows) (vec raw-rows)
                                       :else
                                       (throw (ex-info
                                                (str "Parameter rows for "
                                                     raw-query
                                                     " must be a map or sequence")
                                                {:query query
                                                 :value raw-rows})))]
                           (when (empty? rows)
                             (throw (ex-info (str "No parameter rows for "
                                                  raw-query)
                                             {:query query})))
                           [query (mapv #(normalize-parameter-map query %) rows)])))
                  form)
            canonical (.getCanonicalPath ^File file)]
        (when (empty? parameters)
          (throw (ex-info "The EDN parameter suite is empty" {:path path})))
        {:kind :edn
         :path canonical
         :parameters parameters
         :origins (into {}
                        (map (fn [query]
                               [query {:kind :edn :path canonical}]))
                        (keys parameters))}))))

(defn load-source
  "Load an official parameter directory or an EDN parameter suite."
  [path]
  (let [file (io/file path)]
    (cond
      (.isDirectory ^File file) (read-official-directory path)
      (.isFile ^File file) (read-edn-suite path)
      :else (throw (ex-info (str "Parameter source does not exist: " path)
                            {:path path})))))
