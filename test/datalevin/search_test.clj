(ns datalevin.search-test
  (:require
   [datalevin.search :as sut]
   [datalevin.search-utils :as su]
   [datalevin.interpret :as i]
   [datalevin.core :as d]
   [datalevin.analyzer :as a]
   [datalevin.util :as u]
   [datalevin.csv :as csv]
   [clojure.java.io :as io]
   [clojure.string :as s]
   [jsonista.core :as json]
   [datalevin.test.core :as tdc :refer [db-fixture]]
   [clojure.test :refer [deftest are is use-fixtures]])
  (:import
   [java.util UUID]))

(use-fixtures :each db-fixture)

(deftest english-analyzer-test
  (let [s1 "This is a Datalevin-Analyzers test"
        s2 "This is a Datalevin-Analyzers test. "]
    (is (= (a/en-analyzer s1)
           (a/en-analyzer s2)
           [["datalevin-analyzers" 3 10] ["test" 4 30]]))
    (is (= (subs s1 10 (+ 10 (.length "datalevin-analyzers")))
           "Datalevin-Analyzers" ))))

(deftest parse-query-test
  (are [query result] (= (sut/parse-query* a/en-analyzer query) result)
    " "                                    nil
    "red"                                  "red"
    " red "                                "red"
    "red fox"                              [:or "red" "fox"]
    [:or " " "red"]                        [:or "red"]
    [:or "red" [:not "fox"]]               [:or "red" [:not "fox"]]
    [:or "red fox back" " fox "]           [:or [:or "red" "fox" "back"] "fox"]
    [:or "fox" "red" [:and "black" "cat"]] [:or "fox" "red" [:and "black" "cat"]])

  (are [query]
      (thrown-with-msg? Exception #"Invalid search query"
                        (sut/parse-query* a/en-analyzer query))
    []
    [:or]
    [:not]
    ["book"]
    ["book" "red"]
    [:none "book"]))

(deftest required-terms-test
  (are [expr result] (= result (:req (sut/required-terms {:query expr})))
    [:or "a" "b"]  #{"a" "b"}
    [:and "a" "b"] #{"a" "b"}
    [:not "a"]     #{}

    [:or "fox" "red" [:and "black" "sheep"] [:not "yellow"]]
    #{"fox" "red" "black" "sheep"}

    [:not [:or "b" "c"]]                           #{}
    [:and "a" [:or "b" "c"] [:not [:and "d" "e"]]] #{"a" "b" "c"}))

(deftest fulltext-fns-test
  (let [analyzer (i/inter-fn
                     [^String text]
                   (map-indexed (fn [i ^String t]
                                  [t i (.indexOf text t)])
                                (s/split text #"\s")))
        dir      (u/tmp-dir (str "fulltext-fns-" (UUID/randomUUID)))
        conn     (d/create-conn
                   dir {:a/id     {:db/valueType :db.type/long
                                   :db/unique    :db.unique/identity}
                        :a/string {:db/valueType           :db.type/string
                                   :db/fulltext            true
                                   :db.fulltext/autoDomain true}
                        :b/string {:db/valueType :db.type/string
                                   :db/fulltext  true}}
                   {:auto-entity-time? true
                    :search-opts       {:analyzer analyzer}})
        s        "The quick brown fox jumps over the lazy dog"]
    (d/transact! conn [{:a/id 1 :a/string s :b/string ""}])
    (d/transact! conn [{:a/id 1 :a/string s :b/string "bar"}])
    (is (= (d/q '[:find ?v .
                  :in $ ?q
                  :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                (d/db conn) "brown fox")
           s))
    (is (= (d/q '[:find ?v .
                  :in $ ?q
                  :where [(fulltext $ ?q {:top 2}) [[?e ?a ?v]]]]
                (d/db conn) "brown fox")
           s))
    (is (= (d/q '[:find ?v .
                  :in $ ?q
                  :where [(fulltext $ :a/string ?q) [[?e ?a ?v]]]]
                (d/db conn) "brown fox")
           s))
    (is (= (d/q '[:find ?v .
                  :in $ ?q
                  :where [(fulltext $ :a/string ?q {:top 1}) [[?e ?a ?v]]]]
                (d/db conn) "brown fox")
           s))
    (is (empty? (d/q '[:find ?v .
                       :in $ ?q
                       :where [(fulltext $ ?q) [[?e ?a ?v]]]]
                     (d/db conn) "")))
    (is (= (peek (first (d/fulltext-datoms (d/db conn) "brown fox")))
           s))
    (d/close conn)
    (u/delete-files dir)))

(deftest fulltext-display-option-test
  (let [analyzer (i/inter-fn
                    [^String text]
                  (map-indexed (fn [i ^String t]
                                 [t i (.indexOf text t)])
                               (s/split text #"\s")))
        dir      (u/tmp-dir (str "fulltext-display-" (UUID/randomUUID)))
        conn     (d/create-conn
                   dir {:a/id     {:db/valueType :db.type/long
                                   :db/unique    :db.unique/identity}
                        :a/string {:db/valueType           :db.type/string
                                   :db/fulltext            true
                                   :db.fulltext/autoDomain true}}
                   {:search-opts {:analyzer        analyzer
                                  :index-position? true
                                  :include-text?   true}})
        s        "The quick brown fox jumps over the lazy dog"]
    (d/transact! conn [{:a/id 1 :a/string s}])
    (is (= s
           (d/q '[:find ?text .
                  :in $ ?q
                  :where
                  [(fulltext $ ?q {:top 1 :display :texts}) [[_ _ _ ?text]]]]
                (d/db conn) "brown fox")))
    (let [offsets    (d/q '[:find ?offsets .
                            :in $ ?q
                            :where
                            [(fulltext $ ?q {:top 1 :display :offsets})
                             [[_ _ _ ?offsets]]]]
                          (d/db conn) "brown fox")
          offset-map (into {} offsets)]
      (is (= [10] (get offset-map "brown")))
      (is (= [16] (get offset-map "fox"))))
    (let [[text offsets] (first
                           (d/q '[:find ?text ?offsets
                                  :in $ ?q
                                  :where
                                  [(fulltext $ ?q {:top 1
                                                   :display :texts+offsets})
                                   [[_ _ _ ?text ?offsets]]]]
                                (d/db conn) "brown fox"))
          offset-map     (into {} offsets)]
      (is (= s text))
      (is (= [10] (get offset-map "brown")))
      (is (= [16] (get offset-map "fox"))))
    (d/close conn)
    (u/delete-files dir)))

(defn- rows->maps [csv]
  (let [headers (map keyword (first csv))
        rows    (rest csv)]
    (map #(zipmap headers %) rows)))

(deftest load-csv-test
  (let [dir  (u/tmp-dir (str "load-csv-test-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:id          {:db/valueType :db.type/string
                                  :db/unique    :db.unique/identity}
                    :description {:db/valueType :db.type/string
                                  :db/fulltext  true}})
        data (rows->maps
               (with-open [reader (io/reader "test/data/data.csv")]
                 (doall (csv/read-csv reader))))]
    (d/transact! conn data)
    (is (= 36 (count (d/fulltext-datoms (d/db conn) "Memorex" {:top 100}))))
    (is (= (d/q '[:find (count ?e) .
                  :in $ ?q
                  :where [(fulltext $ ?q {:top 100}) [[?e _ _]]]]
                (d/db conn) "Memorex")
           36))
    (d/transact! conn [{:id          "42"
                        :description "This is a new description"}])
    (is (= (d/q '[:find ?d .
                  :in $ ?i
                  :where
                  [?e :id ?i]
                  [?e :description ?d]]
                (d/db conn) "42")
           "This is a new description"))
    (d/close conn)
    (u/delete-files dir)))

(deftest load-json-test
  (let [dir  (u/tmp-dir (str "load-json-test-" (UUID/randomUUID)))
        conn (d/create-conn
               dir {:id          {:db/valueType :db.type/long
                                  :db/unique    :db.unique/identity}
                    :description {:db/valueType :db.type/string
                                  :db/fulltext  true}})
        data (json/read-value (slurp "test/data/data.json")
                              json/keyword-keys-object-mapper)]
    (d/transact! conn data)
    (is (= 1 (count (d/fulltext-datoms (d/db conn) "GraphstatsR"))))
    (is (= (update (d/q '[:find [?i ?d]
                          :in $ ?q
                          :where
                          [(fulltext $ ?q) [[?e _ ?d]]]
                          [?e :id ?i]]
                        (d/db conn) "GraphstatsR")
                   1 count)
           [6299 508]))
    (d/transact! conn [{:id          6299
                        :description "This is a new description"}])
    (is (= (d/q '[:find ?d .
                  :in $ ?i
                  :where
                  [?e :id ?i]
                  [?e :description ?d]]
                (d/db conn) 6299)
           "This is a new description"))
    (d/close conn)
    (u/delete-files dir)))
(deftest domain-test
  (let [dir      (u/tmp-dir (str "domain-test-" (UUID/randomUUID)))
        analyzer (su/create-analyzer
                   {:token-filters [(su/create-stemming-token-filter
                                      "english")]})
        conn     (d/create-conn
                   dir
                   {:a/id     {:db/valueType :db.type/long
                               :db/unique    :db.unique/identity}
                    :a/string {:db/valueType        :db.type/string
                               :db/fulltext         true
                               :db.fulltext/domains ["da"]}
                    :b/string {:db/valueType           :db.type/string
                               :db/fulltext            true
                               :db.fulltext/autoDomain true
                               :db.fulltext/domains    ["db"]}}
                   {:search-domains {"da" {:analyzer analyzer}
                                     "db" {}}})
        sa       "The quick brown fox jumps over the lazy dogs"
        sb       "Pack my box with five dozen liquor jugs."
        sc       "How vexingly quick daft zebras jump!"
        sd       "Five dogs jump over my fence."]
    (d/transact! conn [{:a/id 1 :a/string sa :b/string sb}])
    (d/transact! conn [{:a/id 2 :a/string sc :b/string sd}])
    (is (thrown-with-msg? Exception #":db.fulltext/autoDomain"
                          (d/q '[:find [?v ...]
                                 :in $ ?q
                                 :where
                                 [(fulltext $ :a/string ?q) [[?e _ ?v]]]]
                               (d/db conn) "jump")))
    (is (= (d/q '[:find [?v ...]
                  :in $ ?q
                  :where
                  [(fulltext $ :b/string ?q) [[?e _ ?v]]]]
                (d/db conn) "jump")
           [sd]))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["da"]}) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc}))
    (is (= (d/q '[:find [?v ...]
                  :in $ ?q
                  :where
                  [(fulltext $ ?q {:domains ["db"]}) [[?e _ ?v]]]]
                (d/db conn) "jump")
           [sd]))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["da" "db"]}) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc sd}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "jump"))
           #{sa sc sd}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "dog"))
           #{sa}))
    (is (= (set (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q) [[?e _ ?v]]]]
                     (d/db conn) "dogs"))
           #{sa sd}))
    (is (empty? (d/q '[:find [?v ...]
                       :in $ ?q
                       :where
                       [(fulltext $ ?q {:domains ["db"]}) [[?e _ ?v]]]]
                     (d/db conn) "dog")))
    (d/close conn)
    (u/delete-files dir)))

;; TODO double compares are not really reliable
;; (def tokens ["b" "c" "d" "e" "f" "g" "h" "i" "j" "k" "l" "m" "n"
;;              "o" "p" "q" "r" "s" "t" "u" "v" "w" "x" "y" "z"])

;; (def doc-num 10)

;; (defn- search-correct
;;   [data query results]
;;   (let [refs (keys data)
;;         docs (vals data)

;;         dfreqs (zipmap refs (map frequencies docs))

;;         norms (zipmap refs (map count (vals dfreqs)))

;;         terms  (s/split query #" ")
;;         qfreqs (frequencies terms)

;;         dfs (reduce (fn [tm term]
;;                       (assoc tm term
;;                              (reduce (fn [dc [_ freqs]]
;;                                        (if (freqs term)
;;                                          (inc ^long dc)
;;                                          dc))
;;                                      0
;;                                      dfreqs)))
;;                     {}
;;                     terms)
;;         wqs (reduce (fn [m [term freq]]
;;                       (assoc m term
;;                              (* ^double (if/tf* freq)
;;                                 ^double (if/idf (dfs term) doc-num))))
;;                     {}
;;                     qfreqs)
;;         rc  (count results)]
;;     (println "results =>" results)
;;     (= results
;;        (->> data
;;             (sort-by
;;               (fn [[ref _]]
;;                 (reduce
;;                   +
;;                   (map (fn [term]
;;                          (/ ^double
;;                             (* (double (or (wqs term) 0.0))
;;                                ^double (if/tf*
;;                                          (or (get-in dfreqs [ref term])
;;                                              0.0)))
;;                             (double (norms ref))))
;;                        terms)))
;;               >)
;;             (#(do (println "data sorted by query =>" %) %))
;;             (take rc)
;;             (map first)))))

;; (test/defspec search-generative-test
;;   1
;;   (prop/for-all
;;     [refs (gen/vector-distinct gen/nat {:num-elements doc-num})
;;      docs (gen/vector-distinct
;;             (gen/vector (gen/elements tokens) 3 10) ;; doc length
;;             {:num-elements doc-num}) ;; num of docs
;;      qs (gen/vector (gen/vector (gen/elements tokens) 1 3) 1)]
;;     (let [dir     (u/tmp-dir (str "search-test-" (UUID/randomUUID)))
;;           lmdb    (d/open-kv dir)
;;           engine  (d/new-search-engine lmdb)
;;           data    (zipmap refs docs)
;;           _       (println "data =>" data)
;;           jf      #(s/join " " %)
;;           txs     (zipmap refs (map jf docs))
;;           queries (map jf qs)
;;           _       (doseq [[k v] txs] (d/add-doc engine k v))
;;           _       (println "queries =>" queries)
;;           ok      (every? #(search-correct data % (d/search engine %))
;;                           queries)
;;           ]
;;       (if/close-kv lmdb)
;;       (u/delete-files dir)
;;       ok)))
