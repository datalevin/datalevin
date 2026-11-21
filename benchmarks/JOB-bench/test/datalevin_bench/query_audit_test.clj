(ns datalevin-bench.query-audit-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin-bench.core :as job]
            [datalevin.core :as d]))

(defn- check-query [query data expected]
  (with-open [conn (d/create-conn nil job/schema
                                {:background-sampling? false})]
    (d/transact! conn data)
    (is (= expected (d/q query (d/db conn))))))

(deftest q1a-null-year-does-not-remove-other-minima
  (let [common [{:db/id 1 :company-type/kind "production companies"}
                {:db/id 2 :info-type/info "top 250 rank"}
                {:db/id 3 :title/title "A title with no year"}
                {:db/id 4 :movie-companies/movie 3 :movie-companies/company-type 1
                 :movie-companies/note "(co-production)"}
                {:db/id 5 :movie-info-idx/movie 3 :movie-info-idx/info-type 2}]]
    (check-query job/q-1a common [["(co-production)" "A title with no year" nil]])
    (check-query job/q-1a
                 (into common [{:db/id 6 :title/title "Z title" :title/production-year 2001}
                               {:db/id 7 :movie-companies/movie 6 :movie-companies/company-type 1
                                :movie-companies/note "(presents)"}
                               {:db/id 8 :movie-info-idx/movie 6 :movie-info-idx/info-type 2}])
                 [["(co-production)" "A title with no year" 2001]])))

(deftest q9b-uses-angel-filter
  (let [common [{:db/id 1 :company-name/country-code "[us]"}
                {:db/id 2 :role-type/role "actress"}
                {:db/id 3 :title/title "Movie" :title/production-year 2008}
                {:db/id 4 :movie-companies/movie 3 :movie-companies/company 1
                 :movie-companies/note "(2008) (USA)"}
                {:db/id 5 :char-name/name "Character"}]
        people (mapcat (fn [[id name aka]]
                         [{:db/id id :name/name name :name/gender "f"}
                          {:db/id (+ 1 id) :aka-name/person id :aka-name/name aka}
                          {:db/id (+ 2 id) :cast-info/person id :cast-info/movie 3
                           :cast-info/person-role 5 :cast-info/role 2 :cast-info/note "(voice)"}])
                       [[10 "Angie" "A wrong alias"] [20 "Angela" "Z valid alias"]])]
    (check-query job/q-9b (into common people)
                 [["Z valid alias" "Character" "Angela" "Movie"]])))

(deftest q14c-uses-danish-filter
  (let [common [{:db/id 1 :info-type/info "countries"}
                {:db/id 2 :info-type/info "rating"}
                {:db/id 3 :keyword/keyword "murder"}
                {:db/id 4 :kind-type/kind "movie"}]
        movies (mapcat (fn [[id country title]]
                         [{:db/id id :title/title title :title/production-year 2010 :title/kind 4}
                          {:db/id (+ 1 id) :movie-info/movie id :movie-info/info country :movie-info/info-type 1}
                          {:db/id (+ 2 id) :movie-info-idx/movie id :movie-info-idx/info "7.0" :movie-info-idx/info-type 2}
                          {:db/id (+ 3 id) :movie-keyword/movie id :movie-keyword/keyword 3}])
                       [[10 "Denish" "A wrong title"] [20 "Danish" "Z valid title"]])]
    (check-query job/q-14c (into common movies) [["7.0" "Z valid title"]])))

(deftest q15d-projects-alternate-title
  (let [common [{:db/id 1 :company-name/country-code "[us]"}
                {:db/id 2 :company-type/kind "production companies"}
                {:db/id 3 :info-type/info "release dates"}
                {:db/id 4 :keyword/keyword "keyword"}]
        movies (mapcat (fn [[id title alternate keyword]]
                         [{:db/id id :title/title title :title/production-year 2001}
                          {:db/id (+ 1 id) :aka-title/movie id :aka-title/title alternate}
                          {:db/id (+ 2 id) :movie-info/movie id :movie-info/info-type 3
                           :movie-info/note "internet" :movie-info/info "Not an alternate title"}
                          {:db/id (+ 3 id) :movie-companies/movie id :movie-companies/company 1
                           :movie-companies/company-type 2}
                          {:db/id (+ 4 id) :movie-keyword/movie id :movie-keyword/keyword keyword}])
                       [[10 "Z valid movie" "Z alternate" 4]])]
    (check-query job/q-15d (into common movies) [["Z alternate" "Z valid movie"]])))
