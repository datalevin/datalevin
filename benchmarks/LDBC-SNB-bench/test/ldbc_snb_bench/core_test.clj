(ns ldbc-snb-bench.core-test
  "Tests for LDBC SNB queries to verify correctness of results.
   Uses the same sample parameters as the benchmark."
  (:require [ldbc-snb-bench.core :as sut]
            [ldbc-snb-bench.queries.interactive :as ic]
            [ldbc-snb-bench.queries.short :as is]
            [datalevin.core :as d]
            [clojure.test :as t :refer [deftest is testing use-fixtures]]))

;; ============================================================
;; Test fixtures
;; ============================================================

(def ^:dynamic *conn* nil)
(def ^:dynamic *db* nil)

(defn db-fixture [f]
  (let [conn (sut/get-conn)]
    (binding [*conn* conn
              *db*   (d/db conn)]
      (try
        (f)
        (finally
          (d/close conn))))))

(use-fixtures :once db-fixture)

;; ============================================================
;; Helper functions
;; ============================================================

(defn run-query-with-params
  "Run a query definition with the given parameters and return results."
  [query-def params]
  (sut/run-query *db* query-def params))

(defn first-row [result]
  (first (:rows result)))

(defn row-count [result]
  (:result-count result))

;; ============================================================
;; Interactive Short Query Tests (IS1-IS7)
;; ============================================================

(deftest is1-test
  (testing "IS1: Profile of a Person"
    (let [result (run-query-with-params is/is1 {:person-id 100})
          row (first-row result)]
      (is (= 1 (row-count result)))
      ;; [first-name last-name birthday location-ip browser-used city-id gender creation-date]
      (is (= "Nicolas" (nth row 0)))
      (is (= "Diaz" (nth row 1)))
      (is (= "190.5.24.199" (nth row 3)))
      (is (= "Chrome" (nth row 4)))
      (is (= 975 (nth row 5)))
      (is (= "male" (nth row 6))))))

(deftest is2-test
  (testing "IS2: Recent messages of a Person"
    (let [result (run-query-with-params is/is2 {:person-id 100})
          row (first-row result)]
      (is (= 10 (row-count result)))
      ;; [message-id message-content message-date post-id original-id original-first original-last]
      (is (= 2336466814835 (nth row 0)))
      (is (= 17592186048763 (nth row 4)))
      (is (= "Carlos" (nth row 5)))
      (is (= "Araya" (nth row 6))))))

(deftest is3-test
  (testing "IS3: Friends of a Person"
    (let [result (run-query-with-params is/is3 {:person-id 100})]
      ;; Should have 29 friends based on the results file
      (is (= 29 (row-count result)))
      ;; First friend (most recent) - [friend-id first-name last-name creation-date]
      (let [row (first-row result)]
        (is (= 37383395345539 (nth row 0)))
        (is (= "Benedict" (nth row 1)))
        (is (= "Zhang" (nth row 2)))))))

(deftest is4-test
  (testing "IS4: Content of a message"
    (let [result (run-query-with-params is/is4 {:message-id 1099512606636})
          row (first-row result)]
      (is (= 1 (row-count result)))
      ;; [creation-date content]
      (is (= "photo1099512606636.jpg" (nth row 1))))))

(deftest is5-test
  (testing "IS5: Creator of a message"
    (let [result (run-query-with-params is/is5 {:message-id 1099512606636})
          row (first-row result)]
      (is (= 1 (row-count result)))
      ;; [person-id first-name last-name]
      (is (= 4398046518478 (nth row 0)))
      (is (= "George" (nth row 1)))
      (is (= "Wilson" (nth row 2))))))

(deftest is6-test
  (testing "IS6: Forum of a message"
    (let [result (run-query-with-params is/is6 {:message-id 1099512606636})
          row (first-row result)]
      (is (= 1 (row-count result)))
      ;; [forum-id forum-title moderator-id moderator-first-name moderator-last-name]
      (is (= 1099511654688 (nth row 0)))
      (is (= "Album 23 of George Wilson" (nth row 1)))
      (is (= 4398046518478 (nth row 2)))
      (is (= "George" (nth row 3)))
      (is (= "Wilson" (nth row 4))))))

(deftest is7-test
  (testing "IS7: Replies to a message"
    (let [result (run-query-with-params is/is7 {:message-id 1099512606636})]
      ;; Based on results file, this has no replies (empty)
      (is (= 0 (row-count result))))))

;; ============================================================
;; Interactive Complex Query Tests (IC1-IC14)
;; ============================================================

(deftest ic1-test
  (testing "IC1: Transitive friends with a certain name"
    (let [result (run-query-with-params ic/ic1 {:person-id 100 :first-name "John"})]
      (is (= 20 (row-count result)))
      ;; First result: [(min ?dist) last-name person-id (pull ...)]
      (let [row (first-row result)]
        (is (= 1 (nth row 0)))  ; distance
        (is (= "Harris" (nth row 1)))  ; last-name
        (is (= 32985348837712 (nth row 2)))))))  ; person-id

(deftest ic2-test
  (testing "IC2: Recent messages by friends"
    (let [result (run-query-with-params ic/ic2 {:person-id 100
                                                 :max-date (sut/to-date "2012-07-01T00:00:00Z")})]
      (is (= 20 (row-count result)))
      ;; First result: [friend-id first-name last-name message-id content creation-date]
      (let [row (first-row result)]
        (is (= 24189255818790 (nth row 0)))  ; friend-id
        (is (= "Juan Carlos" (nth row 1)))  ; first-name
        (is (= "Cejas" (nth row 2)))  ; last-name
        (is (= 1924145711817 (nth row 3)))  ; message-id
        (is (= "fine" (nth row 4)))))))  ; content

(deftest ic3-test
  (testing "IC3: Friends in given countries"
    (let [result (run-query-with-params ic/ic3 {:person-id 100
                                                 :country-x-name "Germany"
                                                 :country-y-name "France"
                                                 :start-date (sut/to-date "2011-01-01T00:00:00Z")
                                                 :duration-days 365})]
      (is (= 3 (row-count result)))
      ;; Results: [person-id first-name last-name x-count y-count total]
      (let [row (first-row result)]
        (is (= 2199023261775 (nth row 0)))
        (is (= "Igor" (nth row 1)))
        (is (= "Gusev" (nth row 2)))
        (is (= 2 (nth row 5))))))  ; total messages
  (testing "IC3: Country names exclude other Place types"
    (let [query-forms (set (:query ic/ic3))
          result
          (run-query-with-params
            ic/ic3 {:person-id 66
                    :country-x-name "Australia"
                    :country-y-name "New_Zealand"
                    :start-date (sut/to-date "2011-01-01T00:00:00Z")
                    :duration-days 365})]
      (is (contains? query-forms '[?country-x :place/type "Country"]))
      (is (contains? query-forms '[?country-y :place/type "Country"]))
      (is (= [[2199023260487 "Bing" "Yang" 1 1 2]
              [4398046519949 "Arjun" "Khan" 1 1 2]
              [6597069774986 "Joe" "Fox" 1 1 2]
              [15393162798303 "Sinan" "Simsek" 1 1 2]
              [19791209306158 "Bobby" "Garcia" 1 1 2]]
             (:rows result))))))

(deftest ic4-test
  (testing "IC4: New topics"
    (let [result (run-query-with-params ic/ic4 {:person-id 100
                                                 :start-date (sut/to-date "2011-01-01T00:00:00Z")
                                                 :duration-days 365})]
      (is (= 10 (row-count result)))
      ;; Results: [tag-name count]
      (let [row (first-row result)]
        (is (= "Robert_Fripp" (nth row 0)))
        (is (= 139 (nth row 1)))))))

(deftest ic5-test
  (testing "IC5: New groups"
    (let [result (run-query-with-params ic/ic5 {:person-id 100
                                                 :min-date (sut/to-date "2011-01-01T00:00:00Z")})]
      (is (= 20 (row-count result)))
      ;; Results: [forum-id forum-title count]
      (let [row (first-row result)]
        (is (= 1374389605568 (nth row 0)))
        (is (= "Group for Benjamin_Britten in Amritsar" (nth row 1)))
        (is (= 83 (nth row 2)))))))

(deftest ic6-test
  (testing "IC6: Tag co-occurrence"
    (let [result (run-query-with-params ic/ic6 {:person-id 100
                                                 :tag-name "Mozart"})]
      ;; Based on results file, this has no results for this tag
      (is (= 0 (row-count result))))))

(deftest ic7-test
  (testing "IC7: Recent likers"
    (let [result (run-query-with-params ic/ic7 {:person-id 100})]
      (is (= 20 (row-count result)))
      ;; Results: [liker-id first-name last-name like-date message-id message-content minutes-latency is-new]
      (let [row (first-row result)]
        (is (= 7138 (nth row 0)))  ; liker-id
        (is (= "Shweta" (nth row 1)))
        (is (= "Chopra" (nth row 2)))
        (is (= 2336466712173 (nth row 4)))  ; message-id
        (is (= 10027 (nth row 6)))  ; minutes-latency
        (is (= true (nth row 7)))))))  ; is-new

(deftest ic8-test
  (testing "IC8: Recent replies"
    (let [result (run-query-with-params ic/ic8 {:person-id 100})]
      (is (= 20 (row-count result)))
      ;; Results: [replier-id first-name last-name comment-date comment-id content]
      (let [row (first-row result)]
        (is (= 15393162798954 (nth row 0)))  ; replier-id
        (is (= "Paul" (nth row 1)))
        (is (= "Brown" (nth row 2)))
        (is (= 2336466712193 (nth row 4)))  ; comment-id
        (is (= "good" (nth row 5)))))))

(deftest ic9-test
  (testing "IC9: Recent messages by friends or friends of friends"
    (let [result (run-query-with-params ic/ic9 {:person-id 100
                                                 :max-date (sut/to-date "2012-07-01T00:00:00Z")})]
      (is (= 20 (row-count result)))
      ;; Results: [creator-id first-name last-name message-id content creation-date]
      (let [row (first-row result)]
        (is (= 26388279068136 (nth row 0)))  ; creator-id
        (is (= "Koji" (nth row 1)))
        (is (= "Kato" (nth row 2)))
        (is (= 1924145867898 (nth row 3)))  ; message-id
        (is (= "thanks" (nth row 4)))))))

(deftest ic10-test
  (testing "IC10: Friend recommendation"
    (let [result (run-query-with-params ic/ic10 {:person-id 100 :month 5})]
      (is (= 10 (row-count result)))
      ;; Results: [person-id first-name last-name score gender city-name]
      (let [row (first-row result)]
        (is (= 4398046512397 (nth row 0)))  ; person-id
        (is (= "Arjun" (nth row 1)))
        (is (= "Kumar" (nth row 2)))
        (is (= 0 (nth row 3)))  ; score
        (is (= "female" (nth row 4)))
        (is (= "Chennai" (nth row 5)))))))

(deftest ic11-test
  (testing "IC11: Job referral"
    (let [result (run-query-with-params ic/ic11 {:person-id 100
                                                  :country-name "Germany"
                                                  :work-from-year 2010})]
      (is (= 10 (row-count result)))
      ;; Results: [person-id first-name last-name company-name work-from]
      (let [row (first-row result)]
        (is (= 32985348840809 (nth row 0)))  ; person-id
        (is (= "Werner" (nth row 1)))
        (is (= "Fischer" (nth row 2)))
        (is (= "Aero_Business_Charter" (nth row 3)))
        (is (= 2000 (nth row 4)))))))

(deftest ic12-test
  (testing "IC12: Expert search"
    (let [result (run-query-with-params ic/ic12 {:person-id 100
                                                  :tag-class-name "MusicalArtist"})]
      (is (= 20 (row-count result)))
      ;; Results: [person-id first-name last-name (distinct tag-names) (count comments)]
      (let [row (first-row result)]
        (is (= 19791209303398 (nth row 0)))  ; person-id
        (is (= "Naresh" (nth row 1)))
        (is (= "Kumar" (nth row 2)))
        (is (= 412 (nth row 4)))))))  ; comment count

(deftest ic13-test
  (testing "IC13: Single shortest path"
    (let [result (run-query-with-params ic/ic13 {:person1-id 100
                                                  :person2-id 6597069770569})]
      (is (= 1 (row-count result)))
      ;; Results: [(min ?dist)]
      (let [row (first-row result)]
        (is (= 3 (nth row 0)))))))  ; shortest path length

(deftest ic14-test
  (testing "IC14: Trusted connection paths"
    (let [result (run-query-with-params ic/ic14 {:person1-id 100
                                                  :person2-id 6597069770569})]
      (is (< 0 (row-count result)))
      ;; Results: [p1-id m1-id m2-id p2-id count weight]
      (let [row (first-row result)]
        (is (= 100 (nth row 0)))  ; p1-id
        (is (= 19791209303398 (nth row 1)))  ; m1-id
        (is (= 4398046519825 (nth row 2)))  ; m2-id
        (is (= 6597069770569 (nth row 3)))  ; p2-id
        (is (= 29 (nth row 4)))  ; interaction count
        (is (= 12.75 (nth row 5)))))))  ; half-weight

;; ============================================================
;; Run all tests convenience function
;; ============================================================

(defn run-all-tests []
  (t/run-tests 'ldbc-snb-bench.core-test))
