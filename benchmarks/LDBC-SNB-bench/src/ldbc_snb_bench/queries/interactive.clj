(ns ldbc-snb-bench.queries.interactive
  "LDBC SNB Interactive Complex Queries (IC1-IC14) implemented per specification at
  https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf"
  (:require
   [ldbc-snb-bench.queries.common :as common]))

(def ic1
  {:name        "IC1"
   :description "Transitive friends with a certain name: Given a start Person with ID, find Persons with a given first name that the start Person is connected to (excluding start Person) by at most 3 steps via the knows relationships. Return Persons, including the distance (1..3), summaries of the Persons workplaces and places of study."
   :params      [:person-id :first-name]
   :rules       common/rule-friends-3
   :query       '[:find (min ?dist) ?last-name ?person-id
                  (pull ?friend
                        [:person/birthday
                         :person/creationDate
                         :person/gender
                         :person/browserUsed
                         :person/locationIP
                         :person/email
                         :person/speaks
                         {:person/isLocatedIn [:place/name]}
                         ;; person school
                         {:studyAt/_person
                          [:studyAt/classYear
                           {:studyAt/organization
                            [:organization/name
                             {:organization/isLocatedIn [:place/name]}]}]}
                         ;; person work
                         {:workAt/_person
                          [:workAt/workFrom
                           {:workAt/organization
                            [:organization/name
                             {:organization/isLocatedIn [:place/name]}]}]}])
                  :in $ % ?start-person-id ?first-name
                  :where
                  [?start :person/id ?start-person-id]
                  (friends-3 ?start ?friend ?dist)
                  [?friend :person/firstName ?first-name]
                  [?friend :person/lastName ?last-name]
                  [?friend :person/id ?person-id]
                  :order-by [?dist :asc ?last-name :asc ?person-id :asc]
                  :limit 20]})

(def ic2
  {:name        "IC2"
   :description "Recent messages by friends: Given a start Person with ID, find the most recent Messages from all of that Person’s friends (friend nodes). Only consider Messages created before the given maxDate (excluding that day)."
   :params      [:person-id :max-date]
   :query       '[:find ?friend-id ?first-name ?last-name ?message-id ?content
                  ?creation-date
                  :in $ ?person-id ?max-date
                  :where
                  ;; friends
                  [?start :person/id ?person-id]
                  [?k :knows/person1 ?start]
                  [?k :knows/person2 ?friend]
                  [?friend :person/id ?friend-id]
                  [?friend :person/firstName ?first-name]
                  [?friend :person/lastName ?last-name]

                  ;; messages
                  [?message :message/hasCreator ?friend]
                  [?message :message/id ?message-id]
                  [(get-some $ ?message
                             :message/content
                             :message/imageFile) [_ ?content]]
                  [?message :message/creationDate ?creation-date]
                  [(< ?creation-date ?max-date)]
                  :order-by [?creation-date :desc ?message-id :asc]
                  :limit 20]})

(def ic3
  {:name        "IC3"
   :description "Friends and friends of friends in given countries: Given a start Person with ID, find Persons that are their friends and friends of friends (excluding the start Person) that have made Posts / Comments in both of the given Countries (named countryXName and countryYName), within [startDate, startDate + durationDays) (closedopen interval). Only Persons that are foreign to these Countries are considered, that is Persons whose location Country is neither named countryXName nor countryYName."
   :params      [:person-id :country-x-name :country-y-name
                 :start-date :end-date]
   :param-fn    (fn [params]
                  (if (:end-date params)
                    params
                    (assoc params :end-date
                           (common/add-days
                             (:start-date params)
                             (:duration-days params)))))
   :query       '[:find ?person ?first-name ?last-name (sum ?x-inc) (sum ?y-inc)
                  (+ (sum ?x-inc) (sum ?y-inc))
                  :in $ ?person-id ?country-x-name ?country-y-name
                  ?start-date ?end-date
                  :where
                  ;; Resolve country entities once for ID comparisons
                  [?country-x :place/name ?country-x-name]
                  [?country-y :place/name ?country-y-name]

                  ;; Friends within 2 hops
                  [?start :person/id ?person-id]
                  (or-join [?start ?person]
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?person])
                           (and [?k1 :knows/person1 ?start]
                                [?k1 :knows/person2 ?mid]
                                [?k2 :knows/person1 ?mid]
                                [?k2 :knows/person2 ?person]
                                [(not= ?person ?start)]))

                  [?person :person/firstName ?first-name]
                  [?person :person/lastName ?last-name]

                  ;; Person must be foreign (city -> country direct)
                  [?person :person/isLocatedIn ?city]
                  [?city :place/isPartOf ?person-country]
                  [(not= ?person-country ?country-x)]
                  [(not= ?person-country ?country-y)]

                  ;; Messages in country X or Y
                  [?message :message/hasCreator ?person]
                  [?message :message/isLocatedIn ?loc]
                  (or-join [?loc ?country-x ?country-y
                            ?x-inc ?y-inc]
                           (and [(= ?loc ?country-x)]
                                [(ground 1) ?x-inc]
                                [(ground 0) ?y-inc])
                           (and [(= ?loc ?country-y)]
                                [(ground 0) ?x-inc]
                                [(ground 1) ?y-inc]))

                  ;; Message date range
                  [?message :message/creationDate ?date]
                  [(<= ?start-date ?date)]
                  [(< ?date ?end-date)]
                  :having
                  [(pos? (sum ?x-inc))]   ; Must have messages in country X
                  [(pos? (sum ?y-inc))]   ; Must have messages in country Y
                  :order-by [5 :desc 0 :asc]
                  :limit 20]})

(def ic4
  {:name        "IC4"
   :description "New topics: Given a start Person with ID, find Tags that are attached to Posts that were created by that Person's friends. Only include Tags that were attached to friends' Posts created within a given time interval [startDate, startDate + durationDays) (closed-open) and that were never attached to friends' Posts created before this interval."
   :params      [:person-id :start-date :duration-days]
   :query       '[:find ?tag-name (count ?post)
                  :in $ ?person-id ?start-date ?duration-days
                  :where
                  ;; Friends
                  [?start :person/id ?person-id]
                  [?k :knows/person1 ?start]
                  [?k :knows/person2 ?friend]

                    ;; messages created by friends in date range
                    [?post :message/hasCreator ?friend]
                    [?post :message/isContainedIn _]
                  [(ldbc-snb-bench.queries.common/add-days
                     ?start-date ?duration-days) ?end-date]
                  [?post :message/creationDate ?date]
                  [(<= ?start-date ?date)]
                  [(< ?date ?end-date)]
                  [?post :message/hasTag ?tag]
                  [?tag :tag/name ?tag-name]

                  ;; Exclude tags that ANY friend used before start-date
                  (not-join [?tag-name]
                            [?inner-start :person/id ?person-id]
                              [?k2 :knows/person1 ?inner-start]
                              [?k2 :knows/person2 ?any-friend]
                              [?other-post :message/hasCreator ?any-friend]
                              [?other-post :message/isContainedIn _]
                            [?other-post :message/creationDate ?d]
                            [(< ?d ?start-date)]
                            [?other-post :message/hasTag ?t]
                            [?t :tag/name ?tag-name])
                  :order-by [1 :desc 0 :asc]
                  :limit 10]})

(def ic5
  {:name        "IC5"
   :description "New groups: Given a start Person with ID, denote their friends and friends of friends (excluding the start Person) as otherPerson. Find Forums that any otherPerson became a member of after a given date (minDate). For each of those Forums, count the number of Posts that were created by the otherPerson."
   :params      [:person-id :min-date]
   :query       '[:find ?forum-id ?forum-title (count-distinct ?post)
                  :in $ ?person-id ?min-date
                  :where
                  ;; Friends within 2 hops
                  [?start :person/id ?person-id]
                  (or-join [?start ?person]
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?person])
                           (and [?k1 :knows/person1 ?start]
                                [?k1 :knows/person2 ?mid]
                                [?k2 :knows/person1 ?mid]
                                [?k2 :knows/person2 ?person]
                                [(not= ?person ?start)]))

                  ;; Forums joined after min-date
                  [?membership :hasMember/person ?person]
                  [?membership :hasMember/forum ?forum]
                  [?membership :hasMember/joinDate ?join-date]
                  [(> ?join-date ?min-date)]

                    ;; Posts created in forums
                    [?post :message/hasCreator ?person]
                    [?post :message/isContainedIn ?forum]
                  [?forum :forum/id ?forum-id]
                  [?forum :forum/title ?forum-title]
                  :order-by [2 :desc 0 :asc]
                  :limit 20]})

(def ic6
  {:name        "IC6"
   :description "Tag co-occurrence: Given a start Person with ID and a Tag with name tagName, find the other Tags that occur together with this Tag on Posts that were created by start Person’s friends and friends of friends (excluding start Person). Return top 10 Tags, and the count of Posts that were created by these Persons, which contain both this Tag and the given Tag."
   :params      [:person-id :tag-name]
   :query       '[:find ?other-tag-name (count ?post)
                  :in $ ?person-id ?tag-name
                  :where
                  ;; Friends within 2 hops
                  [?start :person/id ?person-id]
                  (or-join [?start ?person]
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?person])
                           (and [?k1 :knows/person1 ?start]
                                [?k1 :knows/person2 ?mid]
                                [?k2 :knows/person1 ?mid]
                                [?k2 :knows/person2 ?person]
                                [(not= ?person ?start)]))

                  ;; Other tags created by friends together with tag-name
                    [?tag :tag/name ?tag-name]
                    [?post :message/hasTag ?tag]
                    [?post :message/hasCreator ?person]
                    [?post :message/isContainedIn _]
                  [?post :message/hasTag ?other-tag]
                  [?other-tag :tag/name ?other-tag-name]
                  [(not= ?tag ?other-tag)]
                  :order-by [1 :desc 0 :asc]
                  :limit 10]})

(def ic7
  {:name        "IC7"
   :description "Recent likers: Given a start Person with ID, find the most recent likes on any of start Person's Messages. Find Persons that liked (likes edge) any of start Person's Messages, the Messages they liked most recently, the creation date of that like, and the latency in minutes (minutesLatency) between creation of Messages and like. Additionally, for each Person found return a flag indicating (isNew) whether the liker is a friend of start Person. In case that a Person liked multiple Messages at the same time, return the Message with lowest identifier."
   :params      [:person-id]
   :query       '[:find ?liker-id ?first-name ?last-name ?like-date
                  ?message-id ?message-content ?minutes-latency ?is-new
                  :in $ ?person-id
                  :where
                  ;; Start person
                  [?start :person/id ?person-id]

                  ;; Messages created by start person
                  [?message :message/hasCreator ?start]
                  [?message :message/id ?message-id]
                  [?message :message/creationDate ?message-date]
                  [(get-some $ ?message
                             :message/content
                             :message/imageFile) [_ ?message-content]]

                  ;; Likes on those messages
                  [?like :likes/message ?message]
                  [?like :likes/person ?liker]
                  [?like :likes/creationDate ?like-date]

                  ;; Liker info
                  [?liker :person/id ?liker-id]
                  [?liker :person/firstName ?first-name]
                  [?liker :person/lastName ?last-name]

                  ;; This must be the most recent like from this liker
                  ;; (no other like with later date, or same date + smaller message-id)
                  (not-join [?liker ?like-date ?message-id ?start]
                            [?other-msg :message/hasCreator ?start]
                            [?other-msg :message/id ?other-msg-id]
                            [?other-like :likes/message ?other-msg]
                            [?other-like :likes/person ?liker]
                            [?other-like :likes/creationDate ?other-like-date]
                            (or-join [?like-date ?other-like-date ?message-id ?other-msg-id]
                                     [(> ?other-like-date ?like-date)]
                                     (and [(= ?other-like-date ?like-date)]
                                          [(< ?other-msg-id ?message-id)])))

                  ;; Compute minutes latency
                  [(ldbc-snb-bench.queries.common/minutes-between
                    ?message-date ?like-date) ?minutes-latency]

                  ;; isNew flag: true if liker is not start and not a direct friend
                  (or-join [?start ?liker ?is-new]
                           ;; Is a friend -> false
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?liker]
                                [(ground false) ?is-new])
                           ;; Is start person -> false
                           (and [(= ?liker ?start)]
                                [(ground false) ?is-new])
                           ;; Not a friend and not start -> true
                           (and [(not= ?liker ?start)]
                                (not-join [?start ?liker]
                                          [?kf :knows/person1 ?start]
                                          [?kf :knows/person2 ?liker])
                                [(ground true) ?is-new]))

                  :order-by [?like-date :desc ?liker-id :asc]
                  :limit 20]})

(def ic8
  {:name        "IC8"
   :description "Recent replies: Given a start Person with ID, find the most recent Comments that are replies to Messages of the start Person. Only consider direct (single-hop) replies, not the transitive (multi-hop) ones. Return the reply Comments, and the Person that created each reply Comment."
   :params      [:person-id]
   :query       '[:find ?replier-id ?first-name ?last-name ?comment-date
                  ?comment-id ?content
                  :in $ ?person-id
                  :where
                  [?start :person/id ?person-id]
                  [?message :message/hasCreator ?start]
                  [?comment :message/replyOf ?message]
                  [?comment :message/hasCreator ?replier]
                  [?replier :person/id ?replier-id]
                  [?replier :person/firstName ?first-name]
                  [?replier :person/lastName ?last-name]
                  [?comment :message/creationDate ?comment-date]
                  [?comment :message/id ?comment-id]
                  [?comment :message/content ?content]
                  :order-by [?comment-date :desc ?comment-id :asc]
                  :limit 20]})

(def ic9
  {:name        "IC9"
   :description "Recent messages by friends or friends of friends: Given a start Person with ID, find the most recent Messages created by that Person’s friends or friends of friends (excluding the start Person). Only consider Messages created before the given maxDate (excluding that day)."
   :params      [:person-id :max-date]
   :query       '[:find ?creator-id ?first-name ?last-name ?message-id ?content
                  ?creation-date
                  :in $ ?person-id ?max-date
                  :where
                  [?start :person/id ?person-id]
                  ;; Friends within 2 hops (excluding start)
                  (or-join [?start ?person]
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?person])
                           (and [?k1 :knows/person1 ?start]
                                [?k1 :knows/person2 ?mid]
                                [?k2 :knows/person1 ?mid]
                                [?k2 :knows/person2 ?person]
                                [(not= ?person ?start)]))

                  ;; Messages by those people
                  [?message :message/hasCreator ?person]
                  [?message :message/id ?message-id]
                  (or-join [?message ?content]
                           [?message :message/content ?content]
                           (and (not [?message :message/content _])
                                [?message :message/imageFile ?content]))
                  [?message :message/creationDate ?creation-date]
                  [(< ?creation-date ?max-date)]

                  ;; Person info
                  [?person :person/id ?creator-id]
                  [?person :person/firstName ?first-name]
                  [?person :person/lastName ?last-name]

                  :order-by [?creation-date :desc ?message-id :asc]
                  :limit 20]})

(def ic10
  {:name        "IC10"
   :description "Friend recommendation: Find friends-of-friends (exactly distance 2, not direct friends) with birthday in given month window. Score = 2*commonPosts - totalPosts where commonPosts have tags matching start person's interests."
   :params      [:person-id :month]
   :query
   '[:find ?person-id ?first-name ?last-name
     (sum ?score-contrib)
     ?gender ?city-name
     ;; Note: keep ?post in :with so sum aggregates per post, not per score value.
     :with ?post
     :in $ ?start-person-id ?month
     :where
     ;; Start person
     [?start :person/id ?start-person-id]

     ;; Friends of friends only (distance exactly 2, not distance 1)
     (or-join [?start ?person]
              (and [?k1 :knows/person1 ?start]
                   [?k1 :knows/person2 ?mid]
                   [?k2 :knows/person1 ?mid]
                   [?k2 :knows/person2 ?person]
                   [(not= ?person ?start)]
                   (not-join [?start ?person]
                             [?k :knows/person1 ?start]
                             [?k :knows/person2 ?person])))

     ;; Birthday in window
     [?person :person/birthday ?birthday]
     [(ldbc-snb-bench.queries.common/birthday-in-window? ?birthday ?month)]

     ;; Person info
     [?person :person/id ?person-id]
     [?person :person/firstName ?first-name]
     [?person :person/lastName ?last-name]
     [?person :person/gender ?gender]
     [?person :person/isLocatedIn ?city]
     [?city :place/name ?city-name]

     ;; Score computation:
     ;; - Post with matching tag: +1 (counts once per post, even with multiple tags)
     ;; - Post without matching tag: -1
     ;; - No posts: 0
     (or-join [?person ?start ?post ?score-contrib]
                ;; Post with matching tag: +1 (dedupe multiple matching tags per post)
                (and [?post :message/hasCreator ?person]
                     [?post :message/isContainedIn _]
                     [?post :message/hasTag ?tag]
                     [?start :person/hasInterest ?tag]
                   (not-join [?post ?start ?tag]
                             [?post :message/hasTag ?tag2]
                             [?start :person/hasInterest ?tag2]
                             [(< ?tag2 ?tag)])
                   [(ground 1) ?score-contrib])
                ;; Post without matching tag: -1
                (and [?post :message/hasCreator ?person]
                     [?post :message/isContainedIn _]
                     (not-join [?post ?start]
                             [?post :message/hasTag ?t]
                             [?start :person/hasInterest ?t])
                   [(ground -1) ?score-contrib])
                ;; Person with no posts: 0
                (and (not-join [?person]
                               [?p :message/hasCreator ?person]
                               [?p :message/isContainedIn _])
                     [(ground :no-post) ?post]
                   [(ground 0) ?score-contrib]))

     :order-by [3 :desc ?person-id :asc]
     :limit 10]})

(def ic11
  {:name        "IC11"
   :description "Job referral: Given a start Person with ID, find that Person’s friends and friends of friends (excluding start Person) who started working in some Company in a given Country with name countryName, before a given date (workFromYear)."
   :params      [:person-id :country-name :work-from-year]
   :rules       common/rule-place-country
   :query       '[:find ?person-id ?first-name ?last-name ?company-name ?work-from
                  :in $ % ?person-id-in ?country-name ?work-from-year
                  :where
                  ;; Friends within 2 hops
                  [?start :person/id ?person-id-in]
                  (or-join [?start ?person]
                           (and [?k :knows/person1 ?start]
                                [?k :knows/person2 ?person])
                           (and [?k1 :knows/person1 ?start]
                                [?k1 :knows/person2 ?mid]
                                [?k2 :knows/person1 ?mid]
                                [?k2 :knows/person2 ?person]
                                [(not= ?person ?start)]))

                  ;; Work relationship
                  [?work :workAt/person ?person]
                  [?work :workAt/organization ?company]
                  [?work :workAt/workFrom ?work-from]
                  [(< ?work-from ?work-from-year)]

                  ;; Company in specified country
                  [?company :organization/name ?company-name]
                  [?company :organization/isLocatedIn ?place]
                  (place-country ?place ?country)
                  [?country :place/name ?country-name]

                  ;; Person info
                  [?person :person/id ?person-id]
                  [?person :person/firstName ?first-name]
                  [?person :person/lastName ?last-name]

                  :order-by [?work-from :asc ?person-id :asc ?company-name :desc]
                  :limit 10]})

(def ic12
  {:name        "IC12"
   :description "Expert search: Given a start Person with ID $personId, find the Comments that this Person’s friends made in reply to Posts, considering only those Comments that are direct (single-hop) replies to Posts, not the transitive (multi-hop) ones. Only consider Posts with a Tag in a given TagClass with name $tagClassName or in a descendent of that TagClass. Count the number of these reply Comments, and collect the Tags that were attached to the Posts they replied to, but only collect Tags with the given TagClass or with a descendant of that TagClass. Return Persons with at least one reply, the reply count, and the collection of Tags."
   :params      [:person-id :tag-class-name]
   :rules       common/rule-tagclass-descendant
   :query       '[:find ?person-id ?first-name ?last-name
                  (distinct ?tag-name) (count-distinct ?comment)
                  :in $ % ?person-id-in ?tag-class-name
                  :where
                  ;; Direct friends
                  [?start :person/id ?person-id-in]
                  [?k :knows/person1 ?start]
                  [?k :knows/person2 ?friend]

                  ;; Tag class and descendants
                  [?root :tagclass/name ?tag-class-name]
                  (tagclass-descendant ?root ?tagclass)

                  ;; Tags in those classes
                  [?tag :tag/hasType ?tagclass]
                  [?tag :tag/name ?tag-name]

                    ;; Comments by friends replying to posts with those tags
                    [?comment :message/hasCreator ?friend]
                    [?comment :message/replyOf ?post]
                    [?post :message/isContainedIn _]
                  [?post :message/hasTag ?tag]

                  ;; Friend info
                  [?friend :person/id ?person-id]
                  [?friend :person/firstName ?first-name]
                  [?friend :person/lastName ?last-name]

                  :order-by [4 :desc ?person-id :asc]
                  :limit 20]})

(def ic13
  {:name        "IC13"
   :description "Single shortest path: Given two Persons with IDs $person1Id and $person2Id, find the shortest path between these two Persons in the subgraph induced by the knows edges. Return the length of this path:
• −1: no path found • 0: start person = end person
• > 0: path found (start person ≠ end person)"
   :params      [:person1-id :person2-id]
   :rules       common/rule-friends-3
   :query       '[:find (min ?dist)
                  :in $ % ?person1-id ?person2-id
                  :where
                  [?p1 :person/id ?person1-id]
                  [?p2 :person/id ?person2-id]
                  (or-join [?p1 ?p2 ?dist]
                           ;; Same person -> distance 0
                           (and [(= ?p1 ?p2)]
                                [(ground 0) ?dist])
                           ;; Find path within 3 hops using friends-3 rule
                           ;; Note: use unbound ?friend then filter, as bound target doesn't work
                           (and [(not= ?p1 ?p2)]
                                (friends-3 ?p1 ?friend ?dist)
                                [(= ?friend ?p2)]))]})

(def ic14
  {:name        "IC14"
   :description "Trusted connection paths: Find all shortest paths between two persons and calculate trust weight based on interaction frequency. Returns paths as [p1-id, m1-id, m2-id, p2-id], interaction count, and half-weight. Actual weight = 2 * half-weight."
   :params      [:person1-id :person2-id]
   :rules       common/rule-interaction-half-weight
   :query       '[:find ?p1-id ?m1-id ?m2-id ?p2-id
                  (count-distinct ?interaction) (sum ?half-weight)
                  :in $ % ?person1-id ?person2-id
                  :where
                  ;; Find start and end persons
                  [?p1 :person/id ?person1-id]
                  [?p2 :person/id ?person2-id]

                  ;; 3-hop path: p1 -> m1 -> m2 -> p2
                  [?k1 :knows/person1 ?p1]
                  [?k1 :knows/person2 ?m1]
                  [?k2 :knows/person1 ?m1]
                  [?k2 :knows/person2 ?m2]
                  [(not= ?m2 ?p1)]
                  [?k3 :knows/person1 ?m2]
                  [?k3 :knows/person2 ?p2]
                  [(not= ?p2 ?m1)]

                  ;; Person IDs for output
                  [?p1 :person/id ?p1-id]
                  [?m1 :person/id ?m1-id]
                  [?m2 :person/id ?m2-id]
                  [?p2 :person/id ?p2-id]

                  ;; Count all interactions with half-weights
                  ;; Post replies have half-weight 0.5 (actual weight 1.0)
                  ;; Comment replies have half-weight 0.25 (actual weight 0.5)
                  ;; Actual weight = 2 * sum(half-weight)
                  (or-join [?p1 ?m1 ?m2 ?p2 ?interaction ?half-weight]
                           (interaction-half-weight ?p1 ?m1
                                                     ?interaction ?half-weight)
                           (interaction-half-weight ?m1 ?m2
                                                     ?interaction ?half-weight)
                           (interaction-half-weight ?m2 ?p2
                                                     ?interaction ?half-weight))

                  :order-by [5 :desc]]})

;; ============================================================
;; Query collection
;; ============================================================

(def all-queries
  "All Interactive Complex queries"
  [ic1 ic2 ic3 ic4 ic5 ic6 ic7 ic8 ic9 ic10 ic11 ic12 ic13 ic14])

(defn get-query
  "Get a query by name (e.g., \"IC1\")"
  [name]
  (first (filter #(= (:name %) name) all-queries)))
