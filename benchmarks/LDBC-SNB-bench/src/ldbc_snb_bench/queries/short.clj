(ns ldbc-snb-bench.queries.short
  "LDBC SNB Interactive Short Queries (IS1-IS7) implemented per specification at
  https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf"
  (:require [ldbc-snb-bench.queries.common :as common]))

(def is1
  {:name        "IS1"
   :description "Profile of a Person: Given a start Person with ID, retrieve their first name, last name, birthday, IP address, browser, city of residence, gender and creationDate."
   :params      [:person-id]
   :query       '[:find ?first-name ?last-name ?birthday ?location-ip ?browser-used
                  ?city-id ?gender ?creation-date
                  :in $ ?person-id
                  :where
                  [?p :person/id ?person-id]
                  [?p :person/firstName ?first-name]
                  [?p :person/lastName ?last-name]
                  [?p :person/birthday ?birthday]
                  [?p :person/locationIP ?location-ip]
                  [?p :person/browserUsed ?browser-used]
                  [?p :person/gender ?gender]
                  [?p :person/creationDate ?creation-date]
                  [?p :person/isLocatedIn ?city]
                  [?city :place/id ?city-id]]})

(def is2
  {:name        "IS2"
   :description "Recent messages of a Person: Given a start Person with ID, retrieve the last 10 Messages created by that user. For each Message, return that Message, the original Post in its conversation (post), and the author of that Post (originalPoster). If any of the Messages is a Post, then the original Post (post) will be the same Message, i.e. that Message will appear twice in that result."
   :params      [:person-id]
   :rules       common/rule-root-post
   :query       '[:find ?message-id ?message-content ?message-date
                  ?post-id ?original-id ?original-first ?original-last
                  :in $ % ?person-id
                  :where
                  [?p :person/id ?person-id]
                  [?message :message/hasCreator ?p]
                  [?message :message/id ?message-id]
                  (or-join [?message ?message-content]
                           [?message :message/content ?message-content]
                           (and (not [?message :message/content _])
                                [?message :message/imageFile ?message-content]))
                  [?message :message/creationDate ?message-date]
                  (root-post ?message ?post)
                  [?post :message/id ?post-id]
                  [?post :message/hasCreator ?original]
                  [?original :person/id ?original-id]
                  [?original :person/firstName ?original-first]
                  [?original :person/lastName ?original-last]
                  :order-by [?message-date :desc ?message-id :desc]
                  :limit 10]})

(def is3
  {:name        "IS3"
   :description "Friends of a Person: Given a start Person with ID, retrieve all of their friends, and the date at which they became friends."
   :params      [:person-id]
   :query       '[:find ?friend-id ?first-name ?last-name ?creation-date
                  :in $ ?person-id
                  :where
                  [?p :person/id ?person-id]
                  [?k :knows/person1 ?p]
                  [?k :knows/person2 ?friend]
                  [?k :knows/creationDate ?creation-date]
                  [?friend :person/id ?friend-id]
                  [?friend :person/firstName ?first-name]
                  [?friend :person/lastName ?last-name]
                  :order-by [?creation-date :desc ?friend-id :asc]]})

(def is4
  {:name        "IS4"
   :description "Content of a message: Given a Message with ID, retrieve its content and creation date."
   :params      [:message-id]
   :query       '[:find ?creation-date ?content
                  :in $ ?message-id
                  :where
                  [?message :message/id ?message-id]
                  [?message :message/creationDate ?creation-date]
                  (or-join [?message ?content]
                           [?message :message/content ?content]
                           (and (not [?message :message/content _])
                                [?message :message/imageFile ?content]))]})

(def is5
  {:name        "IS5"
   :description "Creator of a message: Given a Message with ID, retrieve its author"
   :params      [:message-id]
   :query       '[:find ?person-id ?first-name ?last-name
                  :in $ ?message-id
                  :where
                  [?message :message/id ?message-id]
                  [?message :message/hasCreator ?creator]
                  [?creator :person/id ?person-id]
                  [?creator :person/firstName ?first-name]
                  [?creator :person/lastName ?last-name]]})

(def is6
  {:name        "IS6"
   :description "Forum of a message: Given a Message with ID, retrieve the Forum that contains it and the Person that moderates that Forum. Since Comments are not directly contained in Forums, for Comments, return the Forum containing the original Post in the thread which the Comment is replying to."
   :params      [:message-id]
   :rules       common/rule-root-post
   :query       '[:find ?forum-id ?forum-title ?moderator-id
                    ?moderator-first-name ?moderator-last-name
                    :in $ % ?message-id
                    :where
                    [?message :message/id ?message-id]
                    (root-post ?message ?post)
                    [?forum :forum/containerOf ?post]
                    [?forum :forum/id ?forum-id]
                  [?forum :forum/title ?forum-title]
                  [?forum :forum/hasModerator ?moderator]
                  [?moderator :person/id ?moderator-id]
                  [?moderator :person/firstName ?moderator-first-name]
                  [?moderator :person/lastName ?moderator-last-name]]})

(def is7
  {:name        "IS7"
   :description "Replies to a message: Given a Message with ID, retrieve the (1-hop) Comments that reply to it. In addition, return a boolean flag knows indicating if the author of the reply (replyAuthor) knows the author of the original message (messageAuthor). If author is same as original author, return False for knows flag."
   :params      [:message-id]
   :query       '[:find ?comment-id ?content ?creation-date
                  ?author-id ?author-first-name ?author-last-name ?knows
                  :in $ ?message-id
                  :where
                  [?message :message/id ?message-id]
                  [?message :message/hasCreator ?message-author]
                  [?comment :message/replyOf ?message]
                  [?comment :message/id ?comment-id]
                  [?comment :message/content ?content]
                  [?comment :message/creationDate ?creation-date]
                  [?comment :message/hasCreator ?author]
                  [?author :person/id ?author-id]
                  [?author :person/firstName ?author-first-name]
                  [?author :person/lastName ?author-last-name]
                  (or-join [?author ?message-author ?knows]
                           (and [(not= ?author ?message-author)]
                                [?k :knows/person1 ?message-author]
                                [?k :knows/person2 ?author]
                                [(ground true) ?knows])
                           (and (not-join [?author ?message-author]
                                          [?k :knows/person1 ?message-author]
                                          [?k :knows/person2 ?author])
                                [(ground false) ?knows]))
                  :order-by [?creation-date :desc ?author-id :asc]]})

(def all-queries
  "All Interactive Short queries"
  [is1 is2 is3 is4 is5 is6 is7])

(defn get-query
  "Get a query by name (e.g., \"IS1\")"
  [name]
  (first (filter #(= (:name %) name) all-queries)))
