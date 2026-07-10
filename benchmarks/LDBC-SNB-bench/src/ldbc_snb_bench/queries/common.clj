(ns ldbc-snb-bench.queries.common
  "Shared helpers and rules for LDBC SNB queries."
  (:require [datalevin.core :as d])
  (:import [java.time Duration ZoneOffset ZonedDateTime]
           [java.time.temporal ChronoUnit]
           [java.util Date]))

;; ============================================================
;; Date Functions
;; ============================================================

(defn add-days
  "Return a new Date that is `days` after `start`."
  [^Date start days]
  (Date/from (.plus (.toInstant start) (Duration/ofDays (long days)))))

(defn month-of
  "Return the 1-based month number of the Date in UTC."
  [^Date date]
  (.getMonthValue (ZonedDateTime/ofInstant (.toInstant date) ZoneOffset/UTC)))

(defn day-of
  "Return the day-of-month number of the Date in UTC."
  [^Date date]
  (.getDayOfMonth (ZonedDateTime/ofInstant (.toInstant date) ZoneOffset/UTC)))

(defn birthday-in-window?
  "Return true if the birthday is in [month-21, nextMonth-22)."
  [^Date birthday month]
  (let [m (month-of birthday)
        d (day-of birthday)
        next-month (if (= 12 month) 1 (inc month))]
    (or (and (= m month) (>= d 21))
        (and (= m next-month) (< d 22)))))

(defn minutes-between
  "Return the whole-minute difference between two instants."
  [^Date earlier ^Date later]
  (long (.between ChronoUnit/MINUTES (.toInstant earlier) (.toInstant later))))

;; ============================================================
;; Rules
;; ============================================================

(def rule-root-post
  '[[(root-post ?m ?post)
     [_ :forum/containerOf ?m]
     [(ground ?m) ?post]]
    [(root-post ?m ?post)
     [?m :message/replyOf ?parent]
     (root-post ?parent ?post)]])

(def rule-place-country
  '[[(place-country ?place ?country)
     [?place :place/type "Country"]
     [(ground ?place) ?country]]
    [(place-country ?place ?country)
     [?place :place/isPartOf ?parent]
     (place-country ?parent ?country)]])

(def rule-tagclass-descendant
  '[[(tagclass-descendant ?root ?desc)
     [(ground ?root) ?desc]]
    [(tagclass-descendant ?root ?desc)
     [?desc :tagclass/isSubclassOf ?root]]
    [(tagclass-descendant ?root ?desc)
     [?desc :tagclass/isSubclassOf ?parent]
     (tagclass-descendant ?root ?parent)]])

(def rule-friends-3
  "Find friends within 3 hops, call (min ?dist) in :find to get the minimum distance."
  '[[(friends-3 ?start ?friend ?dist)
     [?k :knows/person1 ?start]
     [?k :knows/person2 ?friend]
     [(ground 1) ?dist]]
    [(friends-3 ?start ?friend ?dist)
     [?k1 :knows/person1 ?start]
     [?k1 :knows/person2 ?mid]
     [?k2 :knows/person1 ?mid]
     [?k2 :knows/person2 ?friend]
     [(not= ?friend ?start)]
     [(ground 2) ?dist]]
    [(friends-3 ?start ?friend ?dist)
     [?k1 :knows/person1 ?start]
     [?k1 :knows/person2 ?mid1]
     [?k2 :knows/person1 ?mid1]
     [?k2 :knows/person2 ?mid2]
     [(not= ?mid2 ?start)]
     [?k3 :knows/person1 ?mid2]
     [?k3 :knows/person2 ?friend]
     [(not= ?friend ?start)]
     [(not= ?friend ?mid1)]
     [(ground 3) ?dist]]])

(def rule-interaction-half-weight
  "Interaction half-weights between two persons (both directions)."
  '[[(interaction-half-weight ?a ?b ?interaction ?half-weight)
     [?interaction :message/hasCreator ?a]
     [?interaction :message/replyOf ?post]
     [_ :forum/containerOf ?post]
     [?post :message/hasCreator ?b]
     [(ground 0.5) ?half-weight]]
    [(interaction-half-weight ?a ?b ?interaction ?half-weight)
     [?interaction :message/hasCreator ?b]
     [?interaction :message/replyOf ?post]
     [_ :forum/containerOf ?post]
     [?post :message/hasCreator ?a]
     [(ground 0.5) ?half-weight]]
    [(interaction-half-weight ?a ?b ?interaction ?half-weight)
     [?interaction :message/hasCreator ?a]
     [?interaction :message/replyOf ?parent]
     [?parent :message/replyOf _]
     [?parent :message/hasCreator ?b]
     [(ground 0.25) ?half-weight]]
    [(interaction-half-weight ?a ?b ?interaction ?half-weight)
     [?interaction :message/hasCreator ?b]
     [?interaction :message/replyOf ?parent]
     [?parent :message/replyOf _]
     [?parent :message/hasCreator ?a]
     [(ground 0.25) ?half-weight]]])
