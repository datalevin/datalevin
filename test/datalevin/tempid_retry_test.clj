(ns datalevin.tempid-retry-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.test.core :refer [db-fixture]]
            [datalevin.util :as u]))

(use-fixtures :each db-fixture)

(def schema
  {:person/id {:db/valueType :db.type/long :db/unique :db.unique/identity}
   :card/id {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :card/owner {:db/valueType :db.type/ref}})

(defn- with-store [wal? f]
  (let [dir (u/tmp-dir (str "tempid-retry-" (random-uuid)))
        conn (d/create-conn dir schema {:wal? wal?})]
    (try (f conn) (finally (d/close conn) (u/delete-files dir)))))

(defn- forward-refs [n maps?]
  (vec
    (concat
      (for [i (range n)]
        {:db/id (str "card-" i) :card/id (str "c" i)
         :card/owner (str "person-" i)})
      (for [i (range n)]
        (if maps?
          {:db/id (str "person-" i) :person/id i}
          [:db/add (str "person-" i) :person/id i])))))

(deftest many-late-identity-maps-resolve-forward-refs
  (doseq [[wal? prepare?] [[false false] [true false] [true true]]]
    (testing (str "WAL=" wal? " prepare=" prepare?)
      (with-store wal?
        (fn [conn]
          (let [n 3200]
            (d/transact! conn (mapv #(hash-map :person/id %) (range n)))
            (let [report (binding [c/*use-prepare-path* prepare?]
                           (d/transact! conn (forward-refs n true)))
                  owners (d/q '[:find ?id ?person-id
                                :where [?card :card/id ?id]
                                [?card :card/owner ?person]
                                [?person :person/id ?person-id]] @conn)]
              (is (= (set (map #(vector (str "c" %) %) (range n))) owners))
              (is (every? #(= (d/entid @conn [:person/id %])
                              (get (:tempids report) (str "person-" %)))
                          (range n)))
              (is (= (* 2 n) (count (d/q '[:find ?e :where [?e]] @conn)))))))))))

(deftest sequential-late-upserts-retry-without-growing-the-stack
  (with-store true
    (fn [conn]
      (let [n 256
            depths (atom [])
            probe (fn [_]
                    (swap! depths conj (alength (.getStackTrace (Thread/currentThread))))
                    [])]
        (d/transact! conn (mapv #(hash-map :person/id %) (range n)))
        (let [report (d/transact! conn (into [[:db.fn/call probe]]
                                            (forward-refs n false)))]
          (is (= (inc n) (count @depths)))
          (is (< (- (apply max @depths) (apply min @depths)) 16))
          (is (= n (count (d/q '[:find ?card :where [?card :card/owner]] @conn))))
          (is (every? #(= (d/entid @conn [:person/id %])
                          (get (:tempids report) (str "person-" %)))
                      (range n))))))))

(deftest ordered-identity-retraction-is-not-resolved-in-advance
  (with-store true
    (fn [conn]
      (d/transact! conn [{:db/id 1 :person/id 0}])
      (let [report (d/transact! conn [[:db/retract 1 :person/id 0]
                                      {:db/id "card" :card/id "c"
                                       :card/owner "person"}
                                      {:db/id "person" :person/id 0}])
            person (get (:tempids report) "person")]
        (is (not= 1 person))
        (is (= person (d/entid @conn [:person/id 0])))
        (is (= person (:db/id (:card/owner (d/entity @conn [:card/id "c"])))))))))
