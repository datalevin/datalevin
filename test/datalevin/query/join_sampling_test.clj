(ns datalevin.query.join-sampling-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.core :as d]
   [datalevin.query.cache :as cache]
   [datalevin.query.execute :as execute]
   [datalevin.query.optimizer.join-sampling :as sampling]
   [datalevin.query.plan :as plan])
  (:import [java.util IdentityHashMap List]
           [org.eclipse.collections.impl.list.mutable FastList]))

(use-fixtures :each
  (fn [f] (binding [sampling/*enabled?* true] (f))))

(def schema
  {:left/p {:db/valueType :db.type/long}
   :left/s {:db/valueType :db.type/long}
   :right/p {:db/valueType :db.type/long}
   :right/s {:db/valueType :db.type/long}
   :right/keep {:db/valueType :db.type/boolean}})

(defn tuples [rows]
  (let [out (FastList.)]
    (doseq [row rows] (.add out (object-array row)))
    out))

(def left-cols ['?l #{:left/p '?p} #{:left/s '?s}])
(def joined-cols (conj left-cols '?r))
(def link {:type :val-eq :var '?p :tgt '?r :attrs {'?l :left/p '?r :right/p}})

(defn parent [rows]
  {:size (count rows)
   :steps [(plan/map->InitStep {:cols left-cols :result (tuples rows)})]})

(defn join-steps [attrs]
  [(plan/map->LinkStep {:type :val-eq :index 1 :attr :right/p :var '?r
                       :cols joined-cols})
   (plan/map->MergeScanStep {:index 3 :cols joined-cols :attrs-v attrs})])

(def match-steps (join-steps [[:right/s {:skip? true :fidx 2}]]))

(defn measure [database cache parent target steps]
  (sampling/join-estimate database cache parent target link steps))

(deftest complete-match-counts-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn (for [p (range 64) s (range 4)]
                       {:right/p p :right/s s :right/keep (even? p)}))
    (binding [sampling/*budget* (sampling/new-budget)]
      (let [cache (IdentityHashMap.) target (Object.)
            input (parent (for [p (range 64)] [p p (mod p 4)]))
            sample (measure @conn cache input target match-steps)
            spent @sampling/*budget*]
        (testing "all equality keys are checked; candidates remain separate"
          (is (= {:input 64 :matches 64 :candidates 256 :size 64 :candidate-size 256}
                 (dissoc sample :candidate-link))))
        (testing "repeated estimates reuse observations without spending more"
          (is (= sample (measure @conn cache input target match-steps)))
          (is (= spent @sampling/*budget*))
          (is (= 640 (:size (measure @conn cache (assoc input :size 640)
                                    target match-steps)))))
        (testing "another primary index shares yield, not its candidate count"
          (let [other (sampling/join-estimate @conn cache input target
                                             (assoc link :var '?s) [])]
            (is (= 64 (:size other)))
            (is (nil? (:candidate-size other)))
            (is (= spent @sampling/*budget*))))
        (testing "another prefix with the same link cannot reuse its yield"
          (let [other (parent (for [p (range 64)] [p p (if (< p 4) 0 99)]))]
            (is (= 4 (:size (measure @conn cache other target match-steps))))))
        (testing "target filters are evaluated together with equality"
          (let [steps (join-steps [[:right/s {:skip? true :fidx 2}]
                                  [:right/keep {:skip? true :pred true?}]])]
            (is (= 32 (:size (measure @conn cache input (Object.) steps))))))
        (testing "duplicate input tuples retain their multiplicity"
          (let [input (parent (repeat 10 [0 0 0]))]
            (is (= 10 (:matches (measure @conn cache input target match-steps))))))
        (testing "zero observed matches fall back instead of proving empty"
          (is (nil? (measure @conn cache (parent [[0 0 99]]) target match-steps))))))))

(deftest many-to-many-and-prefix-layout-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn (repeat 8 {:right/p 1 :right/s 2}))
    (binding [sampling/*budget* (sampling/new-budget)]
      (let [cache (IdentityHashMap.) input (parent [[7 1 2] [8 1 2]])
            sample (measure @conn cache input (Object.) match-steps)
            ;; Hash joins can put the same logical columns in another order.
            physical (sampling/with-source
                       {:steps [(plan/map->HashJoinStep
                                  {:cols ['?r #{:right/s '?s} '?l #{:right/p '?p}]})]}
                       input match-steps)
            output (sampling/prefix-sample @conn cache physical)]
        (is (= 16 (:matches sample)))
        (is (= 16 (:size sample)))
        (is (= 16 (.size ^List (:tuples output))))
        (is (= #{[2 7 1] [2 8 1]}
               (set (map #(subvec (vec %) 1) (:tuples output)))))
        (is (true? (:complete? output)))
        (testing "a cached logical prefix is projected into the caller's layout"
          (let [other (assoc physical :steps [(plan/map->HashJoinStep
                                              {:cols joined-cols})])
                spent @sampling/*budget*
                projected (sampling/prefix-sample @conn cache other)]
            (is (= #{[7 1 2] [8 1 2]}
                   (set (map #(subvec (vec %) 0 3) (:tuples projected)))))
            (is (= spent @sampling/*budget*))))))))

(deftest bounded-and-inconclusive-sampling-test
  (with-open [conn (d/create-conn nil
                                  (assoc schema :right/tags
                                         {:db/valueType :db.type/long
                                          :db/cardinality :db.cardinality/many})
                                  {:background-sampling? false})]
    (d/transact! conn (repeat 200 {:right/p 1 :right/s 2 :right/tags [1 2]}))
    (let [input (parent [[7 1 2]])]
      (testing "a large duplicate list is rejected before materialization"
        (binding [sampling/*budget* (sampling/new-budget)
                  sampling/*max-rows* 32]
          (is (nil? (measure @conn (IdentityHashMap.) input (Object.) match-steps)))
          (is (= 1 (:rows @sampling/*budget*)))
          (is (<= (:probes @sampling/*budget*) sampling/*max-probes*))))
      (testing "an exhausted budget falls back"
        (binding [sampling/*budget* (sampling/new-budget)
                  sampling/*max-probes* 0]
          (is (nil? (measure @conn (IdentityHashMap.) input (Object.) match-steps)))))
      (testing "plan enumeration reuses observations without issuing new probes"
        (binding [sampling/*budget* (sampling/new-budget)
                  sampling/*cached-only?* true]
          (is (nil? (measure @conn (IdentityHashMap.) input (Object.) match-steps)))
          (is (= {:probes 0 :rows 0 :cost 0.0} @sampling/*budget*))))
      (testing "unbounded multi-valued EAV products are not sampled"
        (binding [sampling/*budget* (sampling/new-budget)]
          (is (nil? (measure @conn (IdentityHashMap.) input (Object.)
                             (join-steps [[:right/tags {:skip? false}]])))))))))

(deftest native-planner-uses-complete-matches-test
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn
                 (concat
                   (for [p (range 64)] {:left/p p :left/s p})
                   (for [p (range 64) s [p (+ 1000 p) (+ 2000 p) (+ 3000 p)]]
                     {:right/p p :right/s s})))
    (let [query '[:find ?l ?r :where
                  [?l :left/p ?p] [?l :left/s ?s]
                  [?r :right/p ?p] [?r :right/s ?s]]
          context (execute/plan-context* (cache/parsed-q query) [@conn])
          trace (get-in context [:plan '$ 0])
          final (peek (vec trace))]
      (is (= 64 (count (d/q query @conn))))
      (is (= 64 (:size final)))
      (is (pos? (get-in final [:join-sample :matches] 0)))
      (is (every? #(not (contains? % ::sampling/source)) trace))
      (testing "cached plans retain estimates without retaining sample recipes"
        (is (= 64 (:size (last (get-in (execute/plan-context*
                                        (cache/parsed-q query) [@conn])
                                       [:plan '$ 0])))))))))

(deftest experimental-sampling-is-opt-in-test
  (is (false? (.getRawRoot #'sampling/*enabled?*)))
  (binding [sampling/*enabled?* false
            sampling/*budget* (sampling/new-budget)]
    (is (nil? (measure nil (IdentityHashMap.) (parent [[1 1 1]])
                       (Object.) match-steps)))
    (is (= {:probes 0 :rows 0 :cost 0.0} @sampling/*budget*)))
  (binding [sampling/*budget* nil]
    (let [candidate {:size 7}]
      (is (identical? candidate
                      (sampling/with-source candidate (parent [[1 1 1]]) [])))))
  (with-open [conn (d/create-conn nil schema {:background-sampling? false})]
    (d/transact! conn
                 (concat (for [p (range 64)] {:left/p p :left/s p})
                         (for [p (range 64) s [p (+ p 1000)]]
                           {:right/p p :right/s s})))
    (let [query '[:find ?l ?r :where
                  [?l :left/p ?p] [?l :left/s ?s]
                  [?r :right/p ?p] [?r :right/s ?s]]]
      (doseq [enabled? [true false true false]]
        (binding [sampling/*enabled?* enabled?]
          (let [trace (get-in (execute/plan-context* (cache/parsed-q query) [@conn])
                              [:plan '$ 0])]
            (is (= enabled? (boolean (:join-sample (last trace)))))
            (is (every? #(not (contains? % ::sampling/source)) trace))
            (is (= 64 (count (d/q query @conn))))))))))
