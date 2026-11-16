(ns datalevin.query.source-filtered-negation-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.program :as program]
            [datalevin.query.source.runtime :as rt]))

(defn- provider []
  (fake/source
    (fake/info {:weight {:entity-type :long :value-type :long}
                :tag {:entity-type :long :value-type :string}})
    [[2 :weight 100] [3 :weight 115] [4 :weight 100] [4 :weight 120]
     [5 :weight 114] [6 :weight 116] [6 :weight 117]
     [4 :tag "selected"] [6 :tag "other"]]
    {:mode :both}))

(defn- closed? [s]
  (let [events @(:events s)
        acquired (filter #(= :acquire (first %)) events)]
    (and (= (count acquired) (count (filter #(= :close-session (first %)) events)))
         (every? #(every? zero? (vals (control/usage (:control (second %))))) acquired))))

(deftest absence-means-no-qualifying-fact
  (doseq [body ['[[$s ?e :weight ?weight] [(> ?weight 114)]]
               '[[(> ?weight 114)] [$s ?e :weight ?weight]]]]
    (let [s (provider)
          q {:find '[?e] :in '[$s [?e ...]]
             :where [(apply list 'not-join '[?e] body)]}]
      (is (= #{[1] [2] [5]} (d/q q s [1 2 3 4 5 6 2])))
      (is (closed? s)))))

(deftest optional-filter-retains-unmatched-and-below-threshold-rows
  (let [s (provider)
        q '[:find ?e ?w ?witness :in $s [[?e ?witness]]
            :where
            (or-join [[?e] ?w]
              (and [$s ?e :weight ?w] [(> ?w 114)])
              (and (not-join [?e] [$s ?e :weight ?weight] [(> ?weight 114)])
                   [(ground [nil]) [?w]]))]]
    (is (= #{[1 nil :a] [2 nil :a] [2 nil :b] [3 115 :a]
             [4 120 :a] [5 nil :a] [6 116 :a] [6 117 :a]}
           (d/q q s [[1 :a] [2 :a] [2 :b] [3 :a] [4 :a] [5 :a] [6 :a]])))
    (is (closed? s))))

(deftest qualification-can-use-outer-inputs-and-mixed-source-facts
  (let [s (provider)]
    (is (= #{[1] [2] [3] [5] [6]}
           (d/q '[:find ?e :in $s $tags [?e ...] ?threshold
                  :where (not-join [?e ?threshold]
                           [$s ?e :weight ?w] [(> ?w ?threshold)]
                           [$s ?e :tag ?tag] [$tags ?tag])]
             s [["selected"]] [1 2 3 4 5 6] 114)))
    ;; A negative child's non-null qualification must not narrow outer values.
    (is (= #{[1 nil] [2 nil] [3 nil]}
           (d/q '[:find ?e ?value :in $s [[?e ?value]]
                  :where (not-join [?e ?value]
                           [$s ?e :weight ?w] [(some? ?value)] [(> ?w 114)])]
             s [[1 nil] [2 nil] [3 nil] [3 :present]])))
    (is (closed? s))))

(deftest unsupported-negative-calls-and-unbound-inputs-reject-before-acquisition
  (doseq [body ['[[$s ?e :weight ?w] [(clojure.core/odd? ?w)]]
               '[[$s ?e :weight ?w] [(> ?missing 114)]]
               '[[$s ?e :weight ?w] [(> ?w (inc 114))]]
               '[[$s ?e :weight ?w] [(inc ?w) ?next]]
               '[[$s ?e :weight ?w] (not [$s ?e :tag _])]]]
    (let [s (provider)
          failure (try (d/q {:find '[?e] :in '[$s [?e ...]]
                            :where [(apply list 'not-join '[?e] body)]} s [1 2])
                       nil (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :query-source/unsupported (:error failure)))
      (is (empty? (filter #(= :acquire (first %)) @(:events s)))))))

(deftest incomplete-filtered-absence-never-produces-an-unmatched-row
  (doseq [failure [:fetch :quota :cancel]]
    (let [pages (atom 0)
          s (fake/source (fake/info {:weight {:entity-type :long :value-type :long}})
              [[1 :weight 100] [2 :weight 120]]
              (cond-> {:page-requests? true}
                (not= failure :quota)
                (assoc :hooks {:fetch (fn [{:keys [control]}]
                                       (when (= 2 (swap! pages inc))
                                         (if (= failure :cancel)
                                           (control/cancel! control {:error :query-source/cancelled})
                                           (throw (ex-info "incomplete qualification" {:error :query-source/invalid})))))})))
          saved (atom nil)
          result (try
                   (rt/with-runtime
                     {:limits (cond-> {:batch-rows 1} (= failure :quota) (assoc :external-requests 1))}
                     (fn [runtime]
                       (reset! saved runtime)
                       (program/query! runtime
                         '[:find ?e :in $s [?e ...]
                           :where (not-join [?e] [$s ?e :weight ?w] [(> ?w 114)])]
                         [s [1 2 3]])))
                   nil (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= (case failure :fetch :query-source/invalid :quota :query-source/limit :cancel :query-source/cancelled)
             (:error result)))
      (is (= 1 (count (filter #(= :close-session (first %)) @(:events s)))))
      (is (every? zero? (vals (dissoc (control/usage (:control @saved)) :external-requests)))))))
