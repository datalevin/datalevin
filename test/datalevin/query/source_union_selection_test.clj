(ns datalevin.query.source-union-selection-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake]
            [datalevin.query.source.join-cost :as cost]
            [datalevin.query.source.union-selection :as selection]))

(def query
  '[:find ?e ?v ?endpoint :in $driver $a $b
    :where [$driver ?e :row/value ?label]
    (or-join [?e ?v ?endpoint]
      (and [$a ?e :row/value ?v] [(ground 0) ?endpoint])
      (and [$b ?e :row/value ?v] [(ground 1) ?endpoint]))])

(defn provider [rows {:keys [known? startup scan-run per-binding opens estimated-rows]
                      :or {known? true startup 10 scan-run 1000000 per-binding 1 opens :many}}]
  (let [p (fake/source (fake/row-info) (fake/row-facts rows)
            {:mode :both :consumption {:opens opens :max-open-cursors 1}})]
    {:events (:events p)
     :source
     (reify source/IQuerySource
       (-source-info [_] (source/-source-info p))
       (-open-session [_ request] (source/-open-session p request))
       (-plan-source [_ session request]
         (update (source/-plan-source p session request) :alternatives
           (fn [as]
             (mapv (fn [a]
                     (if-not known? a
                       (assoc a :estimate
                         (merge (fake/unknown-estimate (boolean (seq (:requires a))))
                           {:confidence :high
                            :provenance {:kind :statistics :id "fixture" :observed-at-ms (System/currentTimeMillis) :valid-for-ms nil}
                            :startup-ns startup :requests-per-open 1.0}
                           (if (seq (:requires a))
                             {:rows-per-binding (or estimated-rows 1.0) :per-binding-ns per-binding :requests-per-binding 0.0}
                             {:rows (or estimated-rows (count rows)) :run-ns scan-run}))))) as)))))}))

(defn events [p kind] (filterv #(= kind (first %)) @(:events p)))
(defn explanation [config driver-rows]
  (let [driver (provider driver-rows {:scan-run 1})
        a (provider (mapv #(vector % (str "v" %)) (range 1 41)) config)
        b (provider (mapv #(vector % (str "v" %)) (range 1 41)) config)
        ps [driver a b]
        result (apply d/explain {:run? true :intermediate-counts? true} query (map :source ps))]
    {:result result :providers ps}))

(deftest sparse-bindings-reach-every-endpoint-without-extra-planning-callbacks
  (let [{:keys [result providers]} (explanation {} [[1 "first"] [1 "second"]])
        root (first (filter #(zero? (:region-id %)) (:regions result)))
        [driver a b] providers]
    (is (some seq (vals (get-in root [:union-selection :bindings]))))
    (is (= 2 (:result-rows result)))
    (doseq [p [a b]]
      (is (= 1 (count (events p :plan))))
      (is (= 1 (count (events p :acquire))))
      (is (every? #(seq (get-in % [1 :bindings])) (events p :open))))
    (is (= #{[1 "v1" 0] [1 "v1" 1]}
           (d/q query (:source driver) (:source a) (:source b))))
    (doseq [p providers]
      (is (= (count (events p :acquire)) (count (events p :close-session))))
      (is (every? zero? (vals (control/usage (:control (second (first (events p :acquire)))))))))))

(deftest dense-bindings-retain-independent-union-access
  (let [{:keys [result providers]}
        (explanation {:scan-run 1 :per-binding 1000000} (mapv #(vector % (str %)) (range 1 41)))
        root (first (filter #(zero? (:region-id %)) (:regions result)))]
    (is (= 80 (:result-rows result)))
    (is (every? empty? (vals (get-in root [:union-selection :bindings]))))
    (doseq [p (rest providers)]
      (is (every? #(not (contains? (second %) :bindings)) (events p :open))))))

(deftest unknown-or-one-shot-branches-keep-the-existing-plan
  (doseq [config [{:known? false} {:opens :once}]]
    (let [{:keys [result providers]} (explanation config [[1 "first"]])
          root (first (filter #(zero? (:region-id %)) (:regions result)))]
      (is (= 2 (:result-rows result)))
      (is (nil? (:union-selection root)))
      (doseq [p (rest providers)]
        (is (= 1 (count (events p :open))))
        (is (= 1 (count (events p :close-session))))))))

(deftest binding-direction-follows-the-caller-and-restores-outer-witnesses
  (let [q '[:find ?e ?v ?endpoint :in $driver $a $b
            :where [$driver ?driver :row/value ?v]
            (or-join [?e ?v ?endpoint]
              (and [$a ?e :row/value ?v] [(ground 0) ?endpoint])
              (and [$b ?e :row/value ?v] [(ground 1) ?endpoint]))]
        driver (provider [[100 "v1"] [101 "v1"]] {:scan-run 1})
        a (provider [[1 "v1"] [2 "v2"]] {})
        b (provider [[1 "v1"] [2 "v2"]] {})
        inputs (mapv :source [driver a b])
        explained (apply d/explain {:run? false} q inputs)
        root (first (filter #(zero? (:region-id %)) (:regions explained)))]
    (is (= #{'?v} (set (mapcat val (get-in root [:union-selection :bindings])))))
    (is (= #{[1 "v1" 0] [1 "v1" 1]} (apply d/q q inputs)))
    (is (= 4 (apply d/q {:find '[(count ?endpoint) .] :in '[$driver $a $b]
                         :with '[?driver]
                         :where (subvec q (inc (.indexOf ^java.util.List q :where)))} inputs)))))

(deftest zero-estimates-do-not-prove-empty-union-branches
  (let [{:keys [result]} (explanation {:estimated-rows 0} [[1 "first"]])]
    (is (= 2 (:result-rows result)))))

(deftest optional-search-declines-without-hiding-query-failures
  (is (nil? (#'selection/optional-schedule!
              #(throw (ex-info "Bounded search" {:error :query-source/unsupported :phase :planning})))))
  (doseq [data [{:error :query-source/limit :phase :reserve}
               {:error :query-source/cancelled :phase :planning}
               {:error :query-source/invalid :phase :planning}
               {:error :query-source/unsupported :phase :execution}]]
    (let [error (ex-info "Must propagate" data)]
      (is (identical? error (try (#'selection/optional-schedule! #(throw error))
                                (catch Exception caught caught)))))))

(deftest independent-and-bound-costs-use-the-same-key-fanout
  (let [branch {:rows 1000 :batch 256 :dependent? false
                :estimate {:startup-ns 10 :run-ns 100 :requests-per-open 1}}
        lookup {:rows 2 :batch 256 :dependent? true
                :estimate {:startup-ns 10 :per-binding-ns 100 :requests-per-open 1 :requests-per-binding 0}}
        hint {:scan [branch] :lookups {#{'?key} [lookup]}}
        choice (fn [requires] {:kind :core :value {:union-estimate hint}
                              :requires requires :columns [{:variable '?key} {:variable '?out}]})
        input [{:kind :access :value {:rows 1000} :requires [] :columns [{:variable '?key}]}]
        model {:access-cost (fn [_ _] {:access-ns 0 :opens 1 :requests 1}) :local-row-ns 1}
        scan (cost/price (conj input (choice [])) [] :rows model)
        bound (cost/price (conj input (choice [{:variable '?key}])) [] :rows model)]
    (is (= 2000.0 (:tuples scan) (:tuples bound)))
    (is (< (:estimated-ns scan) (:estimated-ns bound)))
    (is (< (:estimated-requests scan) (:estimated-requests bound)))))
