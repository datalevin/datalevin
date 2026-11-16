(ns datalevin.query.source-key-demand-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.core :as d]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.fake :as fake])
  (:import [java.io Closeable]
           [datalevin.utl QueryKey]))

(def query
  '{:find [?e ?v ?w] :in [$a $b $c $d] :limit 2
    :where [(or-join [?e ?v ?av]
              (and [$a ?e :row/value ?v] [(ground 0) ?av])
              (and [$b ?e :row/value ?v] [(ground 1) ?av]))
            (or-join [?e ?w ?bv]
              (and [$c ?e :row/value ?w] [(ground 0) ?bv])
              (and [$d ?e :row/value ?w] [(ground 1) ?bv]))]})

(defn provider [rows config]
  (let [base (fake/source (fake/row-info (get config :value-type :string)) (fake/row-facts rows)
               (merge {:mode :both} (select-keys config [:consumption :actual-consistency :replay])))
        events (atom []) controls (atom [])]
    {:base base :events events :controls controls
     :source
     (reify source/IQuerySource
       (-source-info [_] (source/-source-info base))
       (-plan-source [_ session request]
         (let [result (source/-plan-source base (:delegate session) request)]
           (update result :alternatives
             #(mapv (fn [a]
                      (assoc a :estimate
                        (merge (fake/unknown-estimate (boolean (seq (:requires a))))
                          {:confidence :high :startup-ns 1 :requests-per-open 1
                           :provenance {:kind :statistics :id "keys" :observed-at-ms (System/currentTimeMillis) :valid-for-ms nil}}
                          (if (seq (:requires a))
                            {:rows-per-binding 1 :per-binding-ns 1 :requests-per-binding 0}
                            {:rows (get config :estimated-rows (count rows)) :run-ns 1000000})))) %))))
       (-open-session [_ request]
         (let [delegate (source/-open-session base request)]
           (swap! controls conj (:control request))
           ;; The wrapper exposes the exact acquired delegate to ordinary plans.
           (reify
             clojure.lang.ILookup
             (valAt [_ k] (when (= k :delegate) delegate))
             (valAt [_ k default] (if (= k :delegate) delegate default))
             source/IQuerySourceSession
             (-session-info [_] (source/-session-info delegate))
             (-open-cursor [_ open] (source/-open-cursor delegate open))
             (-cancel-session! [_ reason] (source/-cancel-session! delegate reason))
             (-close-session! [_] (source/-close-session! delegate))
             source/IRelationalQuerySession
             (-open-relational-cursor [_ open]
               (swap! events conj [:window-open (:plan open)])
               (when-let [f (:on-window config)] (f))
               (let [position (atom 0) key (:key (:plan open))
                     limit (get-in open [:plan :window :limit])
                     keys (reduce (fn [out x]
                                    (if (some #(QueryKey/equals [%] [x]) out) out (conj out x))) []
                                  (map #(if (= key :entity) (first %) (nth % 2)) @(:snapshot delegate)))
                     keys (if (:duplicate-keys? config) (mapcat #(repeat 2 %) keys) keys)
                     keys (vec (take (if (:oversize? config) (inc limit) limit) keys))]
                 (reify source/IQuerySourceCursor
                   (-next-batch! [_ fetch]
                     (when (and (:fail-tail? config) (pos? @position))
                       (throw (ex-info "broken key tail" {:error :query-source/provider :phase :fetch})))
                     (when (:cancel? config) (control/cancel! (:control request) {:error :query-source/cancelled :phase :fetch}))
                     (with-open [_permit ^Closeable (source/acquire-request! (:limiter delegate))]
                       (let [end (min (count keys) (+ @position (:max-rows fetch)))
                             rows (mapv vector (subvec keys @position end))
                             columns (:columns open) bytes (source/batch-charge-bytes columns rows)
                             token (source/reserve! (:control request) {:retained-bytes bytes :in-flight-batches 1})]
                         (reset! position end)
                         {:columns columns :rows rows :row-count (count rows) :byte-count bytes
                          :reservation token :exhausted? (= end (count keys))})))
                   (-cursor-metrics [_] {:batches 1 :rows @position :charged-bytes 0 :read-rows @position
                                         :read-bytes nil :external-requests 1 :elapsed-ns 0})
                   (-close-cursor! [_] (swap! events conj [:window-close]))))))))
       source/IRelationalWindowSource
       (-plan-relational-window [_ request]
         (swap! events conj [:window-plan (get-in request [:window :limit])])
         (when (and (not (:decline? config))
                    (<= (get-in request [:window :limit]) (get config :max-window Long/MAX_VALUE)))
           (let [key (get-in request [:find 0 :variable]) term (get-in request [:clauses 0 :terms 0])]
             {:columns (:columns request)
              :plan {:window (:window request) :key (if (= term [:var key]) :entity :value)}}))))}))

(defn fixtures [rows configs]
  (mapv provider rows (concat configs (repeat {}))))

(defn sources [ps] (mapv :source ps))
(defn released! [ps]
  (doseq [p ps]
    (let [events @(:events (:base p))]
      (is (= 1 (count (filter #(= :descriptor (first %)) events))))
      (is (= 1 (count (filter #(= :acquire (first %)) events))))
      (is (= 1 (count (filter #(= :close-session (first %)) events)))))
    (doseq [ctl @(:controls p)]
      (is (every? zero? (vals (dissoc (control/usage ctl) :external-requests)))))))

(deftest full-qualification-deduplicates-across-keys-and-preserves-snapshots
  (let [ps-ref (atom nil) changed? (atom false)
        mutate #(when (compare-and-set! changed? false true)
                  (doseq [p @ps-ref] (fake/replace-facts! (:base p) (fake/row-facts [[999 "new"]]))))
        rows [[[1 "old"] [2 "old"]] [[1 "old"] [2 "old"]]
              [[1 "right"] [2 "right"]] [[1 "right"] [2 "right"]]]
        ps (fixtures rows (repeat {:on-window mutate}))]
    (reset! ps-ref ps)
    (is (= #{[1 "old" "right"] [2 "old" "right"]} (set (apply d/q query (sources ps)))))
    (is @changed?)
    (doseq [p ps]
      (is (= (count (filter #(= :plan (first %)) @(:events (:base p))))
             (count (set (map #(get-in % [1 :region-id]) (filter #(= :plan (first %)) @(:events (:base p)))))))))
    (released! ps)))

(deftest insufficient-windows-grow-and-exhaust-with-global-distinctness
  (let [rows (mapv #(vector % "same") (range 300)) ps (fixtures (repeat 4 rows) [])
        q (assoc query :find '[?v])]
    (is (= #{["same"]} (set (apply d/q q (sources ps)))))
    (is (some #(= [:window-plan 512] %) (mapcat #(deref (:events %)) ps)))
    (released! ps)))

(deftest later-window-decline-uses-the-captured-ordinary-fallback
  (let [left (mapv #(vector % "left") (range 300)) right [[299 "right"]]
        ps (fixtures [left left right right]
             [{:estimated-rows 1 :max-window 256} {:estimated-rows 1 :max-window 256}
              {:estimated-rows 1000} {:estimated-rows 1000}])]
    (is (= #{[299 "left" "right"]} (set (apply d/q (assoc query :limit 1) (sources ps)))))
    (released! ps)))

(deftest intermediate-keys-do-not-spend-public-result-quota
  (let [rows (mapv #(vector % (str %)) (range 300)) ps (fixtures (repeat 4 rows) [])]
    (is (= 1 (count (apply d/q (assoc query :limit 1 :query-source {:limits {:result-rows 1}}) (sources ps)))))
    (released! ps)))

(deftest partial-or-invalid-key-streams-never-become-results-or-fallback
  (doseq [config [{:fail-tail? true} {:oversize? true} {:cancel? true} {:duplicate-keys? true}]]
    (let [rows (mapv #(vector % (str %)) (range 300)) ps (fixtures (repeat 4 rows) (repeat config))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (apply d/q (assoc query :limit 1 :query-source {:limits {:batch-rows 2}}) (sources ps))))
      (is (not-any? #(= :open (first %)) (mapcat #(deref (:events (:base %))) ps)))
      (released! ps))))

(deftest one-shot-or-statement-reads-retain-the-ordinary-plan
  (doseq [config [{:consumption {:opens :once :max-open-cursors 1}}
                  {:actual-consistency :statement} {:decline? true} {:replay :unstable}]]
    (let [ps (fixtures (repeat 4 [[1 "same"]]) (repeat config))
          q (cond-> query (= :statement (:actual-consistency config))
              (assoc :query-source {:consistency :statement}))]
      (is (= #{[1 "same" "same"]} (set (apply d/q q (sources ps)))))
      (is (not-any? #(= :window-open (first %)) (mapcat #(deref (:events %)) ps)))
      (released! ps))))

(deftest optional-completion-keeps-multiplicity-and-unprojected-qualification
  (let [q (-> query
              (assoc :find '[?e ?v ?w ?right] :limit 10 :in '[$a $b $c $d $right $qual])
              (update :where conj
                '(or-join [[?e] ?right]
                   (and [$right ?e :row/value ?right] [$qual ?e :row/value "ok"])
                   (and (not-join [?e] [$right ?e :row/value ?candidate] [$qual ?e :row/value "ok"])
                        [(ground [nil]) [?right]]))))
        ps (fixtures [[[1 "a"] [2 "b"]] [[1 "a"] [2 "b"]]
                      [[1 "c"] [2 "d"]] [[1 "c"] [2 "d"]]
                      [[1 "r1"] [1 "r2"] [2 "unqualified"]] [[1 "ok"]]] [])]
    (is (= #{[1 "a" "c" "r1"] [1 "a" "c" "r2"] [2 "b" "d" nil]}
           (set (apply d/q q (sources ps)))))
    (released! ps)))

(deftest windows-share-one-query-request-budget
  (let [rows (mapv #(vector % "same") (range 300)) ps (fixtures (repeat 4 rows) [])
        q (assoc query :find '[?v] :query-source {:limits {:external-requests 1}})]
    (is (= :query-source/limit
           (try (apply d/q q (sources ps)) nil
                (catch clojure.lang.ExceptionInfo e (:error (ex-data e))))))
    (released! ps)))

(deftest numeric-key-binding-preserves-representation-sensitive-distinctness
  (let [q '{:find [?a ?b ?key] :in [$a $b $c $d] :limit 10
            :where [(or-join [?a ?key ?av]
                      (and [$a ?a :row/value ?key] [(ground 0) ?av])
                      (and [$b ?a :row/value ?key] [(ground 1) ?av]))
                    (or-join [?b ?key ?bv]
                      (and [$c ?b :row/value ?key] [(ground 0) ?bv])
                      (and [$d ?b :row/value ?key] [(ground 1) ?bv]))]}
        rows [[[1 1.0M] [2 1.00M]] [] [[3 1.0M] [4 1.00M]] []]
        before (fixtures rows (repeat {:value-type :bigdec :decline? true}))
        after (fixtures rows (repeat {:value-type :bigdec}))
        a (apply d/q q (sources before)) b (apply d/q q (sources after))]
    (is (= (count a) (count b)))
    (is (every? (fn [row] (some #(QueryKey/equals row %) b)) a))
    (released! before)
    (released! after)))

(deftest explain-and-unsupported-window-shapes-do-not-read
  (doseq [q [query (assoc query :order-by '[?e :asc]) (assoc query :offset 1)
              (dissoc query :limit) (assoc query :find '[(count ?e)]) (assoc query :with '[?av])]]
    (let [ps (fixtures (repeat 4 [[1 "same"]]) []) result (apply d/explain {:run? false} q (sources ps))]
      (is (not-any? #(= :window-open (first %)) (mapcat #(deref (:events %)) ps)))
      (when (not= q query) (is (nil? (get-in result [:operators :key-demand]))))
      (released! ps))))
