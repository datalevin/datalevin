(ns datalevin.remote-pull-transform-cache-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.test.core :refer [server-fixture]]))

(use-fixtures :once server-fixture)

(deftest remote-random-pull-transforms-are-not-memoized
  (with-open [^java.io.Closeable conn
              (d/create-conn
                (str "dtlv://datalevin:datalevin@localhost/pull-xform-" (random-uuid))
                {:value {:db/valueType :db.type/long}
                 :friend {:db/valueType :db.type/ref}}
                {:background-sampling? false})]
    (d/transact! conn [{:db/id 1 :value 1000000000 :friend 2}
                       {:db/id 2 :value 1000000000}])
    (doseq [interval [0 60000]
            pattern '[[[:value :xform rand]]
                      [[:value :xform rand-int]]
                      [{:friend [[:value :xform rand]]}]]]
      (testing (str pattern ", freshness interval " interval)
        (binding [c/*remote-db-last-modified-check-interval-ms* interval]
          (let [view @conn
                store (:store view)
                prepared (d/prepare-pull view pattern)
                before (i/db-info store)]
            (doseq [read [#(d/pull view pattern 1)
                          #(prepared 1)
                          #(d/pull-many view pattern [1])
                          #(i/pull store pattern 1 {})]]
              (let [results (repeatedly 4 read)]
                (is (< 1 (count (distinct results)))
                    "each API must execute the transform again without a write")))
            (is (= before (i/db-info store)))))))))
