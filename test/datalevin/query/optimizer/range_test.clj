(ns datalevin.query.optimizer.range-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.built-ins :as built-ins]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import [java.util UUID]))

(use-fixtures :once db-fixture)

(defn- with-values [values f]
  (let [root (u/tmp-dir (str "like-ranges-" (UUID/randomUUID)))
        conn (d/create-conn root {:text {:db/valueType :db.type/string}
                                  :copy {:db/valueType :db.type/string}})]
    (try
      (d/transact! conn (map-indexed (fn [i value]
                                     {:db/id (inc (long i)) :text value :copy value})
                                   values))
      (f @conn)
      (finally (d/close conn) (u/delete-files root)))))

(defn- like-query [op pattern opts]
  [:find '[?s ...] :where '[?e :text ?s] [(list op '?s pattern opts)]])

(defn- predicate [op pattern opts]
  (let [f (if (= op 'like) built-ins/like built-ins/not-like)]
    #(f % pattern opts)))

(defn- check-scan [db values op pattern opts]
  (testing (pr-str [op pattern opts])
    (let [pred (predicate op pattern opts)
          expected (into #{} (filter pred) values)
          query (like-query op pattern opts)]
      (is (= expected (set (d/q query db))))
      ;; An opaque predicate still runs on a scan, without LIKE-specific
      ;; range inference. Its result must agree with the optimized scan.
      (is (= expected
             (set (d/q '[:find [?s ...] :in $ ?pred
                         :where [?e :text ?s] [(?pred ?s)]] db pred))))
      ;; A known entity uses the reconstructed range predicate instead of
      ;; an AVE scan. It must respect the same bounds for Unicode values.
      (doseq [[i value] (map-indexed vector values)]
        (is (= (if (pred value) #{value} #{})
               (set (d/q [:find '[?s ...] :in '$ '?e
                          :where '[?e :text ?s] [(list op '?s pattern opts)]]
                         db (inc (long i))))))))))

(deftest not-like-keeps-nonmatches-inside-the-literal-prefix-test
  (let [values ["ab" "abc" "abcd" "abz" "ac" "ab%" "ab_" "ab!tail"]]
    (with-values values
      (fn [db]
        (doseq [pattern ["ab_" "ab%z" "ab%" "abc" "ab!%" "ab!_" "ab!!%"]]
          (check-scan db values 'not-like pattern nil))))))

(deftest exact-like-retains-projected-and-joined-bindings-test
  (with-values ["abc" "abcd" "zzz"]
    (fn [db]
      (is (= #{"abc"} (set (d/q (like-query 'like "abc" nil) db))))
      (is (= #{[1 "abc"]}
             (d/q '[:find ?e ?s :where [?e :text ?s] [(like ?s "abc")]] db)))
      (is (= #{"abc"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(and (like ?s "abc") (not= ?s "zzz"))]] db))))
      (is (= #{["abc" 1]}
             (d/q '[:find ?s ?other :where [?e :text ?s]
                    [(like ?s "abc")] [?other :copy ?s]] db)))
      (is (= 1 (d/q '[:find (count ?s) . :where [?e :text ?s]
                      [(like ?s "abc")]] db)))
      (is (= #{"abc"}
             (set (d/q '[:find [?s ...] :in $ ?pattern :where [?e :text ?s]
                         [(like ?s ?pattern)]] db "abc")))))))

(deftest like-prefix-ranges-include-unicode-suffixes-test
  (let [last-codepoint (String. (Character/toChars 0x10FFFF))
        values ["" "ab" "abc" "ab😀" "ab\uFFFF" "ac"
                "\u007F" "\u007F😀" "\u0080"
                "\uD7FFx" "\uE000x" "\uFFFF" "\uFFFF😀"
                "a\uFFFF" "a\uFFFF😀" "a😀" "a😀x" "a😁"
                last-codepoint (str last-codepoint "x")
                (str "a" last-codepoint) (str "a" last-codepoint "x")]]
    (with-values values
      (fn [db]
        (doseq [op ['like 'not-like]
                pattern ["ab%" "\u007F%" "\uD7FF%" "\uE000%" "\uFFFF%"
                         "a\uFFFF%" "a😀%" (str last-codepoint "%")
                         (str "a" last-codepoint "%") "abc" "\uFFFF"]]
          (check-scan db values op pattern nil))))))

(deftest escaped-wildcards-and-escape-characters-keep-matcher-semantics-test
  (let [values ["" "ab" "ab%" "ab%tail" "ab_" "ab_tail" "ab!"
                "ab!tail" "ab|" "ab|tail" "ab😀" "other"]]
    (with-values values
      (fn [db]
        (doseq [op ['like 'not-like]
                [pattern opts] [["" nil] ["ab!%" nil] ["ab!_%" nil] ["ab!!%" nil]
                                ["ab!" nil] ["!%%" nil]
                                ["ab|%" {:escape \|}]
                                ["ab|_%" {:escape \|}]
                                ["ab||%" {:escape \|}]
                                ["ab%" {:escape \u0100}]]]
          (check-scan db values op pattern opts))))))

(deftest long-patterns-do-not-use-truncated-index-keys-as-bounds-test
  (let [prefix (apply str (repeat (inc c/+val-bytes-wo-hdr+) "x"))
        values [prefix (str prefix "tail") (str prefix "😀") "other"]]
    (with-values values
      (fn [db]
        (doseq [op ['like 'not-like]
                pattern [prefix (str prefix "%") (str prefix "_") "x%"
                         (str (subs prefix 0 (dec c/+val-bytes-wo-hdr+)) "%")]]
          (check-scan db values op pattern nil))))))

(deftest invalid-like-patterns-are-rejected-before-scanning-test
  (with-values ["abc"]
    (fn [db]
      (doseq [op ['like 'not-like]
              [pattern opts] [["absent!x%" nil]
                              ["absent%!x" nil]
                              ["absent|x%" {:escape \|}]
                              ["absent%|x" {:escape \|}]]]
        (testing (pr-str [op pattern opts])
          (is (thrown-with-msg?
                IllegalStateException #"Can only escape"
                (d/q (like-query op pattern opts) db)))
          ;; Another predicate can also make the scan empty, including
          ;; for NOT LIKE, which does not infer a range of its own.
          (is (thrown-with-msg?
                IllegalStateException #"Can only escape"
                (d/q [:find '[?s ...] :in '$ '?pattern '?opts
                      :where '[?e :text ?s] '[(= ?s "missing")]
                      [(list op '?s '?pattern '?opts)]] db pattern opts)))
          (is (thrown-with-msg?
                IllegalStateException #"Can only escape"
                (d/q [:find '[?s ...] :in '$ '?e
                      :where '[?e :text ?s] [(list op '?s pattern opts)]]
                     db 1))))))))

(deftest combined-like-ranges-and-pattern-variables-test
  (with-values ["ab" "abc" "abcd" "abcx" "abz" "a%" "%c" "x%"]
    (fn [db]
      (is (= #{"abc" "abcd" "abcx" "abz"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(and (like ?s "ab%") (not-like ?s "ab"))]] db))))
      (is (= #{"abc" "abcd" "abcx" "abz"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(and (not-like ?s "ab") (like ?s "ab%"))]] db))))
      (is (= #{"abc"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(and (like ?s "abc") (like ?s "ab%"))]] db))))
      (is (= #{"abz"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(and (like ?s "ab%") (not-like ?s "abc%")
                          (not-like ?s "ab"))]] db))))
      (is (= #{"abc" "a%" "%c"}
             (set (d/q '[:find [?s ...] :where [?e :text ?s]
                         [(like "abc" ?s)]] db)))))))
