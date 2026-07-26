;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.query-access-function
  (:require
   [clojure.test :refer [deftest is testing]]
   [datalevin.parser :as dp]
   [datalevin.query.access :as qaccess]
   [datalevin.query.access.function :as qfunction]))

(deftest test-compile-input-bound-function-access
  (let [source (Object.)
        parsed (dp/parse-query
                 '[:find ?e ?score
                   :in $search ?query
                   :where
                   [(fulltext $search ?query
                              {:display :refs+scores
                               :offset 2
                               :limit 5})
                    [[?e _ _ ?score]]]])
        spec   (qfunction/compile-function-access
                 {:parsed-q parsed
                  :inputs   [source "red"]}
                 0
                 #{'fulltext})
        expr   (qfunction/access-expr spec :fulltext #{'?e})]
    (is (= 'fulltext (:function spec)))
    (is (= '$search (:source spec)))
    (is (identical? source (:source-value spec)))
    (is (= {'?query "red"} (:input-values spec)))
    (is (= #{} (:requires spec)))
    (is (= #{'?e '?score} (:produces spec)))
    (is (= ['?e '?score] (:cols spec)))
    (is (= [0 3] (vec (get-in spec [:projection :needed]))))
    (is (= ["red" {:display :refs+scores :offset 2 :limit 5}]
           (qfunction/resolve-arguments spec)))
    (is (= '$search (:source expr)))
    (is (= #{(:clause spec)} (:covers expr)))
    (is (= #{(:original-clause spec)}
           (:covered-originals expr)))))

(deftest test-compile-correlated-function-access
  (let [source (Object.)
        parsed (dp/parse-query
                 '[:find ?e
                   :in $docs
                   :where
                   [$docs ?seed :query ?query]
                   [(idoc-match $docs :doc ?query)
                    [[?e _ ?doc]]]])
        specs  (qfunction/compile-function-accesses
                 {:parsed-q parsed :inputs [source]}
                 #{'idoc-match})
        spec   (first specs)]
    (is (= 1 (count specs)))
    (is (= 1 (:clause-idx spec)))
    (is (= #{'?query} (:requires spec)))
    (is (= [:doc {:status "active"}]
           (qfunction/resolve-arguments
             spec {'?query {:status "active"}})))
    (testing "missing correlated arguments are rejected"
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo
            #"not bound"
            (qfunction/resolve-arguments spec))))))

(deftest test-function-access-rejects-unsupported-shapes
  (let [source (Object.)
        scalar (dp/parse-query
                 '[:find ?x
                   :in $db
                   :where
                   [(fulltext $db "red") ?x]])
        unnamed (dp/parse-query
                  '[:find ?e
                    :in $db ?search
                    :where
                    [(?search $db "red") [[?e _ _]]]])]
    (is (nil? (qfunction/compile-function-access
                {:parsed-q scalar :inputs [source]}
                0 #{'fulltext})))
    (is (nil? (qfunction/compile-function-access
                {:parsed-q unnamed :inputs [source identity]}
                0 #{'fulltext})))))

(deftest test-function-access-dispatch-compiles-once
  (let [source  (Object.)
        parsed  (dp/parse-query
                  '[:find ?fulltext-e ?idoc-e ?vector-e
                    :in $db ?query ?query-vector
                    :where
                    [(fulltext $db ?query) [[?fulltext-e _ _]]]
                    [(idoc-match $db :doc {:active true})
                     [[?idoc-e _ _]]]
                    [(vec-neighbors $db :embedding ?query-vector)
                     [[?vector-e _ _]]]])
        forced  (atom 0)
        calls   (atom [])
        backend (fn [kind]
                  (reify
                    qfunction/IFunctionAccessBackend
                    (-function-access-plans [_ _ spec]
                      (swap! calls conj [kind (:function spec)])
                      [{:kind kind :function (:function spec)}])))
        method  (qfunction/access-method
                  {:fulltext (backend :fulltext)
                   :idoc     (backend :idoc)
                   :vector   (backend :vector)})
        context {:parsed-q parsed
                 :inputs [source "red" [0.1 0.2]]
                 :input-values
                 (delay
                   (swap! forced inc)
                   (qaccess/scalar-input-values
                     parsed [source "red" [0.1 0.2]]))}
        plans   (qaccess/access-plans [method] context)]
    (is (= 1 @forced))
    (is (= 3 (count plans)))
    (is (= #{[:fulltext 'fulltext]
             [:idoc 'idoc-match]
             [:vector 'vec-neighbors]}
           (set @calls)))))

(deftest test-function-dispatch-leaves-unused-input-cache-lazy
  (let [source (Object.)
        parsed (dp/parse-query
                 '[:find ?e
                   :in $db
                   :where
                   [$db ?e :name "Ada"]])
        forced (atom 0)
        backend
        (reify
          qfunction/IFunctionAccessBackend
          (-function-access-plans [_ _ _]
            (throw (ex-info "Unexpected function access" {}))))
        method (qfunction/access-method {:fulltext backend})]
    (is (empty?
          (qaccess/access-plans
            [method]
            {:parsed-q parsed
             :inputs [source]
             :input-values
             (delay
               (swap! forced inc)
               (qaccess/scalar-input-values parsed [source]))})))
    (is (zero? @forced))))
