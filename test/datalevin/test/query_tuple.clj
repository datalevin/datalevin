;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.test.query-tuple
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.parser :as dp]
   [datalevin.query.tuple :as qtuple]))

(deftest test-needed-tuple-indices
  (let [projection
        (qtuple/tuple-binding-projection
          (dp/parse-binding '[?e _ ?v]))]
    (is (= ['?e '?v] (:cols projection)))
    (is (= {'?e 0 '?v 1} (:attrs projection)))
    (is (= {'?e 0 '?v 2} (:source-attrs projection)))
    (is (= 3 (:source-width projection)))
    (is (= 2 (:output-width projection)))
    (is (= [0 2] (vec (:needed projection)))))
  (is (= [0 2]
         (vec (qtuple/needed-indices
                (dp/parse-binding '[?e _ ?v])))))
  (is (nil? (qtuple/needed-indices
              (dp/parse-binding '[?e ?a ?v]))))
  (is (nil?
        (qtuple/tuple-binding-projection
          (dp/parse-binding '[?e ?e])))))

(deftest test-access-tuple-emitters-preserve-projected-columns
  (let [aid->attr {7 :body}
        doc-ref   [42 7 "value"]
        datom     (qtuple/make-datom-emitter
                    nil aid->attr (int-array [0 2]))
        fulltext  (qtuple/make-fulltext-emitter
                    nil aid->attr :refs+scores (int-array [0 3]))
        vector    (qtuple/make-vector-emitter
                    nil aid->attr :refs+dists (int-array [1 3]))]
    (is (= [42 "value"] (vec (datom doc-ref))))
    (is (= [42 0.75] (vec (fulltext [doc-ref 0.75]))))
    (is (= [:body 0.25] (vec (vector [doc-ref 0.25]))))))

(deftest test-rich-giant-reference-can-use-provided-value
  (let [emit    (qtuple/make-datom-emitter
                  nil {7 :body} (int-array [0 1 2]))
        doc-ref [:g 99 42 7]
        value   (apply str (repeat 700 "x"))]
    ;; Rich giant references carry e/a, and verified idoc paths can supply v;
    ;; no LMDB lookup should be required for this projection.
    (is (= [42 :body value]
           (vec (emit doc-ref value))))))
