(ns datalevin.prepare-coercion-test
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.prepare :as prepare]))

(deftest canonical-scalar-values-retain-their-boxes
  (doseq [[vt value] [[:db.type/long (Long/valueOf 123456)]
                      [:db.type/ref (Long/valueOf 123456)]
                      [:db.type/float (Float/valueOf (float 1.25))]
                      [:db.type/double (Double/valueOf 1.25)]
                      [:db.type/string (String. "already a string")]]]
    (testing (str vt)
      (is (identical? value (prepare/type-coercion vt value)))
      (doseq [validate? [false true]]
        (is (identical? value (prepare/correct-scalar-value validate? vt value)))
        (is (identical? value
                       (prepare/correct-value-with-props
                         {:validate-data? validate?} {:db/valueType vt} :value value)))))))

(deftest scalar-coercions-still-produce-the-declared-class
  (doseq [[vt value expected] [[:db.type/long (int 123456) (long 123456)]
                               [:db.type/ref (int 123456) (long 123456)]
                               [:db.type/long 123456.75 (long 123456)]
                               [:db.type/float 123456 (float 123456)]
                               [:db.type/float 1.25 (float 1.25)]
                               [:db.type/double (float 1.25) 1.25]
                               [:db.type/double 123456 (double 123456)]
                               [:db.type/string 123456 "123456"]
                               [:db.type/string (StringBuilder. "text") "text"]]]
    (doseq [actual [(prepare/type-coercion vt value)
                   (prepare/correct-scalar-value false vt value)
                   (prepare/correct-value-with-props {} {:db/valueType vt} :value value)]]
      (is (= expected actual))
      (is (= (class expected) (class actual))))))

(deftest canonical-floating-point-validation-retains-range-checks
  (doseq [[vt values] [[:db.type/float [(float 0.0) (float -0.0)
                                        Float/MIN_VALUE Float/MAX_VALUE
                                        (float (- Float/MAX_VALUE))]]
                       [:db.type/double [0.0 -0.0 Double/MIN_VALUE Double/MAX_VALUE
                                         (- Double/MAX_VALUE)]]]
          value values]
    (is (identical? value (prepare/correct-scalar-value true vt value)))
    (is (identical? value
                   (prepare/correct-value-with-props
                     {:validate-data? true} {:db/valueType vt} :value value))))
  (doseq [[vt values] [[:db.type/float [Float/NaN Float/POSITIVE_INFINITY
                                        Float/NEGATIVE_INFINITY Double/MAX_VALUE]]
                       [:db.type/double [Double/NaN Double/POSITIVE_INFINITY
                                         Double/NEGATIVE_INFINITY]]]
          value values]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid data"
          (prepare/correct-scalar-value true vt value)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid data"
          (prepare/correct-value-with-props
            {:validate-data? true} {:db/valueType vt} :value value)))))

(deftest noncanonical-values-retain-validation-before-coercion
  (doseq [[vt value] [[:db.type/long 123456.75]
                      [:db.type/ref 123456.75]
                      [:db.type/string 123456]]]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid data"
          (prepare/correct-scalar-value true vt value)))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Invalid data"
          (prepare/correct-value-with-props
            {:validate-data? true} {:db/valueType vt} :value value)))))

(deftest validation-still-accepts-compatible-numeric-classes
  (doseq [[vt value expected] [[:db.type/long (int 123456) (long 123456)]
                               [:db.type/long (short 123) (long 123)]
                               [:db.type/ref (byte 123) (long 123)]
                               [:db.type/float 1.25 (float 1.25)]
                               [:db.type/double (float 1.25) 1.25]]]
    (doseq [actual [(prepare/correct-scalar-value true vt value)
                   (prepare/correct-value-with-props
                     {:validate-data? true} {:db/valueType vt} :value value)]]
      (is (= expected actual))
      (is (= (class expected) (class actual))))))
