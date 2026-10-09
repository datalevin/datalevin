(ns datalevin.env-flags-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.util :as u]))

(def ^:dynamic *dir* nil)

(use-fixtures :each
  (fn [f]
    (binding [*dir* (u/tmp-dir (str "env-flags-" (random-uuid)))]
      (try (f)
           (finally (u/delete-files *dir*))))))

(deftest native-flag-changes-and-independent-environments
  (let [a (d/open-kv (str *dir* "/a") {:wal? false})
        b (d/open-kv (str *dir* "/b") {:wal? false})]
    (try
      (let [original-a (i/get-env-flags a)
            original-b (i/get-env-flags b)]
        ;; Populate the decoder, then change flags in opposite directions.
        ;; Repeated reads must still reflect the current native environment.
        (doseq [enabled? [true false true false]]
          (i/set-env-flags a #{:nosync} enabled?)
          (i/set-env-flags b #{:nosync} (not enabled?))
          (let [expected-a ((if enabled? conj disj) original-a :nosync)
                expected-b ((if enabled? disj conj) original-b :nosync)
                reads (mapv (fn [_]
                              (future
                                (every? true?
                                        (for [_ (range 100)]
                                          (and (= expected-a (i/get-env-flags a))
                                               (= expected-b (i/get-env-flags b)))))))
                            (range 4))]
            (doseq [result reads]
              (is (true? (deref result 10000 ::timeout)))))))
      (finally (d/close-kv b) (d/close-kv a)))))
