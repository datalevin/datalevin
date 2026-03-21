(ns datalevin.ha-test
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.ha :as ha]
   [datalevin.ha.util :as hu]))

(deftest saturated-long-add-clamps-positive-overflow
  (is (= Long/MAX_VALUE
         (hu/saturated-long-add (- Long/MAX_VALUE 5) 10))))

(deftest lease-expiry-clamps-large-deadlines
  (is (not (#'datalevin.ha/ha-lease-expired-for-promotion?
            {:ha-clock-skew-budget-ms 10}
            {:lease-until-ms (- Long/MAX_VALUE 5)}
            0)))
  (is (#'datalevin.ha/ha-lease-expired-for-promotion?
       {:ha-clock-skew-budget-ms 10}
       {:lease-until-ms (- Long/MAX_VALUE 5)}
       Long/MAX_VALUE)))
