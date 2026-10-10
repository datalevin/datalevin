(ns datalevin.tx-group-batch-collection-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.tx-group.batch :as batch]
            [datalevin.tx-group.batch.charge :as charge])
  (:import [java.util.concurrent.atomic AtomicInteger]))

(defn- echo-values [b]
  (into-array Object
              (map #(batch/data (batch/batch-at b %))
                   (range (batch/batch-count b)))))

(defn- queued-descriptor
  ^datalevin.tx_group.batch.Descriptor [collector value deadline]
  (#'batch/admit! collector 1024 deadline)
  (batch/->Descriptor nil (volatile! value) nil 1024 deadline 1024
                      (AtomicInteger. 0) (volatile! nil) (Thread/currentThread)))

(deftest full-collection-leaves-tail-expiry-to-next-sealing
  (doseq [limit [1 2]]
    (let [collector (batch/create echo-values
                                  {:limits (charge/resolve-limits
                                             {:write-batch-size limit})})
          owner (queued-descriptor collector :owner 0)
          expired-prefix (when (= limit 2)
                           (queued-descriptor collector :expired-prefix 1))
          added (when (= limit 2) (queued-descriptor collector :added 0))
          tail (queued-descriptor collector :tail 0)
          expired-tail (queued-descriptor collector :expired-tail 1)]
      (#'batch/publish-and-elect! collector owner)
      (let [selected (#'batch/claim-next-batch! collector owner)]
        (doseq [d (concat (when (= limit 2) [expired-prefix added])
                         [tail expired-tail])]
          (#'batch/publish-and-elect! collector d))
        (is (= (dec limit) (batch/collect-ready! selected)))
        (is (= limit (batch/batch-count selected)))
        (when (= limit 2)
          (is (= :txlog/write-deadline-exceeded
                 (:error (ex-data (second @(.result expired-prefix)))))))
        (is (nil? @(.result expired-tail)))
        (is (= 0 (batch/collect-ready! selected)))
        (is (nil? @(.result expired-tail)))
        (is (false? (batch/selected? tail)))
        (#'batch/run-sealed-batch! collector selected))
      (is (true? (#'batch/try-lead! collector tail)))
      (#'batch/lead! collector tail)
      (is (= [true :tail] @(.result tail)))
      (is (= :txlog/write-deadline-exceeded
             (:error (ex-data (second @(.result expired-tail))))))
      (doseq [d (remove nil? [owner expired-prefix added tail expired-tail])]
        (#'batch/release-descriptor! collector d))
      (is (batch/serving? collector))
      (is (zero? (:requests (batch/usage collector))))
      (is (zero? (:bytes (batch/usage collector)))))))
