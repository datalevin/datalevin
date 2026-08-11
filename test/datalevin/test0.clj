(ns datalevin.test0
  "tests for core operations"
  (:require
   [clojure.test :as t]
   datalevin.test.bits
   datalevin.test.core
   datalevin.test.components
   datalevin.test.csv
   datalevin.test.datafy
   datalevin.test.db
   datalevin.test.entity
   datalevin.test.ident
   datalevin.test.index
   datalevin.test.listen
   datalevin.test.lru
   datalevin.test.migrate
   datalevin.test.spill
   datalevin.test.transact
   datalevin.test.tuples
   datalevin.test.upsert
   datalevin.test.validation)
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.test.bits
          'datalevin.test.core
          'datalevin.test.components
          'datalevin.test.csv
          'datalevin.test.datafy
          'datalevin.test.db
          'datalevin.test.entity
          'datalevin.test.ident
          'datalevin.test.index
          'datalevin.test.listen
          'datalevin.test.lru
          'datalevin.test.migrate
          'datalevin.test.spill
          'datalevin.test.transact
          'datalevin.test.tuples
          'datalevin.test.upsert
          'datalevin.test.validation)]
    (System/exit (if (zero? ^long (+ ^long fail ^long error)) 0 1))))

(defn -main [& _args]
  (println "clojure version" (clojure-version))
  (println "java version" (System/getProperty "java.version"))
  (println
    "running native?"
    (= "executable" (System/getProperty "org.graalvm.nativeimage.kind")))
  (test-clj))
