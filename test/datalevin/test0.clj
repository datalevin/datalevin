(ns datalevin.test0
  "tests for core operations"
  (:require
   [clojure.test :as t]
   datalevin.test.core
   datalevin.test.components
   datalevin.test.conn
   datalevin.test.db
   datalevin.test.entity
   datalevin.test.explode
   datalevin.test.ident
   datalevin.test.index
   datalevin.test.listen
   datalevin.test.lookup-refs
   datalevin.test.lru
   datalevin.test.parser
   datalevin.test.parser-find
   datalevin.test.parser-rules
   datalevin.test.parser-query
   datalevin.test.parser-where
   datalevin.test.pull-api
   datalevin.test.pull-parser
   datalevin.test.query
   datalevin.test.query-aggregates
   datalevin.test.query-find-specs
   datalevin.test.query-fns
   datalevin.test.query-not
   datalevin.test.query-or
   datalevin.test.query-pull
   datalevin.test.query-rules
   datalevin.test.tuples
   datalevin.test.validation
   datalevin.test.upsert
   datalevin.test.issues
   datalevin.test.transact
   datalevin.lmdb-test
   datalevin.scan-test
   datalevin.storage-test
   datalevin.query-test
   datalevin.remote-test
   datalevin.search-test
   datalevin.vector-test
   datalevin.idoc-test
   datalevin.main-test
   datalevin.core-test
   )
  (:gen-class))

(defn ^:export test-clj []
  (let [{:keys [fail error]}
        (t/run-tests
          'datalevin.test.core
          'datalevin.test.components
          'datalevin.test.conn
          'datalevin.test.db
          'datalevin.test.entity
          'datalevin.test.explode
          'datalevin.test.ident
          'datalevin.test.index
          'datalevin.test.listen
          'datalevin.test.lookup-refs
          'datalevin.test.lru
          'datalevin.test.parser
          'datalevin.test.parser-find
          'datalevin.test.parser-rules
          'datalevin.test.parser-query
          'datalevin.test.parser-where
          'datalevin.test.pull-api
          'datalevin.test.pull-parser
          'datalevin.test.query
          'datalevin.test.query-aggregates
          'datalevin.test.query-find-specs
          'datalevin.test.query-fns
          'datalevin.test.query-not
          'datalevin.test.query-or
          'datalevin.test.query-pull
          'datalevin.test.query-rules
          'datalevin.test.tuples
          'datalevin.test.validation
          'datalevin.test.upsert
          'datalevin.test.issues
          'datalevin.test.transact
          'datalevin.lmdb-test
          'datalevin.scan-test
          'datalevin.storage-test
          'datalevin.query-test
          'datalevin.remote-test
          'datalevin.search-test
          'datalevin.vector-test
          'datalevin.idoc-test
          'datalevin.main-test
          'datalevin.core-test)]
    (System/exit (if (zero? ^long (+ ^long fail ^long error)) 0 1))))

(defn -main [& _args]
  (println "clojure version" (clojure-version))
  (println "java version" (System/getProperty "java.version"))
  (println
    "running native?"
    (= "executable" (System/getProperty "org.graalvm.nativeimage.kind")))
  (test-clj))
