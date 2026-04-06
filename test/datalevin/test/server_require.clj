(ns datalevin.test.server-require
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.server]
   [datalevin.server.dispatch]
   [datalevin.server.ha]))

(deftest server-namespaces-load-test
  (is (some? (resolve 'datalevin.server/create)))
  (is (some? (resolve 'datalevin.server.dispatch/dispatch-message-with-ha-write-admission)))
  (is (some? (resolve 'datalevin.server.ha/ha-write-commit-publish-fn))))
