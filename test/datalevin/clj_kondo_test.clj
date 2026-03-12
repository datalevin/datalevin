(ns datalevin.clj-kondo-test
  (:require
   [clj-kondo.hooks-api :as api]
   [clojure.edn :as edn]
   [clojure.test :refer [deftest is]]))

(load-file "resources/.clj-kondo/datalevin/datalevin/hooks/datalevin.clj")

(def ^:private export-config-path
  "resources/.clj-kondo/datalevin/datalevin/config.edn")

(defn- run-hook
  [hook-var form]
  (-> {:node (api/coerce form)}
      hook-var
      :node
      api/sexpr))

(deftest exported-config-test
  (let [config (edn/read-string (slurp export-config-path))]
    (is (= 'hooks.datalevin/q
           (get-in config [:hooks :analyze-call 'datalevin.core/q])))
    (is (= 'hooks.datalevin/explain
           (get-in config [:hooks :analyze-call 'datalevin.core/explain])))
    (is (= 'datascript.core/pull
           (get-in config [:lint-as 'datalevin.core/pull])))
    (is (= 'datascript.core/pull-many
           (get-in config [:lint-as 'datalevin.core/pull-many])))))

(deftest q-hook-normalizes-extended-query-syntax
  (is
    (= '(datascript.core/q
          '[:find ?e ?name
            :in $ ?pattern
            :where
            [(ground [[nil nil nil]]) [[?e _ _]]]
            [?e :person/name ?name]
            [(vector ?name "%Smith")]
            [(vector ?doc :name) ?name]]
          db
          "%Smith")
       (run-hook #'hooks.datalevin/q
                 '(datalevin.core/q
                    '[:find ?e ?name
                      :in $ ?pattern
                      :where
                      [(fulltext $ ?pattern {:top 1}) [[?e _ _]]]
                      [?e :person/name ?name]
                      [(like ?name "%Smith")]
                      [(idoc-get ?doc :name) ?name]
                      :order-by [?name :desc]
                      :limit 10
                      :offset 5
                      :timeout 1000]
                    db
                    "%Smith")))))

(deftest explain-hook-normalizes-map-queries
  (is
    (= '(datascript.core/q
          '{:find [?e]
            :where [[(ground [[nil nil nil]]) [[?e _ _]]]
                    [(vector ?name "%cat%")]]}
          db
          "cat")
       (run-hook #'hooks.datalevin/explain
                 '(datalevin.core/explain
                    {:run? true}
                    '{:find [?e]
                      :where [[(embedding-neighbors $ "cat" {:top 2}) [[?e _ _]]]
                              [(not-like ?name "%cat%")]]
                      :having [(> ?score 0.5)]
                      :order-by [?e]
                      :limit 1
                      :timeout 5000}
                    db
                    "cat")))))
