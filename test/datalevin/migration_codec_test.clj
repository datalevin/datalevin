(ns datalevin.migration-codec-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.constants :as c]
            [datalevin.core :as d]
            [datalevin.interface :as i]
            [datalevin.migrate :as m]
            [datalevin.util :as u])
  (:import [java.util Arrays UUID]))

(defn- create-old-store! [dir mixed?]
  (let [jar (m/ensure-jar 0 9 27)
        code (str
               "(require '[datalevin.core :as d] '[datalevin.bits :as b])"
               (pr-str
                 (list 'let ['dir dir 'mixed? mixed?]
                       '(when mixed?
                          (let [conn (d/get-conn dir)]
                            (d/transact! conn [{:person/name "Ada"}])
                            (d/close conn)))
                       '(let [kv (d/open-kv dir)]
                          (try
                            (doseq [dbi ["data" "typed" "raw"]] (d/open-dbi kv dbi))
                            (d/open-list-dbi kv "dups")
                            (d/open-list-dbi kv "raw-list")
                            (d/transact-kv kv
                              [[:put "data" "Hello" "Datalevin"]
                               [:put "data" [:app/key 1] {:value "nested"}]
                               [:put "typed" 1 "one" :long :string]
                               [:put "typed" "Hello" "typed" :string :string]
                               [:put "raw" (b/serialize "Hello")
                                (b/serialize "Datalevin") :raw :raw]])
                            (d/put-list-items kv "dups" "Hello" ["one" "two"] :data :data)
                            (d/put-list-items kv "raw-list" (b/serialize "Hello")
                                              [(b/serialize "Datalevin")] :raw :raw)
                            (finally (d/close-kv kv)))))))]
    (m/run-cmd (into ["java"]
                    (concat m/java-opts ["-cp" jar "clojure.main" "-e" code])))))

(deftest migration-reencodes-nippy-index-entries
  (binding [c/*db-background-sampling?* false]
    (let [root (u/tmp-dir (str "migration-codec-" (UUID/randomUUID)))
          raw-key (byte-array [105 5 72 101 108 108 111])
          raw-value (byte-array [105 9 68 97 116 97 108 101 118 105 110])
          overrides {"raw" {:key-type :raw :val-type :raw}
                     "raw-list" {:key-type :raw :val-type :raw}}]
      (try
        (doseq [mixed? [false true]]
          (let [dir (str root "/" mixed?)]
            (create-old-store! dir mixed?)
            (let [kv (d/open-kv dir {:migration-kv-types overrides})]
              (try
                (doseq [dbi ["data" "typed" "raw"]] (d/open-dbi kv dbi))
                (doseq [dbi ["dups" "raw-list"]] (d/open-list-dbi kv dbi))
                (is (= "Datalevin" (d/get-value kv "data" "Hello")))
                (is (= {:value "nested"} (d/get-value kv "data" [:app/key 1])))
                (is (= "one" (d/get-value kv "typed" 1 :long :string)))
                (is (= "typed" (d/get-value kv "typed" "Hello" :string :string)))
                (is (= ["one" "two"] (vec (d/get-list kv "dups" "Hello" :data :data))))
                (is (i/in-list? kv "dups" "Hello" "two" :data :data))
                (is (Arrays/equals raw-value ^bytes (d/get-value kv "raw" raw-key :raw :raw)))
                (is (i/in-list? kv "raw-list" raw-key raw-value :raw :raw))
                (is (nil? (:migration-kv-types (i/env-opts kv))))
                ;; New writes and deletes must address the migrated entries.
                (d/transact-kv kv [[:put "data" "Hello" "updated"]])
                (is (= 2 (d/entries kv "data")))
                (d/transact-kv kv [[:del "data" [:app/key 1]]])
                (is (= 1 (d/entries kv "data")))
                (d/del-list-items kv "dups" "Hello" ["one"] :data :data)
                (is (= ["two"] (vec (d/get-list kv "dups" "Hello" :data :data))))
                (finally (d/close-kv kv))))
            (let [kv (d/open-kv dir)]
              (try
                (d/open-dbi kv "data")
                (is (= "updated" (d/get-value kv "data" "Hello")))
                (finally (d/close-kv kv))))
            (when mixed?
              (let [conn (d/get-conn dir)]
                (try
                  (is (= "Ada" (d/q '[:find ?name . :where [_ :person/name ?name]] @conn)))
                  (finally (d/close conn)))))))
        (finally (u/delete-files root))))))
