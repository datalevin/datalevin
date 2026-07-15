(ns datalevin.test.migrate
  (:require
   [clojure.test :refer [deftest is]]
   [datalevin.core :as d]
   [datalevin.interface :as if]
   [datalevin.migrate :as m]
   [datalevin.util :as u]
   [taoensso.nippy :as nippy])
  (:import
   [java.io ByteArrayInputStream ByteArrayOutputStream DataOutputStream]
   [java.util UUID]))

(deftest load-schema-first-migration-stream
  (let [dir    (u/tmp-dir (str "migration-stream-" (UUID/randomUUID)))
        bytes  (ByteArrayOutputStream.)
        schema {:person/name {:db/valueType :db.type/string}}
        calls  (atom 0)
        analyze @m/analyze-db]
    (try
      (with-open [out (DataOutputStream. bytes)]
        (nippy/freeze-to-out!
          out {:format          m/mixed-stream-format
               :opts            {}
               :schema          schema
               :source-count    2
               :kv-dbis         []
               :kv-source-count 0})
        (nippy/freeze-to-out!
          out [[1 :person/name "Alice"] [2 :person/name "Bob"]])
        (nippy/freeze-to-out!
          out {:frame :datalog-end :datom-count 2})
        (nippy/freeze-to-out! out {:frame :end :entry-count 0}))
      (with-redefs [m/analyze-db
                    (delay (fn [db]
                             (swap! calls inc)
                             (analyze db)))]
        (is (= {:source-count    2
                :dump-count      2
                :loaded-count    2
                :kv-source-count 0
                :kv-dump-count   0
                :kv-loaded-count 0}
               (#'m/load-datalog-stream
                 dir (ByteArrayInputStream. (.toByteArray bytes))))))
      (is (= 1 @calls))
      (let [conn (d/get-conn dir)]
        (try
          (is (= #{["Alice"] ["Bob"]}
                 (d/q '[:find ?name :where [_ :person/name ?name]]
                      (d/db conn))))
          (finally
            (d/close conn))))
      (finally
        (u/delete-files dir)))))

(deftest load-mixed-migration-stream
  (let [source-dir (u/tmp-dir (str "migration-mixed-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "migration-mixed-target-"
                                   (UUID/randomUUID)))
        bytes      (ByteArrayOutputStream.)
        source     (d/open-kv source-dir)
        schema     {:person/name {:db/valueType :db.type/string}}]
    (try
      (d/open-dbi source "app-state")
      (d/open-dbi source "tags" {:flags #{:create :dupsort}})
      (d/transact-kv
        source
        [[:put "app-state" "theme" "dark" :string :string]
         [:put "tags" "role" "admin" :string :string]
         [:put "tags" "role" "author" :string :string]])
      (let [dbis (mapv (fn [dbi]
                         {:dbi     dbi
                          :entries (d/entries source dbi)
                          :opts    (if/dbi-opts source dbi)})
                       ["app-state" "tags"])]
        (with-open [out (DataOutputStream. bytes)]
          (nippy/freeze-to-out!
            out {:format          m/mixed-stream-format
                 :opts            {}
                 :schema          schema
                 :source-count    1
                 :kv-dbis         dbis
                 :kv-source-count 3})
          (nippy/freeze-to-out! out [[1 :person/name "Ada"]])
          (nippy/freeze-to-out!
            out {:frame :datalog-end :datom-count 1})
          (doseq [{:keys [dbi entries] :as dbi-info} dbis]
            (nippy/freeze-to-out! out (assoc dbi-info :frame :dbi))
            (with-open [items (d/range-seq source dbi [:all] :raw :raw)]
              (nippy/freeze-to-out! out (vec (seq items))))
            (nippy/freeze-to-out!
              out {:frame :dbi-end :dbi dbi :entry-count entries}))
          (nippy/freeze-to-out! out {:frame :end :entry-count 3})))
      (d/close-kv source)
      (is (= {:source-count    1
              :dump-count      1
              :loaded-count    1
              :kv-source-count 3
              :kv-dump-count   3
              :kv-loaded-count 3}
             (#'m/load-datalog-stream
               target-dir (ByteArrayInputStream. (.toByteArray bytes)))))
      (let [conn (d/get-conn target-dir)]
        (try
          (is (= #{["Ada"]}
                 (d/q '[:find ?name :where [_ :person/name ?name]] @conn)))
          (let [kv (d/datalog-kv conn)]
            (d/open-dbi kv "app-state")
            (d/open-dbi kv "tags")
            (is (= "dark"
                   (d/get-value kv "app-state" "theme"
                                :string :string true)))
            (is (= ["admin" "author"]
                   (d/get-list kv "tags" "role" :string :string))))
          (finally
            (d/close conn))))
      (finally
        (when-not (d/closed-kv? source)
          (d/close-kv source))
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest load-kv-migration-stream
  (let [source-dir (u/tmp-dir (str "migration-kv-source-"
                                   (UUID/randomUUID)))
        target-dir (u/tmp-dir (str "migration-kv-target-"
                                   (UUID/randomUUID)))
        bytes      (ByteArrayOutputStream.)
        source     (d/open-kv source-dir)]
    (try
      (d/open-dbi source "items")
      (d/open-dbi source "tags" {:flags #{:create :dupsort}})
      (d/transact-kv
        source
        [[:put "items" 1 "one" :long :string]
         [:put "items" 2 "two" :long :string]
         [:put "tags" "color" "red" :string :string]
         [:put "tags" "color" "blue" :string :string]])
      (let [dbis (mapv (fn [dbi]
                         {:dbi     dbi
                          :entries (d/entries source dbi)
                          :opts    (if/dbi-opts source dbi)})
                       ["items" "tags"])]
        (with-open [out (DataOutputStream. bytes)]
          (nippy/freeze-to-out!
            out {:format       m/kv-stream-format
                 :opts         (if/env-opts source)
                 :dbis         dbis
                 :source-count 4})
          (doseq [{:keys [dbi entries] :as dbi-info} dbis]
            (nippy/freeze-to-out! out (assoc dbi-info :frame :dbi))
            (with-open [items (d/range-seq source dbi [:all] :raw :raw)]
              (nippy/freeze-to-out! out (vec (seq items))))
            (nippy/freeze-to-out!
              out {:frame :dbi-end :dbi dbi :entry-count entries}))
          (nippy/freeze-to-out! out {:frame :end :entry-count 4})))
      (d/close-kv source)
      (is (= {:source-count 4 :dump-count 4 :loaded-count 4}
             (#'m/load-kv-stream
               target-dir (ByteArrayInputStream. (.toByteArray bytes)))))
      (let [target (d/open-kv target-dir)]
        (try
          (d/open-dbi target "items")
          (d/open-dbi target "tags")
          (is (= #{"items" "tags"} (set (d/list-dbis target))))
          (is (= ["one" "two"]
                 [(d/get-value target "items" 1 :long :string)
                  (d/get-value target "items" 2 :long :string)]))
          (is (= ["blue" "red"]
                 (d/get-list target "tags" "color" :string :string)))
          (finally
            (d/close-kv target))))
      (finally
        (when-not (d/closed-kv? source)
          (d/close-kv source))
        (u/delete-files source-dir)
        (u/delete-files target-dir)))))

(deftest load-empty-kv-migration-stream
  (let [target-dir (u/tmp-dir (str "migration-kv-empty-"
                                   (UUID/randomUUID)))
        bytes      (ByteArrayOutputStream.)]
    (try
      (with-open [out (DataOutputStream. bytes)]
        (nippy/freeze-to-out!
          out {:format       m/kv-stream-format
               :opts         {}
               :dbis         []
               :source-count 0})
        (nippy/freeze-to-out! out {:frame :end :entry-count 0}))
      (is (= {:source-count 0 :dump-count 0 :loaded-count 0}
             (#'m/load-kv-stream
               target-dir (ByteArrayInputStream. (.toByteArray bytes)))))
      (let [target (d/open-kv target-dir)]
        (try
          (is (empty? (d/list-dbis target)))
          (finally
            (d/close-kv target))))
      (finally
        (u/delete-files target-dir)))))
