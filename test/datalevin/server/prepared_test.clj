(ns datalevin.server.prepared-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.bits :as bits]
            [datalevin.db.tx.template :as template]
            [datalevin.db.tx.wire :as wire]
            [datalevin.prepare :as prepare]
            [datalevin.server.prepared :as prepared])
  (:import [java.nio.channels Pipe SelectionKey Selector]))

(def context (prepare/->PrepareCtx {} {} {}))

(defn- with-key [f]
  (let [pipe (Pipe/open)]
    (with-open [selector (Selector/open)
                source (.source pipe)
                _sink (.sink pipe)]
      (.configureBlocking source false)
      (f (.register source selector 0
                    (volatile! {:wire-opts {:prepared-write? true}}))))))

(defn- descriptor [form]
  (assoc (first (:operations (wire/definition [form] nil))) :id 101))

(defn- message [db-name operation]
  {:type :tx-data-prepared
   :args [db-name {:version 1 :operations [operation]} nil]})

(defn- compile! [key db-name operation]
  (prepared/write-template! key (message db-name operation) context))

(defn- definition []
  (descriptor
    (with-meta [:db/add 1 :doc
                (with-meta {:value (template/parameter :v)
                            :bytes (byte-array [1 2 3])} {:nested :kept})]
      {:source :original})))

(deftest matching-decoded-definitions-share-the-compiled-operation
  (with-key
    (fn [^SelectionKey key]
      (let [original (definition)
            decoded (bits/deserialize (bits/serialize original))
            a (compile! key "db" original)
            b (compile! key "db" decoded)
            forms (template/materialize b [{:v 7}])]
        (is (identical? (first (:operations a)) (first (:operations b))))
        (is (= 7 (get-in forms [0 3 :value])))
        (is (= [1 2 3] (vec (get-in forms [0 3 :bytes]))))
        (is (= {:source :original} (meta (first forms))))
        (is (= {:nested :kept} (meta (get-in forms [0 3]))))))))

(deftest conflicting-definitions-cannot-reuse-an-operation-id
  (with-key
    (fn [^SelectionKey key]
      (let [original (definition)
            a (compile! key "db" original)
            changed [(assoc-in original [:form 2] :other)
                     (assoc original :parameters [[[3 :value] :other]])
                     (update original :form with-meta {:source :changed})
                     (update-in original [:form 3] with-meta {:nested :changed})
                     (assoc-in original [:form 3 :bytes] (byte-array [1 2 4]))
                     (assoc original :parameters [[[99] :v]])]]
        (doseq [operation changed]
          (let [failure (try (prepared/write-template!
                              key (assoc (message "db" operation) :prepare-id 202) context)
                             nil
                             (catch clojure.lang.ExceptionInfo e e))]
            (is (= {:error :prepared/operation-conflict :handle 101} (ex-data failure)))
            (is (not (contains? (:prepared-handles @(.attachment key)) 202)))
            (is (identical? (first (:operations a))
                            (first (:operations (compile! key "db" original)))))))))))

(deftest operation-ids-remain-scoped-to-the-database
  (with-key
    (fn [key]
      (let [original (descriptor [:db/add 1 :value (template/parameter :v)])
            other (assoc-in original [:form 2] :other)
            a (compile! key "db" original)
            b (compile! key "other-db" other)]
        (is (= [[:db/add 1 :value 7]] (template/materialize a [{:v 7}])))
        (is (= [[:db/add 1 :other 8]] (template/materialize b [{:v 8}])))
        (is (= [[:db/add 1 :value 9]]
               (template/materialize (compile! key "db" original) [{:v 9}])))))))
