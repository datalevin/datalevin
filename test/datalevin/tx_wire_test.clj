(ns datalevin.tx-wire-test
  (:require [clojure.test :refer [deftest is]]
            [datalevin.db.tx.template :as template]
            [datalevin.db.tx.wire :as wire]
            [datalevin.prepare :as prepare]))

(def context (prepare/->PrepareCtx {} {} {}))

(deftest parameter-paths-preserve-literal-nils-nesting-and-annotations
  (let [p template/parameter
        source (with-meta [:db/add [:key (p :id)] :value {:nested [(p :value) nil]}] {:source :kept})
        definition (wire/definition [source] nil)
        descriptor (first (:operations definition))
        restored (wire/restore-operation descriptor)
        a (template/prepare-template context [source] nil)
        b (template/prepare-template context [restored] nil)]
    (is (= [:db/add [:key nil] :value {:nested [nil nil]}] (:form descriptor)))
    (is (= [[[1 1] :id] [[3 :nested 0] :value]] (:parameters descriptor)))
    (is (= (template/materialize a [{:id "one" :value 9}])
           (template/materialize b [{:id "one" :value 9}])))
    (is (= {:source :kept} (meta restored)))))

(deftest shared-operations-have-stable-portable-identities
  (let [operation (wire/operation [:db/add 1 :value (template/parameter :v)])
        a (wire/definition [operation] nil) b (wire/definition [operation [:db/retract 1 :other]] nil)]
    (is (= (:id (first (:operations a))) (:id (first (:operations b)))))
    (is (pos? (:id operation)))
    (is (= [[[:db/add 1 :value 7]]]
           [(wire/materialize (wire/request "db" [operation] nil) {:v 7})]))))

(deftest remote-definitions-reject-unsupported-metadata-before-serialization
  (let [effects (atom [])
        marker (template/parameter :metadata)
        host (proxy [Object] []
               (hashCode [] (swap! effects conj :hash) 0)
               (toString [] (swap! effects conj :string) "host"))
        lazy-value (lazy-seq (swap! effects conj :realized) (list 7))]
    (doseq [value [marker host lazy-value]
            form [(with-meta [:db/add 1 :value 7] {:value value})
                  [:db/add 1 :value (with-meta {:nested 7} {:value value})]]
            prepare [#(wire/operation form)
                     #(wire/definition [form] nil)
                     #(wire/request "db" [form] nil)]]
      (let [failure (try (prepare) nil (catch clojure.lang.ExceptionInfo e e))]
        (is (= :prepared/metadata (:error (ex-data failure))))))
    (let [operations (with-meta [[:db/add 1 :value 7]] {:value marker})]
      (is (thrown? clojure.lang.ExceptionInfo (wire/definition operations nil)))
      (is (thrown? clojure.lang.ExceptionInfo (wire/request "db" operations nil))))
    (is (empty? @effects))))

(deftest preparing-a-wire-definition-does-not-serialize-host-values
  (let [calls (atom []) host (proxy [Object] []
                                (hashCode [] (swap! calls conj :hash) 0)
                                (toString [] (swap! calls conj :string) "host"))
        request (wire/request "db" [[:db/add 1 :value host]] nil)]
    (is (not (realized? (::wire/definition-hash (.-metadata ^datalevin.prepared.Request request)))))
    (is (empty? @calls))))

(deftest invalid-wire-paths-do-not-visit-host-containers
  (let [calls (atom []) host (proxy [Object clojure.lang.ILookup] []
                                (valAt ([key] (swap! calls conj key) nil)
                                       ([key default] (swap! calls conj key) default)))]
    (doseq [path [[3 :host] [] [99] [1 :invalid]]]
      (is (thrown? Exception (wire/restore-operation {:form [:db/add 1 :value host] :parameters [[path :v]]}))))
    (is (empty? @calls))))

(deftest remote-definition-validation-matches-local-parameter-contracts
  (is (thrown? Exception (wire/definition [[:db/add 1 :value #{(template/parameter :v)}]] nil)))
  (is (thrown? Exception (wire/request "db" [] nil)))
  (is (thrown? Exception (wire/request "db" [[:db/add 1 :value 0]] {:unknown true})))
  (let [request (wire/request "db" [[:db/add (template/parameter :e) :value 1]] {:batch? true})]
    (is (thrown? Exception (wire/input-rows request [{}])))
    (is (= [] (wire/input-rows request [])))))
