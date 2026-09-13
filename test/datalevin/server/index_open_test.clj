(ns datalevin.server.index-open-test
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [datalevin.client :as client]
   [datalevin.constants :as c]
   [datalevin.core :as d]
   [datalevin.interface :as i]
   [datalevin.server :as server]
   [datalevin.server.api :as api]
   [datalevin.test.core :refer [allocate-port db-fixture]]
   [datalevin.util :as u]
   [datalevin.vector :as v])
  (:import
   [datalevin.cpp VecIdx]
   [datalevin.server Server]
   [datalevin.vector VectorIndex]
   [java.util Map UUID]
   [java.util.concurrent CountDownLatch TimeUnit]))

(use-fixtures :once db-fixture)

(defn- request-error [f]
  (try (f) nil (catch Exception e (:err-data (ex-data e)))))

(deftest second-client-vector-open-preserves-uncheckpointed-vectors-test
  (let [root (u/tmp-dir (str "shared-vector-open-" (UUID/randomUUID)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        ^Map dbs (.-dbs srv)
        db-name "vectors"
        uri (str "dtlv://datalevin:datalevin@localhost:" port "/" db-name)
        opts {:dimensions 2}]
    (try
      (server/start srv)
      (let [kv1 (d/open-kv uri)
            kv2 (d/open-kv uri)]
        (try
          (let [index1 (d/new-vector-index kv1 opts)
                live (:index (.get dbs db-name))
                pending (atom [])]
            ;; Keep the checkpoint empty while the live index contains :one.
            (d/force-vec-checkpoint! index1)
            (with-redefs [v/*submit-async-vec-save* #(swap! pending conj %)]
              (d/add-vec index1 :one [1.0 0.0])
              (is (= 1 (count @pending)))
              (is (= [:one] (d/search-vec index1 [1.0 0.0])))
              (let [index2 (d/new-vector-index kv2 opts)]
                (is (identical? live (:index (.get dbs db-name))))
                (is (i/vec-indexed? index1 :one))
                (is (= [:one] (d/search-vec index1 [1.0 0.0])))
                (is (= [:one] (d/search-vec index2 [1.0 0.0])))
                (d/add-vec index2 :two [0.0 1.0])
                (is (= [:two] (d/search-vec index1 [0.0 1.0] {:top 1})))
                (is (= [:two] (d/search-vec index2 [0.0 1.0] {:top 1})))
                (d/close-vector-index index2)))
            (is (i/vec-closed? live))
            (let [reopened (d/new-vector-index kv1 opts)]
              (try
                (is (not (identical? live (:index (.get dbs db-name)))))
                (is (= 2 (:size (d/vector-index-info reopened))))
                (is (= [:one] (d/search-vec reopened [1.0 0.0] {:top 1})))
                (finally (d/close-vector-index reopened)))))
          (finally
            (d/close-kv kv2)
            (d/close-kv kv1))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest simultaneous-vector-opens-initialize-once-test
  (let [store (reify i/ILMDB
                (env-dir [_] "/tmp/datalevin-vector-open-probe")
                (list-dbis [_] []))
        index (reify i/IVectorIndex (vec-closed? [_] false))
        state (atom {:store store})
        clients (atom {:one {:indices #{}} :two {:indices #{}}})
        attempts (atom 0)
        both-open (CountDownLatch. 2)
        initializing (CountDownLatch. 1)
        second-initialization (CountDownLatch. 1)
        release (CountDownLatch. 1)
        deps {:get-store (fn [_ _] (.countDown both-open) store)
              :db-state (fn [_ _] @state)
              :update-db (fn [_ _ f] (swap! state f))
              :update-client (fn [_ client-id f] (swap! clients update client-id f))
              :write-message (fn [_ message] message)
              :with-index-write-admission
              (fn [_ _ _]
                (when (> (long (swap! attempts inc)) 1)
                  (.countDown second-initialization))
                (.countDown initializing)
                (when-not (.await release 5 TimeUnit/SECONDS)
                  (throw (ex-info "Timed out waiting to initialize index" {})))
                index)}
        open! (fn [client-id]
                (api/new-vector-index deps nil nil client-id
                                      {:args ["db" {:dimensions 2}]}))
        first-open (future (open! :one))]
    (try
      (is (.await initializing 5 TimeUnit/SECONDS))
      (let [second-open (future (open! :two))]
        (try
          (is (.await both-open 5 TimeUnit/SECONDS))
          (is (false? (.await second-initialization 200 TimeUnit/MILLISECONDS)))
          (.countDown release)
          (is (= {:type :command-complete} (deref first-open 5000 ::timeout)))
          (is (= {:type :command-complete} (deref second-open 5000 ::timeout)))
          (is (= 1 @attempts))
          (is (identical? index (:index @state)))
          (is (= {:one {:indices #{"db"}} :two {:indices #{"db"}}} @clients))
          (finally (future-cancel second-open))))
      (finally
        (.countDown release)
        (future-cancel first-open)))))

(deftest remote-index-opens-require-admission-only-for-initialization-test
  (let [root (u/tmp-dir (str "index-open-" (UUID/randomUUID)))
        port (allocate-port)
        ^Server srv (server/create {:root root :port port})
        ^Map dbs (.-dbs srv)]
    (try
      (server/start srv)
      (doseq [[state expected-error]
              [[{:replica/read-only? true} :replica/read-only]
               [{:ha-authority ::authority :ha-role :follower}
                :ha/write-rejected]]
              [db-name op opts read-op]
              [["search" :new-search-engine {:domain "custom-search"} :doc-count]
               ["vector" :new-vector-index
                {:domain "custom-vector" :dimensions 2} :vecs-info]]]
        (testing (str db-name " " expected-error)
          (let [name (str db-name "-" (name expected-error))
                uri (str "dtlv://datalevin:datalevin@localhost:" port "/" name)
                kv (d/open-kv uri)
                c  (client/new-client uri)
                initial (.get dbs name)]
            (try
              (client/disable-ha-write-retry! c)
              (.put dbs name (merge initial state))
              (let [before (set (d/list-dbis kv))]
                (is (= expected-error
                       (:error (request-error
                                #(client/normal-request c op [name opts])))))
                (is (= before (set (d/list-dbis kv)))
                    "rejected opens must not create even one DBI"))
              (.put dbs name initial)
              (if (= read-op :doc-count)
                (d/open-dbi kv (str (:domain opts) "/" c/terms))
                (i/open-list-dbi kv (str (:domain opts) "/" c/vec-refs)
                                 {:key-size c/+max-key-size+
                                  :val-size c/+id-bytes+}))
              (.put dbs name (merge initial state))
              (let [before (set (d/list-dbis kv))]
                (is (= expected-error
                       (:error (request-error
                                #(client/normal-request c op [name opts])))))
                (is (= before (set (d/list-dbis kv)))
                    "partial indexes must not be completed without admission"))
              (.put dbs name initial)
              (client/normal-request c op [name opts])
              (if (= read-op :doc-count)
                (client/normal-request c :add-doc [name :one "sample text"])
                (do
                  (client/normal-request c :add-vec [name :one [1.0 0.0]])
                  (client/normal-request c :persist-vecs [name])))
              (let [opened (.get dbs name)
                    before (set (d/list-dbis kv))
                    fresh (client/new-client uri)]
                (try
                  (.put dbs name (merge opened state))
                  (client/normal-request fresh op [name opts])
                  (is (= before (set (d/list-dbis kv))))
                  (let [result (client/normal-request fresh read-op [name])]
                    (is (= 1 (if (= read-op :doc-count) result (:size result)))))
                  (finally
                    (.put dbs name (apply dissoc (.get dbs name) (keys state)))
                    (client/disconnect fresh))))
              (finally
                (.put dbs name (apply dissoc (.get dbs name) (keys state)))
                (client/disconnect c)
                (d/close-kv kv))))))
      (finally
        (server/stop srv)
        (u/delete-files root)))))

(deftest existing-vector-open-does-not-migrate-legacy-files-test
  (let [dir (u/tmp-dir (str "index-legacy-open-" (UUID/randomUUID)))
        kv (d/open-kv dir)
        opts {:domain "legacy" :dimensions 2}
        ^VectorIndex index (d/new-vector-index kv opts)
        fname (v/index-fname kv "legacy")]
    (try
      (d/add-vec index :one [1.0 0.0])
      (d/force-vec-checkpoint! index)
      (VecIdx/save (.-index index) fname)
      (i/close-vecs index)
      (i/transact-kv kv [[:del c/vec-meta-dbi "legacy" :string]])
      (let [before (set (d/list-dbis kv))]
        (is (nil? (v/open-vector-index kv opts)))
        (is (u/file-exists fname))
        (is (nil? (i/get-value kv c/vec-meta-dbi "legacy" :string :data)))
        (is (= before (set (d/list-dbis kv)))))
      (let [migrated (d/new-vector-index kv opts)]
        (try
          (is (= [:one] (d/search-vec migrated [1.0 0.0])))
          (is (not (u/file-exists fname)))
          (is (some? (i/get-value kv c/vec-meta-dbi "legacy" :string :data)))
          (finally (i/close-vecs migrated))))
      (finally
        (i/close-vecs index)
        (d/close-kv kv)
        (u/delete-files dir)))))

(deftest index-creation-and-mutations-follow-ha-write-routing-test
  (let [root (u/tmp-dir (str "index-failover-" (UUID/randomUUID)))
        source-port (allocate-port)
        target-port (allocate-port)
        ^Server source (server/create {:root (str root "/source")
                                       :port source-port})
        ^Server target (server/create {:root (str root "/target")
                                       :port target-port})
        ^Map dbs (.-dbs source)
        db-name "indexes"
        uri (fn [port] (str "dtlv://datalevin:datalevin@localhost:"
                            port "/" db-name))]
    (try
      (server/start source)
      (server/start target)
      (let [source-kv (d/open-kv (uri source-port))
            target-kv (d/open-kv (uri target-port))
            initial (.get dbs db-name)
            store (:store initial)
            before (set (i/list-dbis store))]
        (try
          (.put dbs db-name
                (assoc initial
                       :ha-authority ::authority :ha-role :follower
                       :ha-node-id 1 :ha-authority-owner-node-id 2
                       :ha-members [{:node-id 2
                                     :endpoint (str "localhost:" target-port)}]))
          (let [engine (d/new-search-engine source-kv {:domain "routed-search"})
                index (d/new-vector-index source-kv
                                         {:domain "routed-vector" :dimensions 2})]
            (try
              (d/add-doc engine :one "routed document")
              (d/add-vec index :one [1.0 0.0])
              (d/force-vec-checkpoint! index)
              (is (= [:one] (vec (d/search engine "routed"))))
              (is (= [:one] (d/search-vec index [1.0 0.0])))
              (is (= before (set (i/list-dbis store)))
                  "all initialization and mutations must run on the target")
              (is (contains? (set (d/list-dbis target-kv))
                             (str "routed-vector/" c/vec-refs)))
              (finally (i/close-vecs index))))
          (finally
            (.put dbs db-name initial)
            (d/close-kv source-kv)
            (d/close-kv target-kv))))
      (finally
        (server/stop source)
        (server/stop target)
        (u/delete-files root)))))
