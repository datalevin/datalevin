(ns datalevin.server.session-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.server.dispatch :as dispatch]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import
   [java.nio.channels SelectionKey]
   [java.util UUID]))

(def ^:dynamic *sessions* nil)

(defn- session-fixture [f]
  (let [dir     (u/tmp-dir (str "session-races-" (UUID/randomUUID)))
        conn    (d/create-conn dir)
        clients (session/load-sessions conn)
        deps    {:sys-conn-fn (constantly conn)
                 :clients-fn (constantly clients)
                 :user-roles-fn (constantly #{:reader})
                 :user-permissions-fn (constantly #{:read})
                 :user-eid-fn (constantly 1)
                 :now-ms-fn (constantly 1)}]
    (try
      (binding [*sessions* {:conn conn :clients clients :deps deps}]
        (f))
      (finally
        (d/close conn)
        (u/delete-files dir)))))

(use-fixtures :once db-fixture)
(use-fixtures :each session-fixture)

(defn- add-session! []
  (let [id (UUID/randomUUID)]
    (session/add-client (:deps *sessions*) nil "127.0.0.1" id "alice")
    id))

(defn- persisted-sessions []
  (into {} (session/load-sessions (:conn *sessions*))))

(defn- touch! [client-id]
  (let [skey (doto (proxy [SelectionKey] [])
               (.attach (volatile! {:client-id client-id})))]
    (#'dispatch/set-last-active (:deps *sessions*) nil skey)))

(defn- await! [task]
  (let [result (deref task 5000 ::timeout)]
    (when (= ::timeout result)
      (throw (ex-info "Session race test timed out" {})))
    result))

(defn- overlap-update! [client-id f competing-operation]
  (let [entered (promise)
        resume  (promise)
        started (promise)
        other   (atom nil)
        update  (future
                  (session/update-client
                    (:deps *sessions*) nil client-id
                    (fn [current]
                      (deliver entered true)
                      (await! resume)
                      (f current))))]
    (try
      (await! entered)
      (reset! other (future
                      (deliver started true)
                      (competing-operation)))
      (await! started)
      (is (= ::pending (deref @other 100 ::pending))
          "changes to the same session wait for the in-flight update")
      (finally
        (deliver resume true)
        (await! update)
        (when @other (await! @other))))))

(deftest missing-and-nil-session-updates-are-no-ops-test
  (let [{:keys [deps clients]} *sessions*
        missing (UUID/randomUUID)
        called  (atom 0)]
    (doseq [id [missing nil]]
      (is (nil? (session/update-client
                  deps nil id
                  #(do (swap! called inc) (assoc % :roles #{:writer})))))
      (touch! id)
      (session/remove-client deps nil id))
    (is (zero? @called))
    (is (empty? clients))
    (is (empty? (persisted-sessions)))))

(deftest stale-updates-cannot-recreate-a-removed-session-test
  (let [id (add-session!)
        {:keys [deps clients]} *sessions*
        called (atom 0)]
    (session/remove-client deps nil id)
    (session/update-client deps nil id
                           #(do (swap! called inc)
                                (assoc % :permissions #{:write})))
    (touch! id)
    (is (zero? @called))
    (is (empty? clients))
    (is (empty? (persisted-sessions)))))

(deftest removal-racing-an-update-deletes-both-session-copies-test
  (let [id (add-session!)
        {:keys [deps clients]} *sessions*]
    ;; If only update-client uses computeIfPresent, removal can delete LMDB
    ;; first, then wait for the update to persist the session again. Reloading
    ;; below catches that orphan even when the live map ends up empty.
    (overlap-update! id #(assoc % :roles #{:writer})
                     #(session/remove-client deps nil id))
    (is (empty? clients))
    (is (empty? (persisted-sessions)))))

(deftest concurrent-session-updates-preserve-both-changes-test
  (let [id (add-session!)
        {:keys [deps clients]} *sessions*
        original (get clients id)]
    (overlap-update! id #(assoc % :roles #{:writer})
                     #(session/update-client deps nil id
                                             (fn [s] (assoc s :indices #{"v"}))))
    (let [expected (assoc original :roles #{:writer} :indices #{"v"})]
      (is (= expected (get clients id)))
      (is (= {id expected} (persisted-sessions))))))

(deftest activity-update-preserves-concurrent-session-changes-test
  (let [id (add-session!)
        clients (:clients *sessions*)
        original (get clients id)]
    (overlap-update! id #(assoc % :permissions #{:write}) #(touch! id))
    (let [updated (get clients id)
          persisted (assoc original :permissions #{:write})]
      (is (= (dissoc persisted :last-active) (dissoc updated :last-active)))
      (is (< 1 (:last-active updated)))
      (is (= {id persisted} (persisted-sessions))
          "activity timestamps do not cause durable writes on every request"))))

(deftest failed-session-persistence-leaves-live-state-intact-test
  (let [id (add-session!)
        {:keys [deps clients]} *sessions*
        original (get clients id)
        failure (ex-info "session storage unavailable" {})
        failing-deps (assoc deps :sys-conn-fn (fn [_] (throw failure)))]
    (doseq [operation [#(session/update-client failing-deps nil id
                                              (fn [s] (assoc s :roles #{:writer})))
                       #(session/remove-client failing-deps nil id)]]
      (is (identical? failure (try (operation) (catch Exception e e))))
      (is (= original (get clients id)))
      (is (= {id original} (persisted-sessions))))))
