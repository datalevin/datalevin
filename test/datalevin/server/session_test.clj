(ns datalevin.server.session-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [datalevin.core :as d]
   [datalevin.server.dispatch :as dispatch]
   [datalevin.server.session :as session]
   [datalevin.test.core :refer [db-fixture]]
   [datalevin.util :as u])
  (:import
   [java.nio.channels SelectionKey Selector]
   [java.util UUID]))

(def ^:dynamic *sessions* nil)

(defn- session-fixture [f]
  (let [dir     (u/tmp-dir (str "session-races-" (UUID/randomUUID)))
        conn    (d/create-conn dir)
        selector (Selector/open)
        clock   (atom 1)
        clients (session/load-sessions conn)
        deps    {:sys-conn-fn (constantly conn)
                 :clients-fn (constantly clients)
                 :user-roles-fn (constantly #{:reader})
                 :user-permissions-fn (constantly #{:read})
                 :user-eid-fn (constantly 1)
                 :now-ms-fn #(deref clock)
                 :idle-timeout-fn (constantly 1000)
                 :selector-fn (constantly selector)}]
    (try
      (binding [*sessions* {:conn conn :clients clients :deps deps :clock clock}]
        (f))
      (finally
        (.close selector)
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

(defn- touch!
  ([client-id] (touch! client-id (swap! (:clock *sessions*) inc)))
  ([client-id now-ms]
   (reset! (:clock *sessions*) now-ms)
   (let [deps (:deps *sessions*)
         skey (doto (proxy [SelectionKey] [])
                (.attach (volatile! {:client-id client-id})))]
     (#'dispatch/set-last-active
      {:touch-client-fn #(session/touch-client deps %1 %2)} nil skey))))

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

(defn- expire! [deps now-ms]
  (reset! (:clock *sessions*) now-ms)
  (session/remove-idle-sessions deps nil))

(defn- restored-deps [now-ms]
  (let [clients (session/load-sessions (:conn *sessions*) now-ms)]
    (assoc (:deps *sessions*) :clients-fn (constantly clients))))

(deftest activity-checkpoints-are-throttled-and-restore-long-lived-sessions-test
  (let [id (add-session!)
        {:keys [clients deps]} *sessions*]
    (doseq [now-ms [2 100 250]] (touch! id now-ms))
    (is (= 250 (:last-active (get clients id))))
    (is (= 1 (:last-active (get (persisted-sessions) id))))
    (touch! id 251)
    (is (= 251 (:last-active (get (persisted-sessions) id))))
    ;; Keep using the same DB for much longer than its original idle timeout.
    (doseq [now-ms [500 501 750 751 1000 1001 1250 1251 1499]]
      (touch! id now-ms))
    (is (= 1251 (:last-active (get (persisted-sessions) id))))
    (let [restored (restored-deps 1600)]
      (expire! restored 1600)
      (is (some? (get ((:clients-fn restored) nil) id)))
      ;; Grace is only needed when reconstructing activity lost in a crash.
      (expire! deps 2500)
      (is (nil? (get clients id))))))

(deftest crash-allowance-is-bounded-across-repeated-restarts-test
  (let [id (add-session!)]
    (touch! id 1001)
    (touch! id 1250)
    (let [restored (restored-deps 2100)]
      (expire! restored 2100)
      (is (some? (get ((:clients-fn restored) nil) id)))
      ;; Stopping an untouched restored instance must retain its original bound.
      (session/flush-sessions! restored nil))
    (let [restored (restored-deps 2251)]
      (expire! restored 2251)
      (is (some? (get ((:clients-fn restored) nil) id))))
    (let [restored (restored-deps 2252)]
      (expire! restored 2252)
      (is (nil? (get ((:clients-fn restored) nil) id))))
    (is (empty? (persisted-sessions)))))

(deftest shutdown-flush-saves-exact-activity-and-idle-expiry-test
  (let [id (add-session!)
        {:keys [deps]} *sessions*]
    (touch! id 250)
    (is (= 1 (:last-active (get (persisted-sessions) id))))
    (session/flush-sessions! deps nil)
    (is (= 250 (:last-active (get (persisted-sessions) id))))
    (is (= 250 (:last-active-checkpoint-until (get (persisted-sessions) id))))
    (let [restored (restored-deps 1250)]
      (expire! restored 1250)
      (is (some? (get ((:clients-fn restored) nil) id)))
      (expire! restored 1251)
      (is (nil? (get ((:clients-fn restored) nil) id))))))

(deftest legacy-session-timestamps-are-migrated-once-test
  (let [id (UUID/randomUUID)
        legacy {:username "alice" :last-active 1 :stores {}}
        lmdb (session/session-lmdb (:conn *sessions*))]
    (d/transact-kv lmdb [[:put session/session-dbi id legacy :uuid :data]])
    (let [restored (restored-deps 10000)]
      (is (= 10000 (:last-active (get ((:clients-fn restored) nil) id))))
      (expire! restored 10001)
      (is (some? (get ((:clients-fn restored) nil) id))))
    (let [restored (restored-deps 11001)]
      (is (= 10000 (:last-active (get ((:clients-fn restored) nil) id))))
      (expire! restored 11001)
      (is (empty? ((:clients-fn restored) nil))))))

(deftest failed-activity-checkpoint-does-not-publish-an-unsaved-window-test
  (let [id (add-session!)
        {:keys [deps clients clock]} *sessions*
        original (get clients id)
        failure (ex-info "session storage unavailable" {})
        failed-deps (assoc deps :sys-conn-fn (fn [_] (throw failure)))]
    (reset! clock 251)
    (is (identical? failure (try (session/touch-client failed-deps nil id)
                                (catch Exception e e))))
    (is (= original (get clients id)))
    (is (= original (get (persisted-sessions) id)))
    (session/touch-client deps nil id)
    (is (= 251 (:last-active (get (persisted-sessions) id))))))

(deftest session-flush-and-removal-cannot-resurrect-a-session-test
  (let [id (add-session!)
        {:keys [deps clients conn]} *sessions*
        entered (promise)
        release (promise)
        removing (promise)
        flushing-deps (assoc deps :sys-conn-fn
                             (fn [_]
                               (deliver entered true)
                               (await! release)
                               conn))]
    (touch! id 100)
    (let [flush (future (session/flush-sessions! flushing-deps nil))
          remove (future (await! entered)
                         (deliver removing true)
                         (session/remove-client deps nil id))]
      (try
        (await! removing)
        (is (= ::pending (deref remove 100 ::pending)))
        (finally
          (deliver release true)
          (await! flush)
          (await! remove))))
    (session/flush-sessions! deps nil)
    (is (empty? clients))
    (is (empty? (persisted-sessions)))))

(deftest idle-sweep-rechecks-activity-after-acquiring-the-session-lock-test
  (let [id (add-session!)
        {:keys [deps clients clock]} *sessions*]
    (reset! clock 2000)
    ;; The sweep sees the old session while its update still holds the key lock.
    (overlap-update! id #(assoc % :last-active 2000)
                     #(session/remove-idle-sessions deps nil))
    (is (= 2000 (:last-active (get clients id))))
    (is (= 2000 (:last-active (get (persisted-sessions) id))))))

(deftest new-activity-consumes-the-restored-crash-allowance-test
  (let [id (add-session!)
        restored (restored-deps 100)
        clients ((:clients-fn restored) nil)]
    (reset! (:clock *sessions*) 100)
    (session/touch-client restored nil id)
    (expire! restored 1100)
    (is (some? (get clients id)))
    (expire! restored 1101)
    (is (nil? (get clients id)))))

(deftest idle-sweep-does-not-wait-for-active-session-persistence-test
  (let [id (add-session!)
        {:keys [deps clock]} *sessions*
        entered (promise)
        release (promise)
        update (future
                 (session/update-client deps nil id
                                        #(do (deliver entered true)
                                             (await! release)
                                             (assoc % :roles #{:writer}))))]
    (try
      (await! entered)
      (reset! clock 100)
      (is (nil? (await! (future (session/remove-idle-sessions deps nil)))))
      (finally
        (deliver release true)
        (await! update)))))
