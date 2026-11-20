(ns datalevin.server.startup-migration-test
  (:require
   [clojure.java.io :as io]
   [clojure.test :refer [deftest is]]
   [datalevin.migrate :as migrate]
   [datalevin.server.session :as session]
   [datalevin.util :as u])
  (:import
   [java.io IOException PipedInputStream PipedOutputStream]
   [java.util UUID]
   [java.util.concurrent ConcurrentHashMap CountDownLatch TimeUnit]))

(defn- restore-deps [open!]
  {:open-store-fn open!
   :needs-migration-fn (fn [_ name] (not= name "current"))
   :close-store-fn (fn [_])
   :resolved-runtime-opts-fn (fn [& _])
   :ensure-ha-runtime-fn (fn [_ _ state _] state)})

(deftest bounded-migrations-merge-session-references
  (let [names ["a" "b" "c" "d" "e"]
        clients {1 {:stores (zipmap (conj names "current" "ha") (repeat {:dbis #{"one"}}))}
                 2 {:stores {"a" {:dbis #{"two"}} "ha" {:consensus-ha? true}}}}
        started (CountDownLatch. 3)
        release (promise)
        opened (atom [])
        active (atom 0)
        peak (atom 0)
        dbs (ConcurrentHashMap.)
        caller (promise)
        open! (fn [_ name dbis _]
                (swap! opened conj [name dbis])
                (if (= name "current")
                  (is (identical? @caller (Thread/currentThread)))
                  (do
                    (swap! peak max (swap! active inc))
                    (.countDown started)
                    (try @release (finally (swap! active dec)))))
                name)
        job (future
              (deliver caller (Thread/currentThread))
              (session/reopen-dbs (restore-deps open!) nil clients dbs 3))]
    (try
      (is (.await started 5 TimeUnit/SECONDS))
      (finally (deliver release true)))
    (is (nil? (deref job 5000 ::timeout)))
    (is (= 3 @peak))
    (is (= (frequencies (conj names "current")) (frequencies (map first @opened))))
    (is (= #{"one" "two"} (get (into {} @opened) "a")))
    (is (= (set (conj names "current")) (set (keys dbs))))))

(deftest failed-migration-waits-for-active-store
  (let [waiting (promise)
        release (promise)
        failed (promise)
        failure (IOException. "failed migration")
        dbs (ConcurrentHashMap.)
        opened (atom [])
        open! (fn [_ name _ _]
                (swap! opened conj name)
                (when (= name "active")
                  (deliver waiting true)
                  @release)
                name)
        deps (assoc (restore-deps open!)
                    :close-store-fn (fn [_] (deliver failed true))
                    :ensure-ha-runtime-fn (fn [_ name state _]
                                            (if (= name "broken")
                                              (do @waiting (throw failure))
                                              state)))
        clients {1 {:stores {"active" {} "broken" {}}}}
        job (future
              (try (session/reopen-dbs deps nil clients dbs 2)
                   (catch Throwable t t)))]
    (try
      (is (= true (deref failed 5000 ::timeout)))
      (is (= ::waiting (deref job 100 ::waiting)))
      (is (empty? dbs))
      (finally (deliver release true)))
    (is (identical? failure (deref job 5000 ::timeout)))
    (is (= #{"active" "broken"} (set @opened)))
    (is (= #{"active"} (set (keys dbs))))))

(deftest cold-jar-cache-publishes-only-complete-downloads
  (let [dir (u/tmp-dir (str "datalevin-migration-jar-" (UUID/randomUUID)))
        jar (io/file dir "release.jar")
        input (PipedInputStream.)
        output (PipedOutputStream. input)]
    (try
      ;; A failed download leaves the final path available for a later retry.
      (is (thrown? IOException (#'migrate/cache-jar! jar (io/file dir "missing"))))
      (is (not (.exists jar)))
      (let [copying (promise)
            source (reify io/IOFactory
                     (make-input-stream [_ _] (deliver copying true) input))
            first-open (future (#'migrate/cache-jar! jar source))]
        (try
          (is (= true (deref copying 5000 ::timeout)))
          (is (not (.exists jar)))
          (let [other (future (#'migrate/cache-jar! jar (io/file dir "missing")))]
            (.write output (.getBytes "complete" "UTF-8"))
            (.close output)
            (is (= (.getAbsolutePath jar) (deref first-open 5000 ::timeout)
                   (deref other 5000 ::timeout)))
            (is (= "complete" (slurp jar))))
          (finally (.close output))))
      (finally
        (.close input)
        (.close output)
        (u/delete-files dir)))))
