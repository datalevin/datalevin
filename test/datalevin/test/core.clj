(ns datalevin.test.core
  (:require
   [clojure.test :as t :refer [is are deftest testing]]
   [datalevin.client :as cl]
   [datalevin.core :as d]
   [datalevin.entity :as de]
   [taoensso.timbre :as log]
   [datalevin.constants :as c]
   [datalevin.util :as u :refer [defrecord-updatable]]
   [datalevin.server :as srv])
  (:import
   [java.net ServerSocket]
   [java.util UUID]))

(defn wrap-res [f]
  (let [res (f)]
    (when (pos? ^long (+ ^long (:fail res) ^long (:error res)))
      (System/exit 1))))

;; utils
(defmethod t/assert-expr 'thrown-msg? [msg form]
  (let [[_ match & body] form]
    `(try ~@body
          (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual nil})
          (catch Throwable e#
            (let [m# (.getMessage e#)]
              (if (= ~match m#)
                (t/do-report {:type :pass, :message ~msg, :expected '~form, :actual e#})
                (t/do-report {:type :fail, :message ~msg, :expected '~form, :actual e#})))
            e#))))

(defn entity-map [db e]
  (when-let [entity (d/entity db e)]
    (->> (assoc (into {} entity) :db/id (:db/id entity))
         (clojure.walk/prewalk #(if (de/entity? %)
                                  {:db/id (:db/id %)}
                                  %)))))

(defn all-datoms [db]
  (into #{} (map (juxt :e :a :v)) (d/datoms db :eav)))

(defn no-namespace-maps [t]
  (binding [*print-namespace-maps* false]
    (t)))

(defn allocate-port
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn server-fixture
  [f]
  (let [port   (allocate-port)
        dir    (u/tmp-dir (str "server-test-" (UUID/randomUUID)))
        server (binding [c/*db-background-sampling?* false]
                 (srv/create {:port port
                              :root dir}))]
    ;; (log/set-min-level! :debug)
    (log/set-min-level! :report)
    (binding [c/*db-background-sampling?* false
              cl/*default-port*        port]
      (try
        (srv/start server)
        (f)
        (catch Exception e (throw e))
        (finally
          (srv/stop server)
          (u/delete-files dir)))))
  (System/gc))

(defn db-fixture
  [f]
  (log/set-min-level! :report)
  (binding [c/*db-background-sampling?* false]
    (f))
  (System/gc))
