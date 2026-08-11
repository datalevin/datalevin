(ns datalevin.test.core
  (:require
   [clojure.test :as t]
   [clojure.walk :as walk]
   [datalevin.core :as d]
   [datalevin.entity :as de]
   [taoensso.timbre :as log]
   [datalevin.constants :as c])
  (:import
   [java.net ServerSocket]))

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
         (walk/prewalk #(if (de/entity? %)
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

(defn db-fixture
  [f]
  (log/set-min-level! :report)
  (binding [c/*db-background-sampling?* false]
    (f))
  (System/gc))
