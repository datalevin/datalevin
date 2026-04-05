(ns datalevin.jepsen.init-cache)

(defonce ^:private registered-caches (atom #{}))

(defn register-cache!
  [cache]
  (swap! registered-caches conj cache)
  cache)

(defn cluster-cache
  []
  (register-cache! (atom #{})))

(defn release-cluster!
  [cluster-id]
  (doseq [cache @registered-caches]
    (swap! cache disj cluster-id))
  nil)
