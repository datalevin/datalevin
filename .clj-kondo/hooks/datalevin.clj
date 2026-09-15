(ns hooks.datalevin
  (:require [clj-kondo.hooks-api :as api]))

(defn- token
  [x]
  (api/token-node x))

(defn- nil-node
  []
  (token nil))

(defn- let-node
  [bindings body]
  (api/list-node
    (list* (token 'let)
           (api/vector-node bindings)
           body)))

(declare node-sexpr)

(defn defcomp
  [{:keys [node]}]
  (let [[_ sym args & body] (:children node)]
    {:node (api/list-node
             (list* (token 'defn) sym args body))}))

(defn defpodfn
  [{:keys [node]}]
  (let [[_ sym args & body] (:children node)]
    {:node (api/list-node
             (list* (token 'defn) sym args body))}))

(defn import-macro
  [{:keys [node]}]
  (let [[_ qsym] (:children node)
        imported (some-> (node-sexpr qsym) name symbol)]
    {:node (api/list-node
             [(token 'defmacro)
              (token (or imported 'imported-macro))
              (api/vector-node [(token '&) (token '_args)])
              (nil-node)])}))

(defn inter-fn
  [{:keys [node]}]
  (let [[_ args & body] (:children node)]
    {:node (api/list-node
             (list* (token 'fn) args body))}))

(defn definterfn
  [{:keys [node]}]
  (let [[_ sym args & body] (:children node)]
    {:node (api/list-node
             (list* (token 'defn) sym args body))}))

(defn with-binding
  [{:keys [node]}]
  (let [[_ binding & body] (:children node)
        [sym init & extra] (:children binding)
        init (or init (nil-node))]
    {:node (let-node [sym init]
                     (concat extra body))}))

(defn scan
  [{:keys [node]}]
  (let [[_ lmdb dbi-name call error keep-rtx?] (:children node)
        body (cond-> [call error]
               keep-rtx? (conj keep-rtx?))]
    {:node (let-node [(token 'dbi) (nil-node)
                      (token 'rtx) (nil-node)
                      (token 'cur) (nil-node)
                      (token 'e) (nil-node)
                      (token '_) lmdb
                      (token '_) dbi-name]
                     body)}))

(defn candidate-array
  [{:keys [node]}]
  (let [[_ & body] (:children node)]
    {:node (let-node [(token 'lst) (nil-node)]
                     body)}))

(defn- node-sexpr
  [node]
  (try
    (api/sexpr node)
    (catch Exception _
      ::unknown)))

(defn cond-plus
  [{:keys [node]}]
  (let [clauses (:children node)]
    (loop [clauses (next clauses)
           bindings []
           pre-body []
           cond-body []]
      (if-let [[test expr & more] (seq clauses)]
        (case (node-sexpr test)
          :let
          (recur more
                 (into bindings (:children expr))
                 pre-body
                 cond-body)

          :do
          (recur more
                 bindings
                 (conj pre-body expr)
                 cond-body)

          :some
          (recur more
                 bindings
                 pre-body
                 (conj cond-body expr expr))

          (recur more
                 bindings
                 pre-body
                 (conj cond-body test expr)))
        {:node (let-node bindings
                         (concat pre-body
                                 [(api/list-node
                                    (list* (token 'cond) cond-body))]))}))))

(defn extend-freeze
  [{:keys [node]}]
  (let [[_ type tag binding & body] (:children node)]
    {:node (api/list-node
             [(token 'do)
              type
              tag
              (api/list-node
                (list* (token 'fn) binding body))])}))

(defn extend-thaw
  [{:keys [node]}]
  (let [[_ tag binding & body] (:children node)]
    {:node (api/list-node
             [(token 'do)
              tag
              (api/list-node
                (list* (token 'fn) binding body))])}))

(def ^:private deftype-plus-markers
  '#{defremote-forward defremote-txlog-methods def-read-kv-forwarders})

(defn- sexpr->node
  [x]
  (cond
    (symbol? x)  (api/token-node x)
    (keyword? x) (api/keyword-node x)
    (string? x)  (api/string-node x)
    (vector? x)  (api/vector-node (mapv sexpr->node x))
    (map? x)     (api/map-node
                   (into []
                         (mapcat (fn [[k v]] [(sexpr->node k) (sexpr->node v)]))
                         x))
    (seq? x)     (api/list-node (map sexpr->node x))
    :else        (api/token-node x)))

;; Mirrors datalevin.remote/remote-forward-method so lint sees the generated
;; request calls, serialization and chatty-detection references.
(defn- remote-forward-method-sexpr
  [request [mname args & flags]]
  (let [opts      (apply hash-map flags)
        wire-op   (or (:op opts) (keyword (name mname)))
        freeze    (:serialize opts)
        frozen    (some-> freeze (as-> f (symbol (str "frozen-" (name f)))))
        wire-args (mapv #(if (and frozen (= % freeze)) frozen %) (rest args))
        call      (concat request
                          [wire-op (into ['db-name] wire-args) 'writing?])
        call      (if (:chatty opts)
                    (list 'do
                          (list 'detect-chatty-kv! 'db-name 'dbi-name wire-op)
                          call)
                    call)]
    (list mname args
          (if frozen
            (list 'let [frozen (list 'b/serialize freeze)] call)
            call))))

(def ^:private remote-txlog-specs
  '[(txlog-watermarks [_])
    (open-tx-log [_ from-lsn upto-lsn])
    (force-txlog-sync! [_])
    (force-lmdb-sync! [_])
    (create-snapshot! [_])
    (list-snapshots [_])
    (snapshot-scheduler-state [_])
    (read-commit-marker [_])
    (verify-commit-marker! [_])
    (txlog-retention-state [_])
    (gc-txlog-segments! [_ retain-floor-lsn])
    (txlog-update-snapshot-floor! [_ snapshot-lsn previous-snapshot-lsn])
    (txlog-clear-snapshot-floor! [_])
    (txlog-update-replica-floor! [_ replica-id applied-lsn])
    (txlog-clear-replica-floor! [_ replica-id])
    (txlog-pin-backup-floor! [_ pin-id floor-lsn expires-ms])
    (txlog-unpin-backup-floor! [_ pin-id])])

(def ^:private remote-txlog-shims
  '[(open-tx-log [this from-lsn]
      (.open-tx-log this from-lsn nil))
    (gc-txlog-segments! [this]
      (.gc-txlog-segments! this nil))
    (txlog-update-snapshot-floor! [this snapshot-lsn]
      (.txlog-update-snapshot-floor! this snapshot-lsn nil))
    (txlog-pin-backup-floor! [this pin-id floor-lsn]
      (.txlog-pin-backup-floor! this pin-id floor-lsn nil))])

;; Mirrors datalevin.kv/def-read-kv-forwarders.
(defn- read-kv-forwarder-sexpr
  [store lmdb [mname args]]
  (list mname (into [store] args)
        (concat ['custom-kv/read-kv (keyword (name mname)) store lmdb]
                args)))

(defn- expand-deftype-plus-marker
  [s]
  (case (first s)
    defremote-forward
    (let [[_ request & specs] s]
      (map #(remote-forward-method-sexpr request %) specs))

    defremote-txlog-methods
    (concat
      (map #(remote-forward-method-sexpr '(cl/normal-request client) %)
           remote-txlog-specs)
      remote-txlog-shims)

    def-read-kv-forwarders
    (let [[_ store lmdb & specs] s]
      (map #(read-kv-forwarder-sexpr store lmdb %) specs))))

(defn deftype-plus
  [{:keys [node]}]
  (let [[_ name fields & body] (:children node)
        expand (fn [child]
                 (let [s (node-sexpr child)]
                   (if (and (seq? s)
                            (contains? deftype-plus-markers (first s)))
                     (map sexpr->node (expand-deftype-plus-marker s))
                     [child])))
        body   (mapcat expand body)]
    {:node (api/list-node
             (list* (token 'deftype) name fields body))}))
