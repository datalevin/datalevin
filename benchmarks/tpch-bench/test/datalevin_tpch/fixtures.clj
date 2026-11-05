(ns datalevin-tpch.fixtures
  "Convert SQL-shaped query fixtures to independently allocated reference facts."
  (:require [datalevin-tpch.common :as c]
            [datalevin-tpch.datalevin :as dt]))

(defn reference-facts [facts]
  (let [targets (set (map #(first (c/attrs (second %))) (vals dt/references)))
        ids (into {} (keep (fn [[e a v]] (when (targets a) [[a v] e]))) facts)
        ;; Some SQL edge-case fixtures deliberately contain dangling keys.
        ;; Reserve distinct absent entities instead of aliasing existing rows.
        missing (atom {})
        next-id (atom (reduce max 0 (map first facts)))]
    (mapv (fn [[e a v]]
            (if-let [[ref table] (dt/references a)]
              (let [key [(first (c/attrs table)) v]
                    target (or (ids key) (@missing key)
                               (let [id (swap! next-id inc)]
                                 (swap! missing assoc key id)
                                 id))]
                [e ref target])
              [e a v])) facts)))
