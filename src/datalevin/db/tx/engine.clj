;; Copyright (c) Huahai Yang. All rights reserved.
;; Distributed under the Eclipse Public License 2.0.
(ns ^:no-doc datalevin.db.tx.engine
  "Internal migration/test selection. The default retains accepted routing.
  The independent reference is the campaign's frozen engine in another JVM.")

(def ^:dynamic *engine*
  ;; Process selection also covers async executors and remote server threads;
  ;; synchronous collector calls additionally convey request-local bindings.
  (keyword (System/getProperty "datalevin.test.write-engine" "current")))

(defn interpreter?
  "Select conservative execution for tests. Compiler selection retains current
  routing for unmigrated entries. Collector descriptors convey caller bindings;
  unknown engines fail explicitly."
  []
  (let [selected *engine*]
    (cond
      (or (identical? selected :current) (identical? selected :compiler)) false
      (identical? selected :interpreter) true
      :else (throw (ex-info "Unknown Datalog transaction engine" {:engine selected})))))

(defn compiler?
  "Select the opt-in compiler slice; unmigrated entries retain current routing."
  []
  (let [selected *engine*]
    (cond
      (identical? selected :compiler) true
      (or (identical? selected :current) (identical? selected :interpreter)) false
      :else (throw (ex-info "Unknown Datalog transaction engine" {:engine selected})))))
