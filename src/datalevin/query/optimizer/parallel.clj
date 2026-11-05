(ns ^:no-doc datalevin.query.optimizer.parallel
  "Admission and worker budgets for independent read partitions."
  (:import [java.util.concurrent ForkJoinPool ForkJoinWorkerThread]))

(def ^:dynamic *max-workers*
  "Maximum participants including the caller; 1 keeps these read paths serial."
  4)
(def ^:dynamic *scan-rows-per-worker* 131072)
(def ^:dynamic *witness-keys-per-worker* 8192)

(defn participants
  "Include the caller in the budget. Keep small work and nested pool tasks
  serial; operators supply the size of their independent units of work."
  ^long [rows minimum]
  (let [minimum (long minimum)
        rows (long rows)]
    (if (or (< (long *max-workers*) 2) (not (pos? minimum))
            (< rows (* 2 minimum))
            (instance? ForkJoinWorkerThread (Thread/currentThread)))
      1
      (max 1 (min (long *max-workers*)
                  (.availableProcessors (Runtime/getRuntime))
                  (inc (.getParallelism (ForkJoinPool/commonPool)))
                  (quot rows minimum))))))
