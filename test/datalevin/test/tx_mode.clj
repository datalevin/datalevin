(ns datalevin.test.tx-mode
  "Historical fixture labels, without a production routing switch.
  Repeated labeled runs test request isolation and binding handoff. Independent
  semantic comparisons use frozen source in another JVM, never these labels.")

(def ^:dynamic *engine* :compiler)
