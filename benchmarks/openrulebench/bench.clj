#!/usr/bin/env -S clojure -M

(require '[openrulebench.runner :as runner])

(let [exit-code (try
                  (runner/main! *command-line-args*)
                  (finally
                    (shutdown-agents)))]
  (System/exit exit-code))
