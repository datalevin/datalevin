(ns ^:no-doc datalevin.query.source.descriptor
  "Invocation-owned capture of fully validated v2 EAV descriptors."
  (:require [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.schema :as schema]
            [datalevin.query.source.value :as value :refer [fail!]]))

(def source-info! schema/source-info!)

(defn capture!
  "Read a classified external source's descriptor exactly once, admit metadata
  before structural traversal, and detach its backing/metadata under quotas.
  All aliases and subsequent phases reuse this captured object. No negotiation,
  session acquisition or execution occurs here."
  [runtime provider]
  (let [ctl (:control runtime) check! #(control/check-active! ctl)
        tokens (volatile! []) success? (volatile! false)]
    (try
      (check!)
      (when-not (satisfies? source/IQuerySource provider)
        (fail! :query-source/unsupported :inputs))
      (let [info (rt/invoke runtime :descriptor #(source/-source-info provider))]
        (vswap! tokens conj (rt/charge-metadata! runtime info :descriptor 1024))
        (schema/admitted-source-info! info check!)
        (vswap! tokens conj (rt/charge-copy! runtime info))
        (let [captured (value/owned-copy info check!) held @tokens]
          (rt/own! runtime #(doseq [token held] (control/release! token)))
          (vreset! success? true)
          captured))
      (finally (when-not @success? (doseq [token @tokens] (control/release! token)))))))
