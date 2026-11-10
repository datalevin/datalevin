(ns ^:no-doc datalevin.query.source.extrema
  "Explicit source-native text extrema over complete core aggregate groups."
  (:require [datalevin.parser :as parser]
            [datalevin.query.source :as source]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value :refer [fail!]]))

(defn prepare!
  "Bind explicit ordering requests to captured descriptors and acquired sessions.
  This validation performs no provider I/O, including for plan-only explains."
  [runtime compilation groups]
  (let [specs (get-in compilation [:prepared :native-extrema])
        entries (into {} (map (juxt :symbol identity)) (:entries compilation))
        by-alias (into {} (mapcat (fn [group] (map #(vector % group) (:aliases group)))) groups)
        elements (parser/find-elements (get-in compilation [:compiled :parsed :qfind]))]
    (into {}
      (map (fn [[variable {:keys [source attribute]}]]
             (let [_ (control/check-active! (:control runtime))
                   descriptor (:descriptor (entries source)) group (by-alias source)
                   session (:session group)]
               (when-not (and (= :string (get-in descriptor [:pattern-contract :attributes attribute :value-type]))
                              (some #(and (parser/aggregate? %)
                                          (#{'min 'max} (get-in % [:fn :symbol]))
                                          (= [variable] (mapv :symbol (:args %)))) elements)
                              (satisfies? source/IQuerySourceTextExtrema session))
                 (fail! :query-source/unsupported :projection))
               [variable {:session session
                          :request (merge (:request group)
                                          {:source-symbol source :source-descriptor descriptor
                                           :definition-fingerprint (:definition-fingerprint descriptor)
                                           :scope-id (get-in descriptor [:scope :id]) :attribute attribute})}]))) specs)))

(defn- extremum! [runtime {:keys [session request]} operator values]
  (let [ctl (:control runtime) limits (control/limits ctl)
        maximum (min 256 (:binding-rows limits))
        ;; Leave one slot for the previous chunk's winner. Extrema compose
        ;; exactly under the source's declared, deterministic total ordering.
        chunk-size (max 1 (dec maximum))
        held (volatile! nil)]
    (try
      (loop [pending (seq values) best nil]
        (control/check-active! ctl)
        (if-not pending
          (do (when-let [token @held] (rt/own! runtime #(control/release! token)))
              (vreset! held nil) best)
          (let [chunk (vec (take chunk-size pending))
                input (cond-> chunk best (conj best))]
            (when-not (every? string? input) (fail! :query-source/invalid :values))
            (let [bytes (value/value-charge input)]
              (when (> (count input) maximum)
                (fail! :query-source/limit :reserve
                       {:category :binding-rows :maximum maximum :attempted (count input)}))
              (when (> bytes (:binding-bytes limits))
                (fail! :query-source/limit :reserve
                       {:category :binding-bytes :maximum (:binding-bytes limits) :attempted bytes})))
            (let [token (rt/charge-copy! runtime input)
                  result (try
                (let [result (rt/invoke runtime :fetch
                               #(source/-text-extremum session (assoc request :operator operator :values input)))]
                  (when-not (and (string? result) (some #(= result %) input))
                    (fail! :query-source/invalid :values))
                  (let [owned (rt/charge-copy! runtime result)]
                    (when-let [old @held] (control/release! old))
                    (vreset! held owned))
                  result)
                (finally (control/release! token)))]
              (recur (seq (drop chunk-size pending)) result)))))
      (finally (when-let [token @held] (control/release! token))))))

(defn wrap [runtime prepared element fallback]
  (let [operator (get-in element [:fn :symbol])
        spec (get prepared (get-in element [:args 0 :symbol]))]
    (if (and spec (#{'min 'max} operator))
      (fn [values] (extremum! runtime spec (keyword (name operator)) values))
      fallback)))
