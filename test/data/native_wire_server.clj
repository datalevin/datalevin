;; Separate-process receiver for the Python and JavaScript wire tests. These
;; bindings deliberately have no access to either client's classes or codec IDs.
(require '[datalevin.server :as srv]
         '[datalevin.udf :as udf]
         '[jsonista.core :as json])

(let [registry (udf/create-registry)]
  (doseq [[lang type-name] [[:python ":app/native-task"] [:javascript ":app/task"]]]
    (let [logical (fn [^datalevin.NativeValue value]
                    (let [data (json/read-value (.payload value))]
                      (if (= lang :python)
                        (subvec data 0 2)
                        [(Long/parseLong (nth data 1)) (nth data 2)])))
          equality (reify java.util.function.BiPredicate
                     (test [_ a b] (= (logical a) (logical b))))
          descriptor (fn [kind id] {:udf/lang lang :udf/kind kind :udf/id id})]
      (udf/register! registry (descriptor :order-fn :native/order)
                     #(first (logical %)))
      (udf/register! registry (descriptor :serializer :native/encode)
                     #(.payload ^datalevin.NativeValue %))
      (udf/register! registry (descriptor :deserializer :native/decode)
                     #(datalevin.NativeValue. "separate-server" type-name % equality))))
  (alter-var-root #'srv/*server-runtime-opts-fn*
                  (constantly (fn [_ _ _ _] {:udf-registry registry}))))
