;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.idoc
  "Indexed document parsing and validation utilities."
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [datalevin.bits :as b]
   [datalevin.constants :as c]
   [datalevin.datom :as d]
   [datalevin.index :as idx]
   [datalevin.interface :as i
    :refer [open-dbi open-list-dbi get-value visit transact-kv]]
   [datalevin.remote :as r]
   [datalevin.lmdb :as l]
   [datalevin.spill :as sp]
   [datalevin.util :as u :refer [raise map+]]
   [jsonista.core :as json]
   [nextjournal.markdown :as md])
  (:import
   [java.util IdentityHashMap HashSet HashMap Collections List Map$Entry Set]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong]
   [java.util.concurrent.locks Lock ReentrantReadWriteLock]
   [datalevin.spill SpillableMap]
   [datalevin.utl LRUCache]
   [org.eclipse.collections.impl.list.mutable FastList]
   [java.math BigDecimal BigInteger]
   [org.roaringbitmap RoaringBitmap]))

(def ^:private default-format :edn)

(def ^:private allowed-formats #{:edn :json :markdown})

(def ^:private json-mapper
  (json/object-mapper {:decode-key-fn identity}))

(defn- resolve-format
  [attr props]
  (let [fmt (or (:db/idocFormat props) default-format)]
    (when-not (allowed-formats fmt)
      (raise "Bad attribute specification for " attr
             ": {:db/idocFormat " fmt "} should be one of " allowed-formats
             {:error     :schema/validation
              :attribute attr
              :key       :db/idocFormat
              :value     fmt}))
    fmt))

(defn- normalize-header
  [s]
  (let [s (-> s
              str
              str/trim
              str/lower-case
              (str/replace #"^[0-9]+[.\)\s-]*" "")
              (str/replace #"[^\p{L}\p{Nd}\s-]" "")
              (str/replace #"\s+" "-")
              (str/replace #"-+" "-")
              (str/replace #"(^-+|-+$)" ""))]
    (when (str/blank? s)
      (raise "Markdown header normalizes to empty string" {:header s}))
    (keyword s)))

(defn- normalize-seg
  [format seg]
  (if (identical? format :markdown)
    (cond
      (and (keyword? seg) (#{:? :*} seg)) seg
      (keyword? seg)                      (normalize-header (subs (str seg) 1))
      (string? seg)                       (normalize-header seg)
      :else                               seg)
    seg))

(defn- ensure-map
  [m path ctx]
  (if (empty? path)
    m
    (let [v (get-in m path)]
      (cond
        (nil? v) (assoc-in m path {})
        (map? v) m
        :else    (raise "Markdown header has both content and subheaders at "
                        ctx {:path path})))))

(defn- set-content
  [m path lines]
  (let [content (str/join "\n" lines)]
    (cond
      (empty? path)
      (do
        (when-not (str/blank? content)
          (raise "Markdown content appears before any header" {}))
        m)

      (str/blank? content) m

      :else (let [existing (get-in m path)]
              (cond
                (nil? existing) (assoc-in m path content)

                (map? existing)
                (raise "Markdown header has both content and subheaders"
                       {:path path})

                :else (assoc-in m path content))))))

(def ^:private markdown-break-tags
  #{:softbreak :hardbreak :linebreak :soft-line-break :hard-line-break :br})

(defn- node-children
  [content]
  (cond
    (vector? content)     content
    (sequential? content) (vec content)
    :else                 []))

(defn- node-text
  [node]
  (cond
    (string? node) node

    (map? node)
    (let [tag      (:type node)
          content  (:content node)
          children (node-children content)
          text     (:text node)]
      (cond
        (markdown-break-tags tag) "\n"
        (string? content)         content
        (string? text)            text
        (seq children)            (apply str (map node-text children))
        (sequential? text)        (apply str (map node-text text))
        :else                     ""))

    (sequential? node) (apply str (map node-text node))
    :else              ""))

(defn- header-node?
  [node]
  (and (map? node) (identical? (:type node) :heading)))

(defn- header-level
  [node]
  (let [level (or (:heading-level node)
                  (:level node)
                  (get-in node [:attrs :level]))]
    (when-not level
      (raise "Markdown heading missing level" {:node node}))
    (if (string? level)
      (Long/parseLong level)
      (long level))))

(defn- header-text [node] (node-text (or (:content node) (:text node) node)))

(defn- top-level-nodes
  [ir]
  (cond
    (map? ir)        (let [content (:content ir)]
                       (if (sequential? content) content [ir]))
    (sequential? ir) ir
    :else            [ir]))

(defn- parse-markdown
  [s]
  (let [ir      (md/parse (str s))
        nodes   (top-level-nodes ir)
        result  (volatile! {})
        levels  (volatile! [])
        path    (volatile! [])
        content (volatile! [])]
    (doseq [node nodes]
      (if (header-node? node)
        (let [^long level (header-level node)
              title       (header-text node)
              key         (normalize-header title)]
          (vswap! result set-content @path @content)
          (vreset! content [])
          (loop []
            (when (and (seq @levels)
                       (>= (long (peek @levels)) level))
              (vswap! levels pop)
              (vswap! path pop)
              (recur)))
          (vswap! result ensure-map @path {:title title})
          (when (contains? (get-in @result @path) key)
            (raise "Markdown header collision after normalization: " title
                   {:path @path :header title}))
          (vswap! path conj key)
          (vswap! levels conj level))
        (let [text (node-text node)]
          (when (and (empty? @path) (not (str/blank? text)))
            (raise "Markdown content appears before any header" {}))
          (when-not (str/blank? text)
            (vswap! content conj text)))))
    (vswap! result set-content @path @content)
    @result))

(defn- parse-json
  [s]
  (try
    (json/read-value s json-mapper)
    (catch Exception e
      (raise "Invalid JSON string for idoc" {:error e}))))

(defn- parse-edn
  [s]
  (try
    (edn/read-string s)
    (catch Exception e
      (raise "Invalid EDN string for idoc" {:error e}))))

(defn- normalize-doc
  [doc {:keys [idoc/max-depth]}]
  (let [seen            (IdentityHashMap.)
        ^long max-depth (when max-depth (long max-depth))]
    (letfn [(walk-coll [x depth f]
              (when (.containsKey seen x)
                (raise "Circular reference in idoc document" {:content x}))
              (.put seen x Boolean/TRUE)
              (try
                (f depth)
                (finally
                  (.remove seen x))))

            (walk [x depth]
              (cond
                (map? x)
                (walk-coll
                  x depth
                  (fn [d]
                    (let [next-depth (unchecked-inc ^long d)]
                      (when (and max-depth (> next-depth max-depth))
                        (raise "Idoc exceeds max depth"
                               {:max-depth max-depth :depth next-depth}))
                      (or
                        (reduce-kv
                          (fn [m k v]
                            (when-not (or (keyword? k) (string? k))
                              (raise "Idoc keys must be keywords or strings"
                                     {:key k}))
                            (when (identical? v :json/null)
                              (raise "Literal :json/null is reserved" {:key k}))
                            (when (and (sequential? v) (not (vector? v)))
                              (raise "Lists are not valid idoc values; use vectors"
                                     {:key k}))
                            (let [v' (walk v next-depth)]
                              (if (identical? v v')
                                m
                                (assoc (or m x) k v'))))
                          nil x)
                        x))))

                (vector? x)
                (walk-coll
                  x depth
                  (fn [d]
                    (let [n (count x)]
                      (loop [i 0
                             v nil]
                        (if (< ^long i n)
                          (let [old (nth x i)
                                new (walk old d)]
                            (recur (unchecked-inc ^long i)
                                   (if (identical? old new)
                                     v
                                     (assoc (or v x) i new))))
                          (or v x))))))

                (and (sequential? x) (not (vector? x)))
                (raise "Lists are not valid idoc values; use vectors"
                       {:content x})

                (identical? x :json/null)
                (raise "Literal :json/null is reserved" {})

                (nil? x) :json/null

                :else x))]
      (walk doc 0))))

(defn parse-value
  [attr props opts v]
  (let [fmt (resolve-format attr props)
        doc (cond
              (string? v) (case fmt
                            :json     (parse-json v)
                            :markdown (parse-markdown v)
                            :edn      (parse-edn v))
              (map? v)    v

              :else
              (raise "Idoc root must be a map" {:attribute attr :value v}))]
    (when-not (map? doc)
      (raise "Idoc root must be a map" {:attribute attr :value doc}))
    (normalize-doc doc opts)))

;; idoc patch

(def ^:private patch-update-ops
  #{:conj :merge :assoc :dissoc :inc :dec})

(defn- normalize-patch-path
  [path]
  (let [path (cond
               (vector? path)  path
               (keyword? path) [path]
               (string? path)  [path]
               :else
               (raise "Idoc patch path must be a keyword, string, or vector"
                      {:path path}))]
    (when (empty? path)
      (raise "Idoc patch path cannot be empty" {:path path}))
    (doseq [seg path]
      (cond
        (integer? seg)
        (when (neg? ^long seg)
          (raise "Idoc patch index must be non-negative"
                 {:path path :segment seg}))

        (or (keyword? seg) (string? seg))
        (when (#{:? :*} seg)
          (raise "Idoc patch path does not allow wildcard segments"
                 {:path path :segment seg}))

        :else
        (raise "Idoc patch path segment must be keyword, string, or integer"
               {:path path :segment seg})))
    path))

(defn- root-path
  [path]
  (let [n (count path)]
    (loop [i 0]
      (cond
        (= i n) path
        (integer? (nth path i)) (subvec path 0 i)
        :else (recur (unchecked-inc ^long i))))))

(defn- path-prefix?
  [prefix path]
  (and (<= (count prefix) (count path))
       (= prefix (subvec path 0 (count prefix)))))

(defn- add-minimal-path
  [paths path]
  (if (some #(path-prefix? % path) paths)
    paths
    (conj (reduce (fn [out existing]
                    (if (path-prefix? path existing)
                      out
                      (conj out existing)))
                  [] paths)
          path)))

(defn- normalize-patch-op
  [op]
  (when-not (sequential? op)
    (raise "Idoc patch op must be sequential" {:op op}))
  (let [argc (count op)
        kind (nth op 0 nil)
        path (normalize-patch-path (nth op 1 nil))]
    (case kind
      :set    (do
                (when-not (= 3 argc)
                  (raise "Idoc patch :set expects exactly one value"
                         {:op op :path path :args (drop 2 op)}))
                (object-array [:set path (nth op 2) nil]))
      :unset  (do
                (when-not (= 2 argc)
                  (raise "Idoc patch :unset does not take extra args"
                         {:op op :path path :args (drop 2 op)}))
                (object-array [:unset path nil nil]))
      :update (let [update-op (nth op 2 nil)
                    uargs     (drop 3 op)]
                (when-not (patch-update-ops update-op)
                  (raise "Unknown idoc patch update op"
                         {:op op :update-op update-op}))
                (object-array [:update path update-op uargs]))

      (raise "Unknown idoc patch op" {:op op :path path}))))

(defn- apply-update-op
  [current update-op args]
  (case update-op
    :conj
    (let [v (cond
              (nil? current)    []
              (vector? current) current
              :else             (raise "Idoc patch :conj requires a vector value"
                                       {:value current}))]
      (apply conj v args))

    :merge
    (let [m (cond
              (nil? current) {}
              (map? current) current
              :else          (raise "Idoc patch :merge requires a map value"
                                    {:value current}))]
      (apply merge m args))

    :assoc
    (let [m (cond
              (nil? current) {}
              (map? current) current
              :else          (raise "Idoc patch :assoc requires a map value"
                                    {:value current}))]
      (apply assoc m args))

    :dissoc
    (let [m (cond
              (nil? current) {}
              (map? current) current
              :else          (raise "Idoc patch :dissoc requires a map value"
                                    {:value current}))]
      (apply dissoc m args))

    :inc
    (do
      (when (seq args)
        (raise "Idoc patch :inc does not take extra args" {:args args}))
      (let [n (if (nil? current) 0 current)]
        (when-not (integer? n)
          (raise "Idoc patch :inc requires an integer" {:value current}))
        (inc ^long n)))

    :dec
    (do
      (when (seq args)
        (raise "Idoc patch :dec does not take extra args" {:args args}))
      (let [n (if (nil? current) 0 current)]
        (when-not (integer? n)
          (raise "Idoc patch :dec requires an integer" {:value current}))
        (dec ^long n)))))

(defn- update-in-idoc*
  [doc path f]
  (letfn [(step [node segs]
            (if (empty? segs)
              (f node)
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (integer? seg)
                  (let [v   (if (vector? node)
                              node
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg}))
                        idx (long seg)]
                    (when (or (neg? idx) (>= idx (count v)))
                      (raise "Idoc patch index out of bounds"
                             {:path path :segment seg :size (count v)}))
                    (assoc v idx (step (nth v idx) rest)))

                  (or (keyword? seg) (string? seg))
                  (let [m (cond
                            (nil? node) {}
                            (map? node) node
                            :else       (raise "Idoc patch path expects map"
                                               {:path path :segment seg}))]
                    (assoc m seg (step (get m seg) rest)))

                  :else
                  (raise
                    "Idoc patch path segment must be keyword, string, or integer"
                    {:path path :segment seg})))))]
    (step doc path)))

(defn- assoc-in-idoc
  [doc path value]
  (update-in-idoc* doc path (constantly value)))

(defn- update-in-idoc
  [doc path f]
  (update-in-idoc* doc path f))

(defn- unset-in-idoc
  [doc path]
  (letfn [(step [node segs ctx]
            (cond
              (nil? node)   nil
              (empty? segs) node
              :else
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (integer? seg)
                  (let [v   (cond
                              (vector? node) node
                              (nil? node)    nil
                              :else
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg}))
                        idx (long seg)]
                    (when (or (neg? idx) (>= idx (count v)))
                      (raise "Idoc patch index out of bounds"
                             {:path path :segment seg :size (count v)}))
                    (if (seq rest)
                      (assoc v idx (step (nth v idx) rest (conj ctx seg)))
                      (into [] cat [(subvec v 0 idx) (subvec v (inc idx))])))

                  (or (keyword? seg) (string? seg))
                  (let [m (cond
                            (map? node) node
                            (nil? node) nil
                            :else       (raise "Idoc patch path expects map"
                                               {:path path :segment seg}))]
                    (if (seq rest)
                      (if (contains? m seg)
                        (let [child  (get m seg)
                              child' (step child rest (conj ctx seg))]
                          (if (= child child') m (assoc m seg child')))
                        m)
                      (dissoc m seg)))

                  :else
                  (raise
                    "Idoc patch path segment must be keyword, string, or integer"
                    {:path path :segment seg})))))]
    (step doc path [])))

(defn apply-patch
  [doc ops]
  (let [ops (cond
              (nil? ops) []
              (sequential? ops) ops
              :else (raise "Idoc patch ops must be sequential" {:ops ops}))]
    (loop [doc   doc
           paths []
           more  (seq ops)]
      (if (seq more)
        (let [op                  (first more)
              ^objects normalized (normalize-patch-op op)
              kind                (aget normalized 0)
              path                (aget normalized 1)
              value               (aget normalized 2)
              args                (aget normalized 3)
              doc'                (case kind
                                    :set    (assoc-in-idoc doc path value)
                                    :unset  (unset-in-idoc doc path)
                                    :update (update-in-idoc
                                              doc path
                                              #(apply-update-op % value args)))]
          (recur doc' (add-minimal-path paths (root-path path)) (next more)))
        {:doc doc :paths paths}))))

;; path encoding

(defn- encode-string-seg
  [s]
  (let [s (-> (str s)
              (str/replace "%" "%25")
              (str/replace "/" "%2F"))]
    (if (str/starts-with? s ":")
      (str "%3A" (subs s 1))
      s)))

(defn encode-path
  [segments]
  (reduce
    (fn [acc seg]
      (if (keyword? seg)
        (str acc "/:" (subs (str seg) 1))
        (str acc "/" (encode-string-seg seg))))
    "" segments))

(defn- path-selector->segments
  [selector k]
  (let [segments (cond
                   (keyword? selector) [selector]
                   (string? selector)  [selector]
                   (vector? selector)  selector
                   :else
                   (raise "Idoc path selector must be a keyword, string, or vector"
                          {:key k :selector selector}))]
    (doseq [seg segments]
      (when-not (or (keyword? seg) (string? seg))
        (raise "Idoc path selector segments must be keywords or strings"
               {:key k :selector selector :segment seg})))
    segments))

(defn- normalize-selector
  [format selector k]
  (mapv #(normalize-seg format %) (path-selector->segments selector k)))

(defn- compile-path-prefixes
  [format k selectors]
  (when (some? selectors)
    (when-not (sequential? selectors)
      (raise "Idoc path selector option must be a sequential collection"
             {:key k :value selectors}))
    (vec (distinct (map #(encode-path (normalize-selector format % k))
                        selectors)))))

(defn compile-path-filter
  [format {:keys [indexed-paths excluded-paths]}]
  (let [included (compile-path-prefixes format :indexed-paths indexed-paths)
        excluded (compile-path-prefixes format :excluded-paths excluded-paths)]
    (when (or (some? included) (seq excluded))
      {:included included
       :excluded (or excluded [])})))

(defn- path-prefix-match?
  [prefix path]
  (or (empty? prefix)
      (= prefix path)
      (str/starts-with? path (str prefix "/"))))

(defn- indexed-path?
  [path-filter path]
  (if-not path-filter
    true
    (let [{:keys [included excluded]} path-filter]
      (and (or (nil? included)
               (some #(path-prefix-match? % path) included))
           (not-any? #(path-prefix-match? % path) excluded)))))

(defn- decode-string-seg
  [s]
  (let [s (if (str/starts-with? s "%3A")
            (str ":" (subs s 3))
            s)
        s (str/replace s "%2F" "/")
        s (str/replace s "%25" "%")]
    s))

(defn decode-path
  [path]
  (if (empty? path)
    []
    (do
      (when-not (str/starts-with? path "/")
        (raise "Idoc path must start with '/'" {:path path}))
      (let [len (count path)]
        (loop [idx 0
               out []]
          (if (>= ^long idx len)
            out
            (let [idx1 (u/long-inc idx)]
              (if (and (= (nth path (int idx)) \/)
                       (< ^long idx1 len)
                       (= (nth path (int idx1)) \:))
                (let [start (+ ^long idx 2)
                      ;; Stop at any "/" (both "/:" for keywords and "/" for strings)
                      next  (long (or (str/index-of path "/" start) len))
                      seg   (subs path (int start) (int next))]
                  (recur next (conj out (keyword seg))))
                (let [start idx1
                      next  (long (or (str/index-of path "/" start) len))
                      seg   (subs path (int start) (int next))]
                  (recur next (conj out (decode-string-seg seg))))))))))))

;; value typing for index encoding

(defn- value-type
  [v]
  (cond
    (integer? v)                      [:db.type/long (long v)]
    (string? v)                       [:db.type/string v]
    (keyword? v)                      [:db.type/keyword v]
    (boolean? v)                      [:db.type/boolean v]
    (instance? Double v)              [:db.type/double (double v)]
    (instance? Float v)               [:db.type/float (float v)]
    (ratio? v)                        [:db.type/bigdec (bigdec v)]
    (number? v)                       [:db.type/double (double v)]
    (symbol? v)                       [:db.type/symbol v]
    (uuid? v)                         [:db.type/uuid v]
    (inst? v)                         [:db.type/instant v]
    (bytes? v)                        [:db.type/bytes v]
    (instance? BigInteger v)          [:db.type/bigint v]
    (instance? clojure.lang.BigInt v) [:db.type/bigint (biginteger v)]
    (instance? BigDecimal v)          [:db.type/bigdec v]
    :else                             [:data v]))

;; borrow triple index encoding for path + value
(defn- indexable-key
  [^long path-id v]
  (let [[vt v'] (value-type v)]
    (b/indexable 0 (int path-id) v' vt c/g0)))

(defn- doc->path-values
  ([doc] (doc->path-values doc []))
  ([doc path0]
   (letfn [(append-seg [^String path seg]
             (if (keyword? seg)
               (str path "/:" (subs (str seg) 1))
               (str path "/" (encode-string-seg seg))))
           (add-leaf [acc ^String path v]
             (assoc! acc path (conj (get acc path #{}) v)))
           (walk [acc node ^String path]
             (cond
               (nil? node)    acc
               (map? node)    (reduce-kv (fn [a k v]
                                           (walk a v (append-seg path k)))
                                         acc node)
               (vector? node) (reduce (fn [a v] (walk a v path)) acc node)
               :else          (add-leaf acc path node)))]
     (persistent! (walk (transient {}) doc (encode-path path0))))))

(defn- append-path-seg
  [^String path seg]
  (if (keyword? seg)
    (str path "/:" (subs (str seg) 1))
    (str path "/" (encode-string-seg seg))))

(defn- collect-path-values!
  [^HashMap acc node ^String path path-filter]
  (cond
    (nil? node) acc

    (map? node)
    (reduce-kv (fn [a k v]
                 (collect-path-values! a v (append-path-seg path k)
                                       path-filter))
               acc node)

    (vector? node)
    (reduce (fn [a v]
              (collect-path-values! a v path path-filter))
            acc node)

    :else
    (do
      (when (indexed-path? path-filter path)
        (let [^HashSet s (or (.get acc path)
                             (let [s (HashSet.)]
                               (.put acc path s)
                               s))]
          (.add s node)))
      acc)))

(defn- doc->path-values-mutable
  ([doc] (doc->path-values-mutable doc []))
  ([doc path0] (doc->path-values-mutable doc path0 nil))
  ([doc path0 path-filter]
   (collect-path-values! (HashMap.) doc (encode-path path0) path-filter)))

(defn- diff-path-values
  ([old new] (diff-path-values old new []))
  ([old new path0] (diff-path-values old new path0 nil))
  ([old new path0 path-filter]
   (letfn [(append-seg [^String path seg]
             (if (keyword? seg)
               (str path "/:" (subs (str seg) 1))
               (str path "/" (encode-string-seg seg))))
           (add-leaf [^HashMap acc ^String path v]
             (when (indexed-path? path-filter path)
               (let [^HashSet s (or (.get acc path)
                                    (let [s (HashSet.)]
                                      (.put acc path s)
                                      s))]
                 (.add s v)))
             acc)
           (collect! [^HashMap acc node ^String path]
             (cond
               (nil? node)    acc
               (map? node)    (reduce-kv (fn [a k v]
                                           (collect! a v (append-seg path k)))
                                         acc node)
               (vector? node) (reduce (fn [a v] (collect! a v path)) acc node)
               :else          (add-leaf acc path node)))
           (walk [old new ^String path ^HashMap acc-old ^HashMap acc-new]
             (cond
               (identical? old new)
               [acc-old acc-new]

               (and (map? old) (map? new))
               (let [step (fn [[ao an] k ov nv]
                            (walk ov nv (append-seg path k) ao an))
                     acc  (reduce-kv (fn [acc k ov]
                                       (step acc k ov (get new k)))
                                     [acc-old acc-new] old)]
                 (reduce-kv (fn [[ao an] k nv]
                              (if (contains? old k)
                                [ao an]
                                (step [ao an] k nil nv)))
                            acc new))

               :else
               [(collect! acc-old old path)
                (collect! acc-new new path)]))]
     (let [^HashMap acc-old (HashMap.)
           ^HashMap acc-new (HashMap.)]
       (walk old new (encode-path path0) acc-old acc-new)
       [acc-old acc-new]))))

(declare get-path-strict update-pattern-cache!)

(defn- patch-path-values-mutable
  ([doc paths] (patch-path-values-mutable doc paths nil))
  ([doc paths path-filter]
  (reduce
    (fn [^HashMap acc path]
      (if-some [node (get-path-strict doc path)]
        (collect-path-values! acc node (encode-path path) path-filter)
        acc))
    (HashMap.) paths)))

;; Path ids are append-only and stored in the path-dict DBI.

(defn- init-paths
  [lmdb path-dict-dbi]
  (if-let [[_ pid] (i/get-first lmdb path-dict-dbi [:all-back] :string :int)]
    pid
    0))

(defn- init-doc-refs
  [lmdb doc-ref-dbi]
  (let [doc-refs    (sp/new-spillable-map)
        all-doc-ids (RoaringBitmap.)
        max-id      (volatile! 0)
        load        (fn [kv]
                      (let [ref (b/read-buffer (l/k kv) :data)
                            did (b/read-buffer (l/v kv) :int)]
                        (when (< ^int @max-id ^int did)
                          (vreset! max-id did))
                        (.put ^SpillableMap doc-refs did ref)
                        (b/bitmap-add all-doc-ids (int did))))]
    (visit lmdb doc-ref-dbi load [:all-back])
    [@max-id doc-refs all-doc-ids]))

(defn- open-dbis
  [lmdb domain]
  (let [doc-ref-dbi   (str domain "/" c/idoc-doc-ref)
        doc-index-dbi (str domain "/" c/idoc-doc-index)
        path-dict-dbi (str domain "/" c/idoc-path-dict)]
    (open-dbi lmdb doc-ref-dbi {:key-size c/+max-key-size+
                                :val-size c/+short-id-bytes+})
    (open-list-dbi lmdb doc-index-dbi {:key-size c/+max-key-size+
                                       :val-size c/+short-id-bytes+})
    (open-dbi lmdb path-dict-dbi {:key-size c/+max-key-size+
                                  :val-size c/+short-id-bytes+})
    [doc-ref-dbi doc-index-dbi path-dict-dbi]))

(deftype PathTrieNode [^ConcurrentHashMap children
                       ^AtomicInteger pid])

(defn- new-path-trie
  []
  (->PathTrieNode (ConcurrentHashMap.) (AtomicInteger. 0)))

(deftype IdocIndex [lmdb
                    domain
                    format
                    path-filter
                    doc-ref-dbi
                    doc-index-dbi
                    path-dict-dbi
                    ^SpillableMap doc-refs
                    ^RoaringBitmap all-doc-ids
                    ^ReentrantReadWriteLock state-lock
                    ^AtomicInteger max-doc
                    ^AtomicInteger max-path
                    path-cache
                    path-seg-cache
                    ^LRUCache pattern-cache
                    path-trie
                    ^AtomicBoolean paths-loaded
                    paths-lock
                    ^LRUCache range-cache
                    ^AtomicLong index-version])

(defn new-idoc-index
  [lmdb {:keys [domain format] :as opts}]
  (let [[doc-ref-dbi doc-index-dbi path-dict-dbi]
        (open-dbis lmdb domain)
        format                         (or format default-format)
        path-filter                    (compile-path-filter format opts)
        max-path                       (init-paths lmdb path-dict-dbi)
        [max-doc doc-refs all-doc-ids] (init-doc-refs lmdb doc-ref-dbi)
        path-cache                     (ConcurrentHashMap.)
        path-seg-cache                 (ConcurrentHashMap.)
        pattern-cache                  (LRUCache. (int c/idoc-pattern-cache-size))
        path-trie                      (new-path-trie)
        paths-loaded                   (AtomicBoolean. false)
        paths-lock                     (Object.)
        range-cache                    (LRUCache. (int c/idoc-range-cache-size))
        index-version                  (AtomicLong. 0)]
    (->IdocIndex lmdb
                 domain
                 format
                 path-filter
                 doc-ref-dbi
                 doc-index-dbi
                 path-dict-dbi
                 doc-refs
                 all-doc-ids
                 (ReentrantReadWriteLock.)
                 (AtomicInteger. max-doc)
                 (AtomicInteger. max-path)
                 path-cache
                 path-seg-cache
                 pattern-cache
                 path-trie
                 paths-loaded
                 paths-lock
                 range-cache
                 index-version)))

(defn transfer
  [^IdocIndex old lmdb]
  (->IdocIndex lmdb
               (.-domain old)
               (.-format old)
               (.-path-filter old)
               (.-doc-ref-dbi old)
               (.-doc-index-dbi old)
               (.-path-dict-dbi old)
               (.-doc-refs old)
               (.-all-doc-ids old)
               (.-state-lock old)
               (.-max-doc old)
               (.-max-path old)
               (.-path-cache old)
               (.-path-seg-cache old)
               (.-pattern-cache old)
               (.-path-trie old)
               (.-paths-loaded old)
               (.-paths-lock old)
               (.-range-cache old)
               (.-index-version old)))

(defn- invalidate-range-cache!
  [^IdocIndex index]
  (.incrementAndGet ^AtomicLong (.-index-version index))
  (.clear ^LRUCache (.-range-cache index)))

(defn- state-read-lock
  ^Lock [^IdocIndex index]
  (.readLock ^ReentrantReadWriteLock (.-state-lock index)))

(defn- state-write-lock
  ^Lock [^IdocIndex index]
  (.writeLock ^ReentrantReadWriteLock (.-state-lock index)))

(defn doc-ref-by-id
  [^IdocIndex index doc-id]
  (let [^Lock lock (state-read-lock index)]
    (.lock lock)
    (try
      (.get ^SpillableMap (.-doc-refs index) doc-id)
      (finally
        (.unlock lock)))))

(declare ids-iterate)

(def ^:private ^:const doc-ref-read-batch-size 512)

(defn doc-refs-by-ids
  "Return alternating document ids and document references for one bounded
  candidate batch."
  [^IdocIndex index doc-ids]
  (let [refs  (FastList. (* 2 (count doc-ids)))
        ^Lock lock (state-read-lock index)]
    (.lock lock)
    (try
      (doseq [doc-id doc-ids]
        (when-let [doc-ref (.get ^SpillableMap (.-doc-refs index) doc-id)]
          (.add refs doc-id)
          (.add refs doc-ref)))
      (finally
        (.unlock lock)))
    refs))

(defn ids-iterate-doc-refs
  [^IdocIndex index ids f]
  (let [batch-ids  (FastList.)
        flush!     (fn []
                     (when-not (.isEmpty batch-ids)
                       (let [^FastList batch-refs
                             (doc-refs-by-ids index batch-ids)]
                         (loop [i 0
                                n (.size batch-refs)]
                           (when (< i n)
                             (f (.get batch-refs i)
                                (.get batch-refs (unchecked-inc-int i)))
                             (recur (unchecked-add-int i 2) n)))
                         (.clear batch-ids))))]
    (ids-iterate
      ids
      (fn [doc-id]
        (.add batch-ids doc-id)
        (when (<= (long doc-ref-read-batch-size) (long (.size batch-ids)))
          (flush!))))
    (flush!)))

(defn- cache-path!
  ([^IdocIndex index path ^long pid] (cache-path! index path pid nil))
  ([^IdocIndex index path ^long pid segs]
   (let [^ConcurrentHashMap path-cache (.-path-cache index)
         ^ConcurrentHashMap seg-cache  (.-path-seg-cache index)
         segs'                         (or segs
                                            (.get seg-cache pid)
                                            (let [s (decode-path path)]
                                              (.put seg-cache pid s)
                                              s))
         ^PathTrieNode root            (.-path-trie index)]
     (.put path-cache path pid)
     (when segs'
       (.put seg-cache pid segs')
       (when root
         (let [pid-int (int pid)]
           (loop [^PathTrieNode node root
                  segs segs']
             (if (seq segs)
               (let [seg (first segs)
                     ^ConcurrentHashMap children (.-children node)
                     child (or (.get children seg)
                               (let [n (new-path-trie)]
                                 (or (.putIfAbsent children seg n) n)))]
                 (recur child (next segs)))
               (.set ^AtomicInteger (.-pid node) pid-int))))))
     pid)))

(defn- load-path-cache!
  [^IdocIndex index]
  (let [^AtomicBoolean loaded (.-paths-loaded index)]
    (when-not (.get loaded)
      (locking (.-paths-lock index)
        (when-not (.get loaded)
          (let [lmdb       (.-lmdb index)
                path-dbi   (.-path-dict-dbi index)
                ^ConcurrentHashMap seg-cache  (.-path-seg-cache index)]
            (visit lmdb path-dbi
                   (fn [kv]
                     (let [p    (b/read-buffer (l/k kv) :string)
                           pid  (b/read-buffer (l/v kv) :int)
                           segs (or (.get seg-cache pid)
                                    (let [s (decode-path p)]
                                      (.put seg-cache pid s)
                                      s))]
                       (cache-path! index p pid segs)))
                   [:all-back]))
          (.set loaded true))))))

(defn- get-path-id
  [^IdocIndex index path]
  (let [^ConcurrentHashMap path-cache (.-path-cache index)]
    (if-let [pid (.get path-cache path)]
      pid
      (let [lmdb     (.-lmdb index)
            path-dbi (.-path-dict-dbi index)
            pid      (get-value lmdb path-dbi path :string :int)]
        (when pid
          (cache-path! index path pid))
        pid))))

(defn- add-state-action!
  [^FastList state-actions action]
  (when state-actions
    (.add state-actions action)))

(defn apply-state-actions!
  [^FastList state-actions]
  (when (and state-actions (not (.isEmpty state-actions)))
    (let [^IdentityHashMap by-index (IdentityHashMap.)]
      (doseq [action state-actions]
        (let [^IdocIndex index (nth action 0)
              ^FastList actions (or (.get by-index index)
                                     (let [actions (FastList.)]
                                       (.put by-index index actions)
                                       actions))]
          (.add actions action)))
      (doseq [^Map$Entry entry (.entrySet by-index)]
        (let [^IdocIndex index (.getKey entry)
              ^FastList actions (.getValue entry)
              ^Lock lock (state-write-lock index)]
          (.lock lock)
          (try
            (doseq [action actions]
              (case (nth action 1)
                :add-doc
                (let [doc-id  (nth action 2)
                      doc-ref (nth action 3)]
                  (.put ^SpillableMap (.-doc-refs index) doc-id doc-ref)
                  (b/bitmap-add (.-all-doc-ids index) (int doc-id)))

                :update-doc-ref
                (let [doc-id  (nth action 2)
                      doc-ref (nth action 3)]
                  (.put ^SpillableMap (.-doc-refs index) doc-id doc-ref))

                :remove-doc
                (let [doc-id (nth action 2)]
                  (.remove ^SpillableMap (.-doc-refs index) (int doc-id))
                  (b/bitmap-del (.-all-doc-ids index) (int doc-id)))

                :cache-path
                (let [path (nth action 2)
                      pid  (long (nth action 3))
                      segs (nth action 4)]
                  (cache-path! index path pid segs)
                  (update-pattern-cache! index segs pid))

                :invalidate
                nil))
            (finally
              (.unlock lock)))
          (invalidate-range-cache! index))))))

(defn- ensure-path-id
  ([^IdocIndex index path ^FastList txs]
   (ensure-path-id index path txs nil))
  ([^IdocIndex index path ^FastList txs ^FastList state-actions]
   (ensure-path-id index path txs state-actions nil))
  ([^IdocIndex index path ^FastList txs ^FastList state-actions
    ^HashMap pending-paths]
   (let [^ConcurrentHashMap path-cache (.-path-cache index)
         cached                        (.get path-cache path)]
     (if cached
       cached
       (if-let [pid (or (when pending-paths
                          (.get pending-paths path))
                        (get-path-id index path))]
         pid
         (let [pid      (.incrementAndGet ^AtomicInteger (.-max-path index))
               path-dbi (.-path-dict-dbi index)]
           (when pending-paths
             (.put pending-paths path pid))
           (.add txs (l/kv-tx :put path-dbi path pid :string :int))
           (let [segs (decode-path path)]
             (if state-actions
               (add-state-action! state-actions
                                  [index :cache-path path pid segs])
               (do
                 (cache-path! index path pid segs)
                 (update-pattern-cache! index segs pid))))
           pid))))))

(defn- commit-idoc-plan!
  [^IdocIndex index res ^FastList txs ^FastList state-actions]
  (when-not (.isEmpty txs)
    (transact-kv (.-lmdb index) txs)
    (apply-state-actions! state-actions))
  res)

(defn- legacy-giant-doc-ref
  [doc-ref]
  (when (and (vector? doc-ref)
             (identical? :g (first doc-ref))
             (< 2 (count doc-ref)))
    [:g (second doc-ref)]))

(defn- doc-ref-lookups
  [doc-ref]
  (if-let [legacy-ref (legacy-giant-doc-ref doc-ref)]
    [doc-ref legacy-ref]
    [doc-ref]))

(defn- pending-doc-entry
  [^HashMap pending-doc-ids doc-ref]
  (when pending-doc-ids
    (if-let [doc-id (.get pending-doc-ids doc-ref)]
      [doc-id doc-ref]
      (when-let [legacy-ref (legacy-giant-doc-ref doc-ref)]
        (when-let [doc-id (.get pending-doc-ids legacy-ref)]
          [doc-id legacy-ref])))))

(defn- stored-doc-entry
  [^IdocIndex index doc-ref]
  (let [lmdb        (.-lmdb index)
        doc-ref-dbi (.-doc-ref-dbi index)]
    (if-let [doc-id (get-value lmdb doc-ref-dbi doc-ref :data :int)]
      [doc-id doc-ref]
      (when-let [legacy-ref (legacy-giant-doc-ref doc-ref)]
        (when-let [doc-id (get-value lmdb doc-ref-dbi legacy-ref :data :int)]
          [doc-id legacy-ref])))))

(defn- planned-doc-entry
  [^IdocIndex index doc-ref ^HashMap pending-doc-ids]
  (or (pending-doc-entry pending-doc-ids doc-ref)
      (stored-doc-entry index doc-ref)))

(defn- planned-doc-id
  [^IdocIndex index doc-ref ^HashMap pending-doc-ids]
  (some-> (planned-doc-entry index doc-ref pending-doc-ids) first))

(defn- planned-path-id
  [^IdocIndex index path ^HashMap pending-paths]
  (or (when pending-paths
        (.get pending-paths path))
      (get-path-id index path)))

(defn add-doc-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    doc-ref doc check-exist?]
   (add-doc-plan! index txs state-actions (HashMap.) (HashMap.)
                  doc-ref doc check-exist?))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths doc-ref doc check-exist?]
   (add-doc-plan! index txs state-actions pending-paths nil
                  doc-ref doc check-exist?))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths ^HashMap pending-doc-ids
    doc-ref doc check-exist?]
   (if (and check-exist?
            (planned-doc-id index doc-ref pending-doc-ids))
     :doc-exists
     (let [doc-id      (.incrementAndGet ^AtomicInteger (.-max-doc index))
           index-dbi   (.-doc-index-dbi index)
           doc-ref-dbi (.-doc-ref-dbi index)]
       ;; TODO: if doc-ref ever exceeds LMDB key size, fall back to a :g ref.
       (.add txs (l/kv-tx :put doc-ref-dbi doc-ref doc-id :data :int))
       (when pending-doc-ids
         (.put pending-doc-ids doc-ref doc-id))
       (add-state-action! state-actions [index :add-doc doc-id doc-ref])
       (doseq [[path values] (doc->path-values-mutable doc []
                                                       (.-path-filter index))
               :let          [pid (ensure-path-id index path txs
                                                  state-actions
                                                  pending-paths)]]
         (doseq [v    values
                 :let [idx (indexable-key pid v)]]
           (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int))))
       :doc-added))))

(defn add-doc
  ([index doc-ref doc] (add-doc index doc-ref doc true))
  ([^IdocIndex index doc-ref doc check-exist?]
   (let [txs           (FastList.)
         state-actions (FastList.)
         res           (add-doc-plan! index txs state-actions
                                      doc-ref doc check-exist?)]
     (commit-idoc-plan! index res txs state-actions))))

(defn- remote-doc-ref-entries
  [lmdb doc-ref-dbi docs]
  (when (instance? datalevin.remote.KVStore lmdb)
    (let [doc-refs    (map first docs)
          lookup-refs (vec (distinct (mapcat doc-ref-lookups doc-refs)))
          ids         (r/get-values lmdb doc-ref-dbi lookup-refs
                                    :data :int true)
          ref->id     (zipmap lookup-refs ids)]
      (into {}
            (keep (fn [doc-ref]
                    (some (fn [lookup-ref]
                            (when-let [doc-id (get ref->id lookup-ref)]
                              [doc-ref [doc-id lookup-ref]]))
                          (doc-ref-lookups doc-ref))))
            doc-refs))))

(defn add-docs-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions docs check-exist?]
   (add-docs-plan! index txs state-actions (HashMap.) (HashMap.)
                   docs check-exist?))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths docs check-exist?]
   (add-docs-plan! index txs state-actions pending-paths nil
                   docs check-exist?))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths ^HashMap pending-doc-ids docs check-exist?]
   (when (seq docs)
     (let [lmdb        (.-lmdb index)
           index-dbi   (.-doc-index-dbi index)
           doc-ref-dbi (.-doc-ref-dbi index)
           doc-ref-entries (when check-exist?
                             (remote-doc-ref-entries lmdb doc-ref-dbi docs))
           idx->ids    (HashMap.)]
       (doseq [[doc-ref doc] docs]
         (when-not (and check-exist?
                        (or (pending-doc-entry pending-doc-ids doc-ref)
                            (if doc-ref-entries
                              (get doc-ref-entries doc-ref)
                              (stored-doc-entry index doc-ref))))
           (let [doc-id (.incrementAndGet ^AtomicInteger (.-max-doc index))]
             (.add txs (l/kv-tx :put doc-ref-dbi doc-ref doc-id :data :int))
             (when pending-doc-ids
               (.put pending-doc-ids doc-ref doc-id))
             (add-state-action! state-actions [index :add-doc doc-id doc-ref])
             (doseq [[path values] (doc->path-values-mutable doc []
                                                             (.-path-filter index))
                     :let          [pid (ensure-path-id index path txs
                                                        state-actions
                                                        pending-paths)]]
               (doseq [v    values
                       :let [idx (indexable-key pid v)
                             ^List ids (or (.get idx->ids idx)
                                           (let [ids (FastList.)]
                                             (.put idx->ids idx ids)
                                             ids))]]
                 (.add ids doc-id))))))
       (doseq [[idx ids] idx->ids]
         (.add txs (l/kv-tx :put-list index-dbi idx ids :avg :int)))
       :docs-added))))

(defn add-docs
  ([index docs] (add-docs index docs true))
  ([^IdocIndex index docs check-exist?]
   (let [txs           (FastList.)
         state-actions (FastList.)
         res           (add-docs-plan! index txs state-actions
                                       docs check-exist?)]
     (commit-idoc-plan! index res txs state-actions))))

(defn remove-doc-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions doc-ref doc]
   (remove-doc-plan! index txs state-actions nil nil doc-ref doc))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
   ^HashMap pending-paths ^HashMap pending-doc-ids doc-ref doc]
   (when-let [[doc-id stored-ref]
              (planned-doc-entry index doc-ref pending-doc-ids)]
     (let [index-dbi (.-doc-index-dbi index)
           doc-ids   [doc-id]]
       (doseq [[path values] (doc->path-values-mutable doc []
                                                       (.-path-filter index))
               :let          [pid (planned-path-id index path pending-paths)]]
         (when pid
           (doseq [v    values
                   :let [idx (indexable-key pid v)]]
             (.add txs (l/kv-tx :del-list index-dbi idx doc-ids :avg :int)))))
       (.add txs (l/kv-tx :del (.-doc-ref-dbi index) stored-ref :data))
       (when pending-doc-ids
         (.remove pending-doc-ids doc-ref)
         (.remove pending-doc-ids stored-ref))
       (add-state-action! state-actions [index :remove-doc doc-id])
       :doc-removed))))

(defn remove-doc
  [^IdocIndex index doc-ref doc]
  (let [txs           (FastList.)
        state-actions (FastList.)
        res           (remove-doc-plan! index txs state-actions doc-ref doc)]
    (commit-idoc-plan! index res txs state-actions)))

(defn remove-docs-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions docs]
   (remove-docs-plan! index txs state-actions nil nil docs))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths ^HashMap pending-doc-ids docs]
   (when (seq docs)
     (let [lmdb           (.-lmdb index)
           index-dbi      (.-doc-index-dbi index)
           doc-ref-dbi    (.-doc-ref-dbi index)
           doc-ref-entries (remote-doc-ref-entries lmdb doc-ref-dbi docs)
           idx->ids       (HashMap.)]
       (doseq [[doc-ref doc] docs]
         (when-let [[doc-id stored-ref]
                    (or (pending-doc-entry pending-doc-ids doc-ref)
                        (if doc-ref-entries
                          (get doc-ref-entries doc-ref)
                          (stored-doc-entry index doc-ref)))]
           (doseq [[path values] (doc->path-values-mutable doc []
                                                           (.-path-filter index))
                   :let          [pid (planned-path-id index path pending-paths)]]
             (when pid
               (doseq [v    values
                       :let [idx (indexable-key pid v)
                             ^List ids (or (.get idx->ids idx)
                                           (let [ids (FastList.)]
                                             (.put idx->ids idx ids)
                                             ids))]]
                 (.add ids doc-id))))
           (.add txs (l/kv-tx :del doc-ref-dbi stored-ref :data))
           (when pending-doc-ids
             (.remove pending-doc-ids doc-ref)
             (.remove pending-doc-ids stored-ref))
           (add-state-action! state-actions [index :remove-doc doc-id])))
       (doseq [[idx ids] idx->ids]
         (.add txs (l/kv-tx :del-list index-dbi idx ids :avg :int)))
       :docs-removed))))

(defn remove-docs
  [^IdocIndex index docs]
  (let [txs           (FastList.)
        state-actions (FastList.)
        res           (remove-docs-plan! index txs state-actions docs)]
    (commit-idoc-plan! index res txs state-actions)))

(defn update-doc-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    old-ref old-doc new-ref new-doc]
   (update-doc-plan! index txs state-actions (HashMap.) (HashMap.)
                     old-ref old-doc new-ref new-doc))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths old-ref old-doc new-ref new-doc]
   (update-doc-plan! index txs state-actions pending-paths nil
                     old-ref old-doc new-ref new-doc))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths ^HashMap pending-doc-ids
    old-ref old-doc new-ref new-doc]
   (if-let [[doc-id stored-old-ref]
            (planned-doc-entry index old-ref pending-doc-ids)]
     (if (and (= old-ref new-ref) (= old-doc new-doc))
       :doc-updated
       (let [tx-count          (.size txs)
             index-dbi         (.-doc-index-dbi index)
             doc-ref-dbi       (.-doc-ref-dbi index)
             doc-ids           [doc-id]
             ^Set empty-set    (Collections/emptySet)
             [old-map new-map] (diff-path-values old-doc new-doc []
                                                  (.-path-filter index))]
         (when-not (= old-ref new-ref)
           (.add txs (l/kv-tx :del doc-ref-dbi stored-old-ref :data))
           (.add txs (l/kv-tx :put doc-ref-dbi new-ref doc-id :data :int))
           (when pending-doc-ids
             (.remove pending-doc-ids old-ref)
             (.remove pending-doc-ids stored-old-ref)
             (.put pending-doc-ids new-ref doc-id))
           (add-state-action! state-actions
                              [index :update-doc-ref doc-id new-ref]))
         (doseq [[path old-vals] old-map
                 :let            [^Set new-vals (or (.get ^HashMap new-map path)
                                                    empty-set)]]
           (when-let [pid (planned-path-id index path pending-paths)]
             (doseq [v old-vals]
               (when-not (.contains new-vals v)
                 (let [idx (indexable-key pid v)]
                   (.add txs (l/kv-tx :del-list index-dbi idx doc-ids
                                      :avg :int)))))))
         (doseq [[path new-vals] new-map
                 :let            [^Set old-vals (or (.get ^HashMap old-map path)
                                                    empty-set)]]
           (let [pid (ensure-path-id index path txs state-actions
                                     pending-paths)]
             (doseq [v new-vals]
               (when-not (.contains old-vals v)
                 (let [idx (indexable-key pid v)]
                   (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int)))))))
         (when (< tx-count (.size txs))
           (add-state-action! state-actions [index :invalidate]))
         :doc-updated))
     :doc-missing)))

(defn update-doc
  [^IdocIndex index old-ref old-doc new-ref new-doc]
  (let [txs           (FastList.)
        state-actions (FastList.)
        res           (update-doc-plan! index txs state-actions
                                        old-ref old-doc new-ref new-doc)]
    (commit-idoc-plan! index res txs state-actions)))

(defn- get-path*
  [doc segments strict?]
  (letfn [(strict-miss [seg]
            (cond
              (integer? seg)
              (raise "Idoc patch path expects vector"
                     {:segment seg :path segments})

              (or (keyword? seg) (string? seg))
              (raise "Idoc patch path expects map"
                     {:segment seg :path segments})

              :else
              (raise
                "Idoc patch path segment must be keyword, string, or integer"
                {:segment seg :path segments})))
          (step [node segs]
            (if (empty? segs)
              node
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (nil? node) nil

                  (vector? node)
                  (if (integer? seg)
                    (if (and (<= 0 (long seg)) (< (long seg) (count node)))
                      (step (nth node (long seg)) rest)
                      nil)
                    (if strict?
                      (strict-miss seg)
                      (let [vals (keep #(step % segs) node)]
                        (when (seq vals) (vec vals)))))

                  (and strict? (integer? seg))
                  (strict-miss seg)

                  (map? node)
                  (if (or (not strict?) (keyword? seg) (string? seg))
                    (step (get node seg) rest)
                    (strict-miss seg))

                  strict?
                  (strict-miss seg)

                  :else nil))))]
    (step doc segments)))

(defn- get-path-strict
  [doc segments]
  (get-path* doc segments true))

(defn patch-doc-plan!
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    old-ref old-doc new-ref new-doc patch]
   (patch-doc-plan! index txs state-actions (HashMap.) (HashMap.)
                    old-ref old-doc new-ref new-doc patch))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths old-ref old-doc new-ref new-doc {:keys [paths]}]
   (patch-doc-plan! index txs state-actions pending-paths nil
                    old-ref old-doc new-ref new-doc {:paths paths}))
  ([^IdocIndex index ^FastList txs ^FastList state-actions
    ^HashMap pending-paths ^HashMap pending-doc-ids
    old-ref old-doc new-ref new-doc {:keys [paths]}]
   (if-let [[doc-id stored-old-ref]
            (planned-doc-entry index old-ref pending-doc-ids)]
     (if (and (= old-ref new-ref) (= old-doc new-doc))
       :doc-updated
       (let [tx-count       (.size txs)
             index-dbi      (.-doc-index-dbi index)
             doc-ref-dbi    (.-doc-ref-dbi index)
             doc-ids        [doc-id]
             ^Set empty-set (Collections/emptySet)
             paths          (or paths [])
             old-map        (patch-path-values-mutable old-doc paths
                                                        (.-path-filter index))
             new-map        (patch-path-values-mutable new-doc paths
                                                        (.-path-filter index))]
         (when-not (= old-ref new-ref)
           (.add txs (l/kv-tx :del doc-ref-dbi stored-old-ref :data))
           (.add txs (l/kv-tx :put doc-ref-dbi new-ref doc-id :data :int))
           (when pending-doc-ids
             (.remove pending-doc-ids old-ref)
             (.remove pending-doc-ids stored-old-ref)
             (.put pending-doc-ids new-ref doc-id))
           (add-state-action! state-actions
                              [index :update-doc-ref doc-id new-ref]))
         (doseq [[path old-vals] old-map
                 :let            [^Set new-vals (or (.get ^HashMap new-map path)
                                                    empty-set)]]
           (when-let [pid (planned-path-id index path pending-paths)]
             (doseq [v old-vals]
               (when-not (.contains new-vals v)
                 (let [idx (indexable-key pid v)]
                   (.add txs (l/kv-tx :del-list index-dbi idx doc-ids
                                      :avg :int)))))))
         (doseq [[path new-vals] new-map
                 :let            [^Set old-vals (or (.get ^HashMap old-map path)
                                                    empty-set)]]
           (let [pid (ensure-path-id index path txs state-actions
                                     pending-paths)]
             (doseq [v new-vals]
               (when-not (.contains old-vals v)
                 (let [idx (indexable-key pid v)]
                   (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int)))))))
         (when (< tx-count (.size txs))
           (add-state-action! state-actions [index :invalidate]))
         :doc-updated))
     :doc-missing)))

(defn patch-doc
  [^IdocIndex index old-ref old-doc new-ref new-doc patch]
  (let [txs           (FastList.)
        state-actions (FastList.)
        res           (patch-doc-plan! index txs state-actions
                                       old-ref old-doc new-ref new-doc patch)]
    (commit-idoc-plan! index res txs state-actions)))

;; query evaluation

(def ^:dynamic *trace*
  "Optional tracing hook for idoc-match. When bound, it is called with a map
  of trace data after each domain scan."
  nil)

(defn- normalize-path
  [format segments]
  (if (identical? format :markdown)
    (mapv #(normalize-seg format %) segments)
    segments))

(defn- path-wildcards?
  [segments]
  (some #(and (keyword? %) (#{:? :*} %)) segments))

(defn- match-path?
  [pattern path]
  (let [p    (vec pattern)
        t    (vec path)
        lp   (count p)
        lt   (count t)
        memo (volatile! {})]
    (letfn [(step [^long i ^long j]
              (if-let [res (get @memo [i j])]
                res
                (let [res (cond
                            (= i lp) (= j lt)
                            :else
                            (let [seg (nth p (int i))]
                              (cond
                                (= seg :*)
                                (or (step (u/long-inc i) j)
                                    (and (< j lt) (step i (u/long-inc j))))

                                (= seg :?)
                                (and (< j lt)
                                     (step (u/long-inc i) (u/long-inc j)))

                                :else
                                (and (< j lt)
                                     (= seg (nth t (int j)))
                                     (step (u/long-inc i)
                                           (u/long-inc j))))))]
                  (vswap! memo assoc [i j] res)
                  res)))]
      (step 0 0))))

(defn- update-pattern-cache!
  [^IdocIndex index segs ^long pid]
  (let [^LRUCache pattern-cache (.-pattern-cache index)]
    (doseq [pattern (.keys pattern-cache)]
      (when (match-path? pattern segs)
        (when-let [cached (.get pattern-cache pattern)]
          (.put pattern-cache pattern (conj cached pid)))))))

(defn- path-expr? [x] (or (keyword? x) (string? x) (vector? x)))

(defn- path-expr->segments
  [x]
  (cond
    (keyword? x) [x]
    (string? x) [x]
    (vector? x) x
    :else (raise "Idoc path must be keyword, string, or vector" {:path x})))

(defn- strict-eq?
  [a b]
  (let [[ta a'] (value-type a)
        [tb b'] (value-type b)]
    (and (= ta tb) (= a' b'))))

(defn- pred-chain-values
  [doc-val args ^long pos]
  (let [argsv (vec args)]
    (u/concatv (subvec argsv 0 pos) [doc-val] (subvec argsv pos))))

(defn- pred-match?
  ([op doc-val args]
   (pred-match? op doc-val args 0))
  ([op doc-val args pos]
   (let [vals    (pred-chain-values doc-val args pos)
         [t0 v0] (value-type (first vals))
         cmp-ok  (fn [a b]
                   (case op
                     :>  (pos? (compare a b))
                     :>= (not (neg? (compare a b)))
                     :<  (neg? (compare a b))
                     :<= (not (pos? (compare a b)))))]
     (case op
       :nil? (identical? doc-val :json/null)
       (:> :>= :< :<=)
       (when-not (or (nil? v0) (identical? t0 :data))
         (loop [prev v0
                i    1]
           (if (>= i (count vals))
             true
             (let [[ti vi] (value-type (nth vals i))]
               (when (= t0 ti)
                 (and (cmp-ok prev vi)
                      (recur vi (u/long-inc i))))))))))))

(declare parse-predicate get-path values-for-path)

(defn- doc-matches*
  [format doc expr ctx-path]
  (cond
    (map? expr)
    (cond
      (vector? doc)
      (boolean (some #(doc-matches* format % expr ctx-path) doc))

      (map? doc)
      (every?
        (fn [[k v]]
          (let [k' (normalize-seg format k)]
            (cond
              (= k' :?)
              (boolean
                (some (fn [[ck cv]]
                        (doc-matches* format cv v
                                      (conj (or ctx-path [])
                                            (normalize-seg format ck))))
                      doc))

              (= k' :*)
              (letfn [(match-depth [node path]
                        (or (doc-matches* format node v path)
                            (cond
                              (map? node)
                              (boolean
                                (some (fn [[ck cv]]
                                        (match-depth
                                          cv
                                          (conj path (normalize-seg format ck))))
                                      node))

                              (vector? node)
                              (boolean (some #(match-depth % path) node))

                              :else false)))]
                (match-depth doc (or ctx-path [])))

              :else
              (doc-matches* format (get doc k') v (conj (or ctx-path []) k')))))
        expr)

      :else false)

    (vector? expr)
    (let [[op & rest] expr]
      (case op
        :and (every? #(doc-matches* format doc % nil) rest)
        :or  (boolean (some #(doc-matches* format doc % nil) rest))
        :not (not (doc-matches* format doc (first rest) nil))
        (raise "Unknown idoc logical operator" {:op op :expr expr})))

    (and (sequential? expr) (not (vector? expr)))
    (let [op   (keyword (first expr))
          args (rest expr)]
      (if (and (nil? ctx-path) (seq args) (some path-expr? args))
        (let [{:keys [path args pos]} (parse-predicate expr nil)
              vals                    (values-for-path format doc path)]
          (boolean (some #(pred-match? op % args pos) vals)))
        (cond
          (and (= op :nil?) (vector? doc))
          (boolean (some #(pred-match? :nil? % []) doc))

          (vector? doc)
          (boolean (some #(pred-match? op % args) doc))

          :else
          (pred-match? op doc args))))

    :else
    (cond
      (vector? doc) (boolean (some #(strict-eq? % expr) doc))
      (nil? doc)    false
      :else         (strict-eq? doc expr))))

(defn doc-ref->doc
  [lmdb doc-ref]
  (if (and (vector? doc-ref) (identical? :g (first doc-ref)))
    (let [datom (idx/gt->datom lmdb (second doc-ref))]
      (when datom (d/datom-v datom)))
    (peek doc-ref)))

(defn get-path
  [doc segments]
  (get-path* doc segments false))

(defn- values-for-path
  [format doc path]
  (let [path (normalize-path format path)]
    (if (path-wildcards? path)
      (let [path-values (doc->path-values doc)]
        (into [] cat (for [[p vals] path-values
                           :let     [segs (decode-path p)]
                           :when    (match-path? path segs)]
                       vals)))
      (let [v (get-path doc path)]
        (cond
          (nil? v)    nil
          (vector? v) v
          :else       [v])))))

(defn- indexable-key*
  [^long path-id vt v]
  (b/indexable 0 (int path-id) v vt c/g0))

(defn- ids-for-eq-path-id
  [^IdocIndex index ^long pid value]
  (let [idx (indexable-key pid value)
        bm  (RoaringBitmap.)]
    (i/visit-list (.-lmdb index) (.-doc-index-dbi index)
                  (fn [v] (.add bm (int v)))
                  idx :avg :int false)
    bm))

(defn- matching-path-ids
  [^IdocIndex index path]
  (let [^LRUCache pattern-cache (.-pattern-cache index)]
    (if-let [cached (.get pattern-cache path)]
      cached
      (do
        (load-path-cache! index)
        (let [^PathTrieNode root (.-path-trie index)
              res
              (if root
                (let [plen                  (count path)
                      ^IdentityHashMap seen (IdentityHashMap.)
                      acc                   (transient [])]
                  (letfn [(mark! [^PathTrieNode node ^long idx]
                            (let [^booleans visited
                                  (or (.get seen node)
                                      (let [arr (boolean-array
                                                  (int (inc plen)))]
                                        (.put seen node arr)
                                        arr))]
                              (if (aget visited idx)
                                false
                                (do (aset visited idx true) true))))
                          (step [^PathTrieNode node ^long idx]
                            (when (mark! node idx)
                              (if (== idx plen)
                                (let [pid (.get ^AtomicInteger (.-pid node))]
                                  (when (pos? pid)
                                    (conj! acc pid)))
                                (let [seg (nth path idx)]
                                  (cond
                                    (= seg :*)
                                    (do
                                      (step node (inc idx))
                                      (doseq [child (.values
                                                      ^ConcurrentHashMap
                                                      (.-children node))]
                                        (step child idx)))

                                    (= seg :?)
                                    (doseq [child (.values
                                                    ^ConcurrentHashMap
                                                    (.-children node))]
                                      (step child (inc idx)))

                                    :else
                                    (when-let [child (.get
                                                       ^ConcurrentHashMap
                                                       (.-children node) seg)]
                                      (step child (inc idx))))))))]
                    (step root 0)
                    (persistent! acc)))
                (let [^ConcurrentHashMap seg-cache
                      (.-path-seg-cache index)
                      ids (transient [])]
                  (doseq [^Map$Entry entry (.entrySet seg-cache)]
                    (let [pid  (long (.getKey entry))
                          segs (.getValue entry)]
                      (when (match-path? path segs)
                        (conj! ids pid))))
                  (persistent! ids)))]
          (.put pattern-cache path res)
          res)))))

(defn- ids-for-eq
  [^IdocIndex index path value]
  (if (path-wildcards? path)
    (or (b/bitmaps-or
          (map+ #(ids-for-eq-path-id index % value)
                (matching-path-ids index path)))
        (RoaringBitmap.))
    (if-let [pid (get-path-id index (encode-path path))]
      (ids-for-eq-path-id index pid value)
      (RoaringBitmap.))))

(defn- ids-for-range-path-id
  [^IdocIndex index ^long pid lo hi]
  (let [[lo-t lo-v] (when lo (value-type lo))
        [hi-t hi-v] (when hi (value-type hi))
        vt          (or lo-t hi-t)]
    (when (and lo-t hi-t (not= lo-t hi-t))
      (raise "Range bounds must have the same type" {:lo lo :hi hi}))
    (when (identical? vt :data)
      (raise "Range predicates do not support :data values" {:value (or lo hi)}))
    (let [^LRUCache range-cache          (.-range-cache index)
          ^AtomicLong index-version      (.-index-version index)

          version   (.get index-version)
          min-key   (indexable-key* pid vt c/v0)
          max-key   (indexable-key* pid vt c/vmax)
          low       (if lo (indexable-key* pid vt lo-v) min-key)
          high      (if hi (indexable-key* pid vt hi-v) max-key)
          cache-key [(b/pr-indexable low) (b/pr-indexable high)]]
      (letfn [(compute []
                (let [ids     (RoaringBitmap.)
                      visitor (fn [kv]
                                (.add ids ^int (b/read-buffer (l/v kv) :int)))]
                  (i/visit-list-key-range
                    (.-lmdb index) (.-doc-index-dbi index) visitor
                    [:closed low high] :avg :int)
                  (when (= version (.get index-version))
                    (.put range-cache cache-key [version ids]))
                  ids))]
        (if-let [cached (.get range-cache cache-key)]
          (let [[cached-version cached-ids] cached]
            (if (= cached-version version)
              cached-ids
              (compute)))
          (compute))))))

(defn- ids-for-range
  [^IdocIndex index path lo hi]
  (if (path-wildcards? path)
    (or (b/bitmaps-or
          (map+ #(ids-for-range-path-id index % lo hi)
                (matching-path-ids index path)))
        (RoaringBitmap.))
    (if-let [pid (get-path-id index (encode-path path))]
      (ids-for-range-path-id index pid lo hi)
      (RoaringBitmap.))))

(defn- parse-predicate
  [expr ctx-path]
  (let [op        (keyword (first expr))
        argsv     (vec (rest expr))
        vec-idxs  (keep-indexed
                    (fn [idx arg] (when (vector? arg) idx))
                    argsv)
        path-idxs (keep-indexed
                    (fn [idx arg] (when (path-expr? arg) idx))
                    argsv)]
    (when (and ctx-path (seq path-idxs))
      (raise "Map value predicates cannot specify a path" {:expr expr}))
    (cond
      ctx-path
      (do
        (when-not (seq ctx-path)
          (raise "Predicate requires a path" {:expr expr}))
        {:op op :path ctx-path :args argsv :pos 0})

      (empty? path-idxs)
      (raise "Predicate requires a path" {:expr expr})

      (and (seq vec-idxs) (> (count vec-idxs) 1))
      (raise "Predicate requires a single path" {:expr expr})

      :else
      (let [^long pos (if (seq vec-idxs)
                        (first vec-idxs)
                        (when (= 1 (count path-idxs)) (first path-idxs)))
            _         (when (nil? pos)
                        (raise "Predicate requires a single path" {:expr expr}))
            path      (path-expr->segments (nth argsv pos))
            args'     (vec (concat (subvec argsv 0 pos)
                                   (subvec argsv (inc pos))))]
        {:op op :path path :args args' :pos pos}))))

(defn- ids-for-predicate
  [^IdocIndex index format expr ctx-path]
  (let [{:keys [op path args ^long pos]} (parse-predicate expr ctx-path)
        path                             (normalize-path format path)
        arg-count                        (count args)
        before                           (when (pos? pos) (nth args (dec pos)))
        after                            (when (< pos arg-count) (nth args pos))
        bounds                           (case op
                                           (:< :<=) {:lo before :hi after}
                                           (:> :>=) {:lo after :hi before}
                                           :nil?    {})
        {:keys [lo hi]}                  bounds]
    (case op
      :nil?           (ids-for-eq index path :json/null)
      (:> :>= :< :<=) (do (when (and (nil? lo) (nil? hi))
                            (raise "Predicate requires bounds" {:expr expr}))
                          (ids-for-range index path lo hi))
      (raise "Unknown idoc predicate" {:op op}))))

(defn- strict-predicate-verify-ids
  [^IdocIndex index format expr ctx-path]
  (let [{:keys [op path args ^long pos]} (parse-predicate expr ctx-path)
        path                             (normalize-path format path)
        arg-count                        (count args)
        before                           (when (pos? pos) (nth args (dec pos)))
        after                            (when (< pos arg-count) (nth args pos))
        bounds                           (case op
                                           :< {:lo before :hi after}
                                           :> {:lo after :hi before}
                                           nil)]
    (when bounds
      (let [{:keys [lo hi]} bounds
            lo-ids          (when (some? lo) (ids-for-eq index path lo))
            hi-ids          (when (some? hi) (ids-for-eq index path hi))]
        (cond
          (and lo-ids hi-ids) (.or ^RoaringBitmap lo-ids ^RoaringBitmap hi-ids)
          lo-ids              lo-ids
          hi-ids              hi-ids
          :else               (RoaringBitmap.))))))

(defn- all-doc-ids
  [^IdocIndex index]
  (let [^Lock lock (state-read-lock index)]
    (.lock lock)
    (try
      (.clone ^RoaringBitmap (.-all-doc-ids index))
      (finally
        (.unlock lock)))))

(defn- and-candidate-ids
  [candidates]
  (when-let [candidates (not-empty (filterv some? candidates))]
    (b/bitmaps-and candidates)))

(defn- or-candidate-ids
  [candidates]
  (let [candidates (vec candidates)]
    (when (and (seq candidates) (every? some? candidates))
      (b/bitmaps-or candidates))))

(defn- ids-for-expr
  [^IdocIndex index format expr ctx-path]
  (cond
    (map? expr)
    (when-not (empty? expr) ;; empty query matches all documents
      (and-candidate-ids
        (map+ (fn [[k v]]
                (let [k'   (normalize-seg format k)
                      path (conj (or ctx-path []) k')]
                  (ids-for-expr index format v path)))
              expr)))

    (vector? expr)
    (let [[op & rest] expr]
      (case op
        :and (when-not (empty? rest)
               (and-candidate-ids
                 (map+ #(ids-for-expr index format % ctx-path) rest)))
        :or  (when-not (empty? rest)
               (or-candidate-ids
                 (map+ #(ids-for-expr index format % ctx-path) rest)))
        :not nil
        (raise "Unknown idoc logical operator" {:op op})))

    (and (sequential? expr) (not (vector? expr)))
    (ids-for-predicate index format expr ctx-path)

    :else
    (if (seq ctx-path)
      (do
        (when (nil? expr)
          (raise "Use (nil? :field) to match null values" {:path ctx-path}))
        (ids-for-eq index (normalize-path format ctx-path) expr))
      (raise "Idoc scalar query must be inside a map" {:expr expr}))))

(defn- index-exact-value?
  [v]
  (let [[vt v'] (value-type v)]
    (not (b/giant? (b/indexable 0 0 v' vt c/g0)))))

(defn- exact-predicate?
  [expr ctx-path]
  (let [op   (keyword (first expr))
        args (rest expr)
        args (if (and (nil? ctx-path) (some path-expr? args))
               (:args (parse-predicate expr nil))
               args)]
    (if (#{:> :<} op)
      false
      (every? index-exact-value? args))))

(defn- exact-expr?
  ([expr] (exact-expr? expr nil))
  ([expr ctx-path]
   (cond
     (map? expr)
     (let [n (count expr)]
       (cond
         (zero? n) true
        (= 1 n)
        (let [[k v] (first expr)]
          (exact-expr? v (conj (or ctx-path []) k)))
         :else false))

     (vector? expr)
     (let [op (first expr)]
       (case op
         :or  (every? #(exact-expr? % ctx-path) (rest expr))
         :and false
         :not false
         false))

     (and (sequential? expr) (not (vector? expr)))
     (exact-predicate? expr ctx-path)

     :else
     (index-exact-value? expr))))

(defn candidate-ids*
  [^IdocIndex index expr]
  (let [format  (.-format index)
        ids     (ids-for-expr index format expr nil)
        strict? (and (sequential? expr)
                     (not (vector? expr))
                     (#{:> :<} (keyword (first expr))))
        verify  (when strict?
                  (strict-predicate-verify-ids index format expr nil))
        exact?  (if strict?
                  (and (some? ids) (b/bitmap-empty? verify))
                  (and (some? ids) (exact-expr? expr)))]
    (if (nil? ids)
      {:ids (all-doc-ids index) :exact? false}
      {:ids ids :exact? exact? :verify verify})))

(defn candidate-ids
  [^IdocIndex index expr]
  (:ids (candidate-ids* index expr)))

(defn matches-doc?
  [^IdocIndex index doc expr]
  (doc-matches* (.-format index) doc expr nil))

(defn ids-count
  [ids]
  (cond
    (nil? ids)      0
    (b/bitmap? ids) (.getCardinality ^RoaringBitmap ids)
    :else           (count ids)))

(defn doc-count
  "Return the current number of documents in an idoc domain without preparing
  query candidates."
  ^long [^IdocIndex index]
  (let [^Lock lock (state-read-lock index)]
    (.lock lock)
    (try
      (.getCardinality ^RoaringBitmap (.-all-doc-ids index))
      (finally
        (.unlock lock)))))

(defn ids-empty?
  [ids]
  (cond
    (nil? ids)      true
    (b/bitmap? ids) (.isEmpty ^RoaringBitmap ids)
    :else           (empty? ids)))

(defn ids-contains?
  [ids v]
  (cond
    (nil? ids)      false
    (b/bitmap? ids) (.contains ^RoaringBitmap ids (int v))
    :else           (contains? ids v)))

(defn ids-iterate
  [ids f]
  (if (b/bitmap? ids)
    (let [iter (.getIntIterator ^RoaringBitmap ids)]
      (loop []
        (when (.hasNext iter)
          (f (.next iter))
          (recur))))
    (doseq [id ids]
      (f id))))
