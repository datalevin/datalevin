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
   [datalevin.util :as u :refer [raise map+]]
   [jsonista.core :as json]
   [nextjournal.markdown :as md])
  (:import
   [java.util IdentityHashMap HashSet HashMap Collections List Map$Entry Set]
   [java.util.concurrent ConcurrentHashMap]
   [java.util.concurrent.atomic AtomicBoolean AtomicInteger AtomicLong]
   [org.eclipse.collections.impl.map.mutable.primitive IntObjectHashMap]
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
                          (assoc m k (walk v next-depth)))
                        {} x))))

                (vector? x)
                (walk-coll x depth (fn [d] (mapv #(walk % d) x)))

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
  (if-let [idx (first (keep-indexed (fn [i seg]
                                      (when (integer? seg) i))
                                    path))]
    (subvec path 0 idx)
    path))

(defn- path-prefix?
  [prefix path]
  (and (<= (count prefix) (count path))
       (= prefix (subvec path 0 (count prefix)))))

(defn- minimize-path-infos
  [path-infos]
  (let [uniq   (vals (into {} (map (juxt :path identity)) path-infos))
        sorted (sort-by (comp count :path) uniq)]
    (reduce (fn [acc pinfo]
              (if (some #(path-prefix? (:path %) (:path pinfo)) acc)
                acc
                (conj acc pinfo)))
            [] sorted)))

(defn- normalize-patch-op
  [op]
  (when-not (sequential? op)
    (raise "Idoc patch op must be sequential" {:op op}))
  (let [[kind path & args] op
        path               (normalize-patch-path path)]
    (case kind
      :set    (do
                (when-not (= 1 (count args))
                  (raise "Idoc patch :set expects exactly one value"
                         {:op op :path path :args args}))
                {:op :set :path path :value (first args)})
      :unset  (do
                (when (seq args)
                  (raise "Idoc patch :unset does not take extra args"
                         {:op op :path path :args args}))
                {:op :unset :path path})
      :update (let [[update-op & uargs] args]
                (when-not (patch-update-ops update-op)
                  (raise "Unknown idoc patch update op"
                         {:op op :update-op update-op}))
                {:op :update :path path :update-op update-op :args uargs})

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

(defn- assoc-in-idoc
  [doc path value]
  (letfn [(step [node segs ctx]
            (if (empty? segs)
              value
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (integer? seg)
                  (let [v   (cond
                              (vector? node) node
                              (nil? node)
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg})
                              :else
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg}))
                        idx (long seg)]
                    (when (or (neg? idx) (>= idx (count v)))
                      (raise "Idoc patch index out of bounds"
                             {:path path :segment seg :size (count v)}))
                    (assoc v idx (step (nth v idx) rest (conj ctx seg))))

                  (or (keyword? seg) (string? seg))
                  (let [m (cond
                            (nil? node) {}
                            (map? node) node
                            :else       (raise "Idoc patch path expects map"
                                               {:path path :segment seg}))]
                    (assoc m seg (step (get m seg) rest (conj ctx seg))))

                  :else
                  (raise
                    "Idoc patch path segment must be keyword, string, or integer"
                    {:path path :segment seg})))))]
    (step doc path [])))

(defn- update-in-idoc
  [doc path f]
  (letfn [(step [node segs ctx]
            (if (empty? segs)
              (f node)
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (integer? seg)
                  (let [v   (cond
                              (vector? node) node
                              (nil? node)
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg})
                              :else
                              (raise "Idoc patch path expects vector"
                                     {:path path :segment seg}))
                        idx (long seg)]
                    (when (or (neg? idx) (>= idx (count v)))
                      (raise "Idoc patch index out of bounds"
                             {:path path :segment seg :size (count v)}))
                    (assoc v idx (step (nth v idx) rest (conj ctx seg))))

                  (or (keyword? seg) (string? seg))
                  (let [m (cond
                            (nil? node) {}
                            (map? node) node
                            :else       (raise "Idoc patch path expects map"
                                               {:path path :segment seg}))]
                    (assoc m seg (step (get m seg) rest (conj ctx seg))))

                  :else
                  (raise
                    "Idoc patch path segment must be keyword, string, or integer"
                    {:path path :segment seg})))))]
    (step doc path [])))

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
  (let [ops  (cond
               (nil? ops) []
               (sequential? ops) ops
               :else (raise "Idoc patch ops must be sequential" {:ops ops}))
        norm (mapv normalize-patch-op ops)
        doc'  (reduce
                (fn [m {:keys [op path value update-op args]}]
                  (case op
                    :set    (assoc-in-idoc m path value)
                    :unset  (unset-in-idoc m path)
                    :update (update-in-idoc
                              m path #(apply-update-op % update-op args))))
                doc norm)
        paths (minimize-path-infos
                (map (fn [{:keys [path]}]
                       {:path (root-path path)})
                     norm))]
    {:doc doc' :paths paths}))

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

(defn- doc->path-values-mutable
  ([doc] (doc->path-values-mutable doc []))
  ([doc path0]
   (letfn [(append-seg [^String path seg]
             (if (keyword? seg)
               (str path "/:" (subs (str seg) 1))
               (str path "/" (encode-string-seg seg))))
           (add-leaf [^HashMap acc ^String path v]
             (let [^HashSet s (or (.get acc path)
                                  (let [s (HashSet.)]
                                    (.put acc path s)
                                    s))]
               (.add s v))
             acc)
           (walk [^HashMap acc node ^String path]
             (cond
               (nil? node)    acc
               (map? node)    (reduce-kv (fn [a k v]
                                           (walk a v (append-seg path k)))
                                         acc node)
               (vector? node) (reduce (fn [a v] (walk a v path)) acc node)
               :else          (add-leaf acc path node)))]
     (walk (HashMap.) doc (encode-path path0)))))

(defn- merge-path-values!
  [^HashMap acc ^HashMap m]
  (doseq [[path vals] m]
    (let [^HashSet s (or (.get acc path)
                         (let [s (HashSet.)]
                           (.put acc path s)
                           s))]
      (.addAll s vals)))
  acc)

(defn- diff-path-values
  ([old new] (diff-path-values old new []))
  ([old new path0]
   (letfn [(append-seg [^String path seg]
             (if (keyword? seg)
               (str path "/:" (subs (str seg) 1))
               (str path "/" (encode-string-seg seg))))
           (add-leaf [^HashMap acc ^String path v]
             (let [^HashSet s (or (.get acc path)
                                  (let [s (HashSet.)]
                                    (.put acc path s)
                                    s))]
               (.add s v))
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
  [doc paths]
  (reduce
    (fn [^HashMap acc {:keys [path]}]
      (if-let [node (get-path-strict doc path)]
        (merge-path-values! acc (doc->path-values-mutable node path))
        acc))
    (HashMap.) paths))

;; Path ids are append-only and stored in the path-dict DBI.

(defn- init-paths
  [lmdb path-dict-dbi]
  (if-let [[_ pid] (i/get-first lmdb path-dict-dbi [:all-back] :string :int)]
    pid
    0))

(defn- init-doc-refs
  [lmdb doc-ref-dbi]
  (let [doc-refs    (IntObjectHashMap.)
        all-doc-ids (RoaringBitmap.)
        max-id      (volatile! 0)
        load        (fn [kv]
                      (let [ref (b/read-buffer (l/k kv) :data)
                            did (b/read-buffer (l/v kv) :int)]
                        (when (< ^int @max-id ^int did) (vreset! max-id did))
                        (.put doc-refs (int did) ref)
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
                    doc-ref-dbi
                    doc-index-dbi
                    path-dict-dbi
                    doc-refs
                    ^RoaringBitmap all-doc-ids
                    ^AtomicInteger max-doc
                    ^AtomicInteger max-path
                    path-cache
                    path-seg-cache
                    pattern-cache
                    path-trie
                    ^AtomicBoolean paths-loaded
                    paths-lock
                    range-cache
                    ^AtomicLong index-version])

(defn new-idoc-index
  [lmdb {:keys [domain format] :as _opts}]
  (let [[doc-ref-dbi doc-index-dbi path-dict-dbi] (open-dbis lmdb domain)
        max-path                          (init-paths lmdb path-dict-dbi)
        [max-doc doc-refs all-doc-ids]    (init-doc-refs lmdb doc-ref-dbi)
        path-cache                        (ConcurrentHashMap.)
        path-seg-cache                    (ConcurrentHashMap.)
        pattern-cache                     (ConcurrentHashMap.)
        path-trie                         (new-path-trie)
        paths-loaded                      (AtomicBoolean. false)
        paths-lock                        (Object.)
        range-cache                       (ConcurrentHashMap.)
        index-version                     (AtomicLong. 0)]
    (->IdocIndex lmdb
                 domain
                 format
                 doc-ref-dbi
                 doc-index-dbi
                 path-dict-dbi
                 doc-refs
                 all-doc-ids
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
               (.-doc-ref-dbi old)
               (.-doc-index-dbi old)
               (.-path-dict-dbi old)
               (.-doc-refs old)
               (.-all-doc-ids old)
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
  (let [^ConcurrentHashMap range-cache (.-range-cache index)]
    (when-not (.isEmpty range-cache)
      (.clear range-cache))))

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

(defn- ensure-path-id
  [^IdocIndex index path ^FastList txs]
  (let [^ConcurrentHashMap path-cache (.-path-cache index)
        cached                        (.get path-cache path)]
    (if cached
      cached
      (if-let [pid (get-path-id index path)]
        pid
        (let [pid      (.incrementAndGet ^AtomicInteger (.-max-path index))
              path-dbi (.-path-dict-dbi index)]
          (.add txs (l/kv-tx :put path-dbi path pid :string :int))
          (let [segs (decode-path path)]
            (cache-path! index path pid segs)
            (update-pattern-cache! index segs pid))
          pid)))))

(defn add-doc
  ([index doc-ref doc] (add-doc index doc-ref doc true))
  ([^IdocIndex index doc-ref doc check-exist?]
   (if (and check-exist?
            (get-value (.-lmdb index) (.-doc-ref-dbi index)
                       doc-ref :data :int))
     :doc-exists
     (let [txs         (FastList.)
           doc-id      (.incrementAndGet ^AtomicInteger (.-max-doc index))
           index-dbi   (.-doc-index-dbi index)
           doc-ref-dbi (.-doc-ref-dbi index)]
       ;; TODO: if doc-ref ever exceeds LMDB key size, fall back to a :g ref.
       (.add txs (l/kv-tx :put doc-ref-dbi doc-ref doc-id :data :int))
       (.put ^IntObjectHashMap (.-doc-refs index) (int doc-id) doc-ref)
       (b/bitmap-add (.-all-doc-ids index) (int doc-id))
       (doseq [[path values] (doc->path-values-mutable doc)
               :let          [pid (ensure-path-id index path txs)]]
         (doseq [v    values
                 :let [idx (indexable-key pid v)]]
           (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int))))
       (transact-kv (.-lmdb index) txs)
       (invalidate-range-cache! index)
       :doc-added))))

(defn- remote-doc-ref-ids
  [lmdb doc-ref-dbi docs]
  (when (instance? datalevin.remote.KVStore lmdb)
    (zipmap (map first docs)
            (r/get-values lmdb doc-ref-dbi (map first docs) :data :int true))))

(defn add-docs
  ([index docs] (add-docs index docs true))
  ([^IdocIndex index docs check-exist?]
   (when (seq docs)
     (let [txs         (FastList.)
           lmdb        (.-lmdb index)
           index-dbi   (.-doc-index-dbi index)
           doc-ref-dbi (.-doc-ref-dbi index)
           doc-refs    (.-doc-refs index)
           doc-ref-ids (when check-exist?
                         (remote-doc-ref-ids lmdb doc-ref-dbi docs))
           idx->ids    (HashMap.)]
       (doseq [[doc-ref doc] docs]
         (when-not (and check-exist?
                        (if doc-ref-ids
                          (get doc-ref-ids doc-ref)
                          (get-value lmdb doc-ref-dbi doc-ref :data :int)))
           (let [doc-id (.incrementAndGet ^AtomicInteger (.-max-doc index))]
             (.add txs (l/kv-tx :put doc-ref-dbi doc-ref doc-id :data :int))
             (.put ^IntObjectHashMap doc-refs (int doc-id) doc-ref)
             (b/bitmap-add (.-all-doc-ids index) (int doc-id))
             (doseq [[path values] (doc->path-values-mutable doc)
                     :let          [pid (ensure-path-id index path txs)]]
               (doseq [v    values
                       :let [idx (indexable-key pid v)
                             ^List ids (or (.get idx->ids idx)
                                           (let [ids (FastList.)]
                                             (.put idx->ids idx ids)
                                             ids))]]
                 (.add ids doc-id))))))
       (doseq [[idx ids] idx->ids]
         (.add txs (l/kv-tx :put-list index-dbi idx ids :avg :int)))
       (when-not (.isEmpty txs)
         (transact-kv lmdb txs)
         (invalidate-range-cache! index))
       :docs-added))))

(defn remove-doc
  [^IdocIndex index doc-ref doc]
  (when-let [doc-id (get-value (.-lmdb index) (.-doc-ref-dbi index)
                               doc-ref :data :int)]
    (let [txs       (FastList.)
          index-dbi (.-doc-index-dbi index)]
      (doseq [[path values] (doc->path-values-mutable doc)
              :let          [pid (get-path-id index path)]]
        (when pid
          (doseq [v    values
                  :let [idx (indexable-key pid v)]]
            (.add txs (l/kv-tx :del-list index-dbi idx [doc-id] :avg :int)))))
      (.add txs (l/kv-tx :del (.-doc-ref-dbi index) doc-ref :data))
      (.remove ^IntObjectHashMap (.-doc-refs index) (int doc-id))
      (b/bitmap-del (.-all-doc-ids index) (int doc-id))
      (transact-kv (.-lmdb index) txs)
      (invalidate-range-cache! index)
      :doc-removed)))

(defn remove-docs
  [^IdocIndex index docs]
  (when (seq docs)
    (let [txs         (FastList.)
          lmdb        (.-lmdb index)
          index-dbi   (.-doc-index-dbi index)
          doc-ref-dbi (.-doc-ref-dbi index)
          doc-refs    (.-doc-refs index)
          doc-ref-ids (remote-doc-ref-ids lmdb doc-ref-dbi docs)
          idx->ids    (HashMap.)]
      (doseq [[doc-ref doc] docs]
        (when-let [doc-id (if doc-ref-ids
                            (get doc-ref-ids doc-ref)
                            (get-value lmdb doc-ref-dbi doc-ref :data :int))]
          (doseq [[path values] (doc->path-values-mutable doc)
                  :let          [pid (get-path-id index path)]]
            (when pid
              (doseq [v    values
                      :let [idx (indexable-key pid v)
                            ^List ids (or (.get idx->ids idx)
                                          (let [ids (FastList.)]
                                            (.put idx->ids idx ids)
                                            ids))]]
                (.add ids doc-id))))
          (.add txs (l/kv-tx :del doc-ref-dbi doc-ref :data))
          (.remove ^IntObjectHashMap doc-refs (int doc-id))
          (b/bitmap-del (.-all-doc-ids index) (int doc-id))))
      (doseq [[idx ids] idx->ids]
        (.add txs (l/kv-tx :del-list index-dbi idx ids :avg :int)))
      (when-not (.isEmpty txs)
        (transact-kv lmdb txs)
        (invalidate-range-cache! index))
      :docs-removed)))

(defn update-doc
  [^IdocIndex index old-ref old-doc new-ref new-doc]
  (if-let [doc-id (get-value (.-lmdb index) (.-doc-ref-dbi index)
                             old-ref :data :int)]
    (if (and (= old-ref new-ref) (= old-doc new-doc))
      :doc-updated
      (let [txs               (FastList.)
            lmdb              (.-lmdb index)
            index-dbi         (.-doc-index-dbi index)
            doc-ref-dbi       (.-doc-ref-dbi index)
            doc-refs          (.-doc-refs index)
            ^Set empty-set    (Collections/emptySet)
            [old-map new-map] (diff-path-values old-doc new-doc)]
        (when-not (= old-ref new-ref)
          (.add txs (l/kv-tx :del doc-ref-dbi old-ref :data))
          (.add txs (l/kv-tx :put doc-ref-dbi new-ref doc-id :data :int))
          (.put ^IntObjectHashMap doc-refs (int doc-id) new-ref))
        (doseq [[path old-vals] old-map
                :let            [^Set new-vals (or (.get ^HashMap new-map path)
                                                   empty-set)]]
          (when-let [pid (get-path-id index path)]
            (doseq [v old-vals]
              (when-not (.contains new-vals v)
                (let [idx (indexable-key pid v)]
                  (.add txs (l/kv-tx :del-list index-dbi idx [doc-id]
                                     :avg :int)))))))
        (doseq [[path new-vals] new-map
                :let            [^Set old-vals (or (.get ^HashMap old-map path)
                                                   empty-set)]]
          (let [pid (ensure-path-id index path txs)]
            (doseq [v new-vals]
              (when-not (.contains old-vals v)
                (let [idx (indexable-key pid v)]
                  (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int)))))))
        (when-not (.isEmpty txs)
          (transact-kv lmdb txs)
          (invalidate-range-cache! index))
        :doc-updated))
    :doc-missing))

(defn- get-path-strict
  [doc segments]
  (letfn [(step [node segs]
            (if (empty? segs)
              node
              (let [seg  (first segs)
                    rest (rest segs)]
                (cond
                  (nil? node) nil

                  (integer? seg)
                  (if (vector? node)
                    (if (and (<= 0 (long seg)) (< (long seg) (count node)))
                      (step (nth node (long seg)) rest)
                      nil)
                    (raise "Idoc patch path expects vector"
                           {:segment seg :path segments}))

                  (or (keyword? seg) (string? seg))
                  (if (map? node)
                    (step (get node seg) rest)
                    (raise "Idoc patch path expects map"
                           {:segment seg :path segments}))

                  :else
                  (raise
                    "Idoc patch path segment must be keyword, string, or integer"
                    {:segment seg :path segments})))))]
    (step doc segments)))

(defn patch-doc
  [^IdocIndex index old-ref old-doc new-ref new-doc {:keys [paths]}]
  (if-let [doc-id (get-value (.-lmdb index) (.-doc-ref-dbi index)
                             old-ref :data :int)]
    (if (and (= old-ref new-ref) (= old-doc new-doc))
      :doc-updated
      (let [txs            (FastList.)
            lmdb           (.-lmdb index)
            index-dbi      (.-doc-index-dbi index)
            doc-ref-dbi    (.-doc-ref-dbi index)
            doc-refs       (.-doc-refs index)
            ^Set empty-set (Collections/emptySet)
            paths          (or paths [])
            old-map        (patch-path-values-mutable old-doc paths)
            new-map        (patch-path-values-mutable new-doc paths)]
        (when-not (= old-ref new-ref)
          (.add txs (l/kv-tx :del doc-ref-dbi old-ref :data))
          (.add txs (l/kv-tx :put doc-ref-dbi new-ref doc-id :data :int))
          (.put ^IntObjectHashMap doc-refs (int doc-id) new-ref))
        (doseq [[path old-vals] old-map
                :let            [^Set new-vals (or (.get ^HashMap new-map path)
                                                   empty-set)]]
          (when-let [pid (get-path-id index path)]
            (doseq [v old-vals]
              (when-not (.contains new-vals v)
                (let [idx (indexable-key pid v)]
                  (.add txs (l/kv-tx :del-list index-dbi idx [doc-id]
                                     :avg :int)))))))
        (doseq [[path new-vals] new-map
                :let            [^Set old-vals (or (.get ^HashMap old-map path)
                                                   empty-set)]]
          (let [pid (ensure-path-id index path txs)]
            (doseq [v new-vals]
              (when-not (.contains old-vals v)
                (let [idx (indexable-key pid v)]
                  (.add txs (l/kv-tx :put index-dbi idx doc-id :avg :int)))))))
        (when-not (.isEmpty txs)
          (transact-kv lmdb txs)
          (invalidate-range-cache! index))
        :doc-updated))
    :doc-missing))

;; query evaluation

(def ^:dynamic *trace*
  "Optional tracing hook for idoc-match. When bound, it is called with a map
  of trace data after each domain scan."
  nil)

(defn- normalize-seg
  [format seg]
  (if (identical? format :markdown)
    (cond
      (and (keyword? seg) (#{:? :*} seg)) seg
      (keyword? seg)                      (normalize-header (subs (str seg) 1))
      (string? seg)                       (normalize-header seg)
      :else                               seg)
    seg))

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
  (let [^ConcurrentHashMap pattern-cache (.-pattern-cache index)]
    (when-not (.isEmpty pattern-cache)
      (doseq [^Map$Entry entry (.entrySet pattern-cache)]
        (let [pattern (.getKey entry)]
          (when (match-path? pattern segs)
            (.put pattern-cache pattern
                  (conj (.getValue entry) pid))))))))

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
  (letfn [(step [node segs]
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
                    (let [vals (keep #(step % segs) node)]
                      (when (seq vals) (vec vals))))

                  (map? node)
                  (step (get node seg) rest)

                  :else nil))))]
    (step doc segments)))

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
  (let [^ConcurrentHashMap pattern-cache (.-pattern-cache index)]
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
    (let [^ConcurrentHashMap range-cache (.-range-cache index)
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
  (.clone ^RoaringBitmap (.-all-doc-ids index)))

(defn- ids-for-expr
  [^IdocIndex index format expr ctx-path]
  (cond
    (map? expr)
    (when-not (empty? expr) ;; empty query matches all documents
      (b/bitmaps-and
        (map+ (fn [[k v]]
                (let [k'   (normalize-seg format k)
                      path (conj (or ctx-path []) k')]
                  (ids-for-expr index format v path)))
              expr)))

    (vector? expr)
    (let [[op & rest] expr]
      (case op
        :and (when-not (empty? rest)
               (b/bitmaps-and
                 (map+ #(ids-for-expr index format % ctx-path) rest)))
        :or  (when-not (empty? rest)
               (b/bitmaps-or
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
