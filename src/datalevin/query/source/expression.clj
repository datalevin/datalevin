(ns ^:no-doc datalevin.query.source.expression
  "Bounded local scalar calls using the native resolver and query built-ins."
  (:require [datalevin.query.resolve :as resolve]
            [datalevin.query.source :as source]
            [datalevin.query.source.arithmetic :as arithmetic]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value])
  (:import [datalevin.utl LikeFSM]))

(def arithmetic-functions '#{+ - * / quot rem mod inc dec})
(def scalar-functions
  (into arithmetic-functions
    '#{identity ground = not= < <= > >= nil? some? zero? pos? neg? even? odd?
       like not-like in not-in}))

(defn- checked [control f]
  (fn [args]
    (source/check-active! control)
    (let [result (f args)]
      (source/check-active! control)
      result)))

(defn- membership-index! [runtime coll]
  ;; Native in uses Clojure sets, including their nil/false results and
  ;; scale-insensitive decimal equality. QueryKey sets differ here.
  (assert (and (coll? coll) (not (map? coll))) "function `in` expects a collection")
  (let [control (:control runtime)
        token (rt/charge! runtime coll 1024 4)]
    (try
      [token (if (set? coll) coll
                 (persistent!
                   (reduce (fn [index x]
                             (source/check-active! control)
                             (conj! index x)) (transient #{}) coll)))]
      (catch Throwable e (source/release! token) (throw e)))))

(defn- membership-call [runtime f ^objects args dynamic-positions]
  (let [control (:control runtime)
        static? (not (contains? dynamic-positions 1))
        index (delay (let [[token index] (membership-index! runtime (aget args 1))]
                       (rt/own! runtime #(source/release! token)) index))
        member (fn [^objects values index]
                 (let [found (index (aget values 0))
                       not? (or (= f 'not-in)
                                (and (= 3 (alength values)) (aget values 2)))]
                   (if not? (not found) found)))]
    (checked control
      (if static?
        #(member % @index)
        (fn [^objects values]
          (let [[token index] (membership-index! runtime (aget values 1))]
            (try (member values index)
                 (finally (source/release! token)))))))))

(defn- like-matcher! [runtime ^objects args]
  (let [pattern (aget args 1)
        {:keys [escape]} (when (<= 3 (alength args)) (aget args 2))
        ;; Tokens, literal segments, and compilation scratch are bounded by
        ;; UTF-16 pattern length. Use the native matcher without its global
        ;; cache, so each query owns the compiled pattern it retains.
        token (source/reserve! (:control runtime)
                {:retained-bytes (value/add-charge 4096
                                   (value/multiply-charge 128 (count pattern)))})]
    (try
      [token (if escape (LikeFSM. ^String pattern ^char escape) (LikeFSM. ^String pattern))]
      (catch Throwable e (source/release! token) (throw e)))))

(defn- like-call [runtime f ^objects args dynamic-positions]
  (let [control (:control runtime)
        static? (not (or (contains? dynamic-positions 1) (contains? dynamic-positions 2)))
        matcher (delay (let [[token matcher] (like-matcher! runtime args)]
                         (rt/own! runtime #(source/release! token)) matcher))
        match (fn [^objects values ^LikeFSM matcher]
                (let [result (.match matcher ^String (aget values 0))
                      not? (or (= f 'not-like)
                               (and (= 4 (alength values)) (aget values 3)))]
                  (if not? (not result) result)))]
    (checked control
      (if static?
        #(match % @matcher)
        (fn [values]
          (let [[token matcher] (like-matcher! runtime values)]
            (try (match values matcher)
                 (finally (source/release! token)))))))))

(defn prepare-call
  "Prepare only previously admitted flat scalar calls. The ordinary resolver
  supplies arguments and owns scalar binding, nil filtering, and equality."
  [runtime f resolved ^objects args dynamic-positions]
  (let [n (alength args)]
    (cond
      (arithmetic-functions f)
      (arithmetic/wrap-scalar runtime (resolve/make-call resolved))

      (and (#{'in 'not-in} f) (or (= n 2) (and (= f 'in) (= n 3))))
      (membership-call runtime f args dynamic-positions)

      (or (and (= f 'like) (<= 2 n 4))
          (and (= f 'not-like) (<= 2 n 3)))
      (like-call runtime f args dynamic-positions)

      :else (checked (:control runtime) (resolve/make-call resolved)))))
