(ns ^:no-doc datalevin.query.access.dependency
  "Typed dependency scheduling for complete access and local operator plans.
  This planner consumes declared schemas, never rows, estimates or cursors.
  Its caller owns validation, canonical choice order and workspace admission."
  (:require [clojure.set :as set])
  (:import [clojure.lang BigInt]))

(defn supplies?
  "Whether every value declared by from is admitted by to. Complete logical
  types are compared without coercion or inference from observed values. :data
  admits any already validated logical type."
  [from to]
  (and from (or (= (:type from) (:type to)) (= :data (:type to)))
       (or (:nullable? to) (not (:nullable? from)))))

(defn extend-columns
  "Extend a conjunction's schema after a selected operator. A join can remove
  nulls and narrow :data by a declared fact type, but cannot establish equality
  between incompatible complete types."
  [known columns]
  (reduce (fn [known column]
            (when known
              (let [variable (:variable column) before (get known variable)]
                (when (or (nil? before) (= (:type before) (:type column))
                          (= :data (:type before)) (= :data (:type column)))
                  (assoc known variable
                         (assoc column :type (if (and before (= :data (:type column)))
                                               (:type before) (:type column))
                                :nullable? (and (:nullable? column)
                                                (get before :nullable? true))))))))
          known columns))

(defn schedule
  "Search choices in their caller-supplied canonical order. Each choice has
  :covers (nonempty work IDs), :requires/:columns (typed vectors), :after (work
  IDs), and :resources (exclusive resource IDs). :unavailable? excludes a spent
  permission. A choice can only add bindings within this conjunction; a scoped
  operator must declare only its public exports, with its body kept private.

  inputs are actual seeds, not the union of possible outputs. required-work
  includes local operators as well as accesses. check! and reserve! run before
  retaining search state; the caller releases all workspace on return/failure.
  Iterative DFS avoids consuming the JVM stack for long dependency chains.
  Optional visit! accounts for each admitted search frame, independently of
  cancellation checks while scanning choices. Returns :complete, :infeasible,
  or :bound without executing any choice."
  [{:keys [inputs required-work required-vars choices check! reserve! max-states visit!]}]
  (check!)
  ;; One admitted scratch envelope covers transitions and memo probes, including
  ;; transitions immediately discarded without retaining a new search frame.
  (let [columns (long (reduce (fn [^long n choice] (+ n (count (:columns choice))))
                              (count inputs) choices))
        base (+ 1024 (* 128 (count required-work)))
        scaled (*' 512 columns)]
    (reserve! (if (instance? Long scaled)
                (+' base (long scaled))
                (.add (BigInt/valueOf base) ^BigInt scaled))))
  (let [frame! (fn [known done used index choice]
                 (check!)
                 (reserve! (+ 512 (* 256 (count known)) (* 64 (count done)) (* 128 (count used))))
                 (when visit! (visit!))
                 {:known known :done done :used used :index index :choice choice})
        initial (frame! (into {} (map (juxt :variable identity)) inputs) #{} #{} 0 nil)]
    (loop [stack [initial] failed #{} visited 1]
      (check!)
      (if-let [{:keys [known done used index] :as frame} (peek stack)]
        (cond
          (and (= done required-work) (set/subset? required-vars (set (keys known))))
          {:status :complete :choices (mapv :choice (next stack)) :columns known}

          (= index (count choices))
          (do (reserve! 128)
              (recur (pop stack) (conj failed [known done used]) visited))

          :else
          (let [{:keys [covers requires columns after resources unavailable?] :as choice}
                (nth choices index)
                stack (assoc stack (dec (count stack)) (assoc frame :index (inc (long index))))
                next-known
                (when (and (not unavailable?)
                           (empty? (set/intersection covers done))
                           (set/subset? after done)
                           (empty? (set/intersection resources used))
                           (every? #(supplies? (get known (:variable %)) %) requires))
                  (extend-columns known columns))]
            (if next-known
              (let [next-done (into done covers) next-used (into used resources)]
                (if (contains? failed [next-known next-done next-used])
                  (recur stack failed visited)
                  (if (>= visited (long max-states))
                    {:status :bound}
                    (recur (conj stack (frame! next-known next-done next-used 0 choice))
                           failed (inc visited)))))
              (recur stack failed visited))))
        {:status :infeasible}))))
