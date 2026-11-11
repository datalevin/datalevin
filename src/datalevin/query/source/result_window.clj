(ns ^:no-doc datalevin.query.source.result-window
  "Owned, distinct result windows with bounded retained rows."
  (:require [datalevin.parser :as parser]
            [datalevin.query.execute.result :as result]
            [datalevin.query.source.control :as control]
            [datalevin.query.source.runtime :as rt]
            [datalevin.query.source.value :as value])
  (:import [datalevin.parser FindRel]
           [datalevin.utl QueryKey$KeyMap]
           [java.util Comparator PriorityQueue]))

(defn specification [parsed]
  (let [limit (:qlimit parsed) offset (or (:qoffset parsed) 0)
        names (get-in parsed [:qreturn-map :symbols])
        positions (reduce (fn [m [i v]] (if (contains? m v) m (assoc m v i)))
                          {} (map-indexed vector (parser/find-vars (:qfind parsed))))]
    (when (and (instance? FindRel (:qfind parsed)) (integer? limit) (<= 1 limit Long/MAX_VALUE)
               (integer? offset) (<= 0 offset Long/MAX_VALUE)
               (= (count names) (count (set names))))
      {:limit limit :offset offset
       :order (mapv (fn [[column direction]] {:column (if (symbol? column) (positions column) column)
                                            :direction direction}) (partition 2 (:qorder parsed)))})))

(defn collector!
  "Return a bounded collector for finite relational results, or nil. Borrowed
  rows may include :with witnesses; distinctness is on the final find tuple.
  All rows are still consumed/validated. Replaced rows release their charges."
  [runtime parsed]
  (when-let [{:keys [limit offset order]} (specification parsed)]
    (let [ctl (:control runtime) check! #(control/check-active! ctl)
          width (count (parser/find-elements (:qfind parsed)))
          capacity (+' (long limit) (long offset))
          cmp (result/order-comps get [] (mapcat (juxt :column :direction) order) check!)
          worst-first (reify Comparator (compare [_ a b] (.compare ^Comparator cmp (:key b) (:key a))))
          heap (PriorityQueue. 1 worst-first) members (QueryKey$KeyMap.)
          release! (fn [{:keys [tokens]}] (run! control/release! tokens))
          close (fn []
                  (loop [] (when-let [entry (.poll heap)] (release! entry) (recur)))
                  (.clear members))
          base (control/reserve! ctl {:retained-bytes (+ 4096 (* 128 width))})]
      (rt/own! runtime #(try (close) (finally (control/release! base))))
      {:add! (fn [row]
               (check!)
               (let [key (subvec row 0 width)]
                 (when (and (not (.containsKey members key))
                            (or (not (value/at-most? capacity (.size heap)))
                                (and (seq order) (neg? (.compare ^Comparator cmp key (:key (.peek heap)))))))
                   (when (= (.size heap) capacity)
                     (let [old (.poll heap)] (.remove members (:key old)) (release! old)))
                   (let [token (rt/charge-copy! runtime row)
                         overhead (try (control/reserve! ctl {:retained-bytes (+ 512 (* 64 width))})
                                       (catch Throwable e (control/release! token) (throw e)))]
                     (try
                       (let [owned (value/owned-copy row check!) entry {:row owned :key (subvec owned 0 width) :tokens [token overhead]}]
                         (.add heap entry) (.put members (:key entry) entry))
                       (catch Throwable e (control/release! token) (control/release! overhead) (throw e)))))))
       :rows (fn [] (check!)
               (if (<= (.size heap) (long offset)) []
                   (mapv :row (iterator-seq (.iterator heap)))))})))
