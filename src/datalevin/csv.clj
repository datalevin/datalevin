;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns ^:no-doc datalevin.csv
  "CSV reader and writer"
  (:require
   [clojure.string :as s])
  (:import
   [datalevin.utl CSVReader]
   [java.io Reader Writer StringReader]))

(defn read-csv
  [input & options]
  (let [{:keys [separator quote]
         :or   {separator \, quote \"}} options
        ^Reader reader (cond
                         (string? input) (StringReader. input)
                         (instance? Reader input) input
                         :else
                         (throw
                           (IllegalArgumentException.
                             "CSV input must be a String or java.io.Reader")))]
    (lazy-seq
      (map vec
           (iterator-seq
             (CSVReader. reader (char separator) (char quote)))))))

;; minor modification from clojure.data.csv, included for convenience

;; Copyright (c) Jonas Enlund. All rights reserved.  The use and
;; distribution terms for this software are covered by the Eclipse
;; Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;; which can be found in the file epl-v10.html at the root of this
;; distribution.  By using this software in any fashion, you are
;; agreeing to be bound by the terms of this license.  You must not
;; remove this notice, or any other, from this software.

(defn- write-cell [^Writer writer obj quote quote?]
  (let [string     (str obj)
	      must-quote (quote? string)]
    (when must-quote (.write writer (int quote)))
    (.write writer (if must-quote
		                 (s/escape string {quote (str quote quote)})
		                 string))
    (when must-quote (.write writer (int quote)))))

(defn- write-record [^Writer writer record sep quote quote?]
  (loop [record record]
    (when-first [cell record]
      (write-cell writer cell quote quote?)
      (when-let [more (next record)]
	      (.write writer (int sep))
	      (recur more)))))

(defn- write-csv*
  [^Writer writer records sep quote quote? ^String newline]
  (loop [records records]
    (when-first [record records]
      (write-record writer record sep quote quote?)
      (.write writer newline)
      (recur (next records)))))

(defn write-csv
  [writer data & options]
  (let [opts      (apply hash-map options)
        separator (or (:separator opts) \,)
        quote     (or (:quote opts) \")
        quote?    (or (:quote? opts)
                      (let [should-quote #{separator quote \return \newline}]
                        #(some should-quote %)))
        newline   (or (:newline opts) :lf)]
    (write-csv* writer
		            data
		            separator
		            quote
                quote?
		            ({:lf "\n" :cr+lf "\r\n"} newline))))
