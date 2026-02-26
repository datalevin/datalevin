;;
;; Copyright (c) Huahai Yang. All rights reserved.
;; The use and distribution terms for this software are covered by the
;; Eclipse Public License 2.0 (https://opensource.org/license/epl-2-0)
;; which can be found in the file LICENSE at the root of this distribution.
;; By using this software in any fashion, you are agreeing to be bound by
;; the terms of this license.
;; You must not remove this notice, or any other, from this software.
;;
(ns datalevin.main
  "Functions and vars to support the `dtlv` command line tool"
  (:refer-clojure :exclude [drop load])
  (:require
   [clojure.tools.cli :refer [parse-opts]]
   [clojure.string :as s]
   [clojure.pprint :as p]
   [clojure.stacktrace :as st]
   [sci.core :as sci]
   [datalevin.core :as d]
   [datalevin.dump :as dump]
   [datalevin.util :as u]
   [datalevin.interpret :as i]
   [datalevin.lmdb :as l]
   [datalevin.server :as srv]
   [datalevin.interface :as if]
   [pod.huahaiy.datalevin :as pod]
   [datalevin.constants :as c])
  (:import
   [java.io BufferedReader BufferedWriter PushbackReader FileOutputStream
    FileInputStream DataOutputStream DataInputStream OutputStreamWriter
    InputStreamReader])
  (:gen-class))

(def ^:private version-str
  (str
    "
  Datalevin (version: " c/version ")"))

(defn- parse-version
  "return [major minor non-breaking] version numbers"
  [s]
  (let [[major minor non-breaking] (s/split s #"\.")]
    [(Integer/parseInt major)
     (Integer/parseInt minor)
     (Integer/parseInt non-breaking)]))

(def ^:private commands
  #{"copy" "drop" "dump" "exec" "help" "load" "repl" "serv" "stat"})

(def ^:private serv-help
  "
  Command serv - run as a server.

  Optional options:
      -p --port        Listening port, default is 8898
      -r --root        Root data directory, default is /var/lib/datalevin
      -v --verbose     Show detailed logging messages

  Examples:
      dtlv -p 8899 -v serv
      dtlv -r /data/dtlv serv")

(def ^:private stat-help
  "
  Command stat - show statistics of the main database or sub-database(s).

  Required option:
      -d --dir PATH   Path to the database directory
  Optional options:
      -a --all        All of the sub-databases
  Optional arguments:
      name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb stat
      dtlv -d /data/companydb -a stat
      dtlv -d /data/companydb stat sales products")

(def ^:private dump-help
  "
  Command dump - dump the content of the database or sub-database(s).

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional options:
      -a --all        All of the sub-databases
      -f --file PATH  Write to the specified target file instead of stdout
      -g --datalog    Dump as a Datalog database
      -n --nippy      Dump database in nippy binary format
      -l --list       List the names of sub-databases instead of the content
  Optional arguments:
      Name(s) of sub-database(s)

  Examples:
      dtlv -d /data/companydb -l dump
      dtlv -d /data/companydb -g dump
      dtlv -d /data/companydb -f ~/sales-data dump sales
      dtlv -d /data/companydb -f ~/company-data -a dump")

(def ^:private load-help
  "
  Command load - load data into the database or a sub-database.

  Required option:
      -d --dir  PATH  Path to the target database directory
  Optional option:
      -f --file PATH  Load from the specified source file instead of stdin
      -g --datalog    Load a Datalog database
      -n --nippy      Load a database in nippy binary format
  Optional argument:
      Name of the single sub-database to load the data into, useful when loading
      data into a sub-database with a name different from the original name

  Examples:
      dtlv -d /data/companydb -f ~/sales-data load new-sales
      dtlv -d /data/companydb -f ~/sales-data -g load")

(def ^:private copy-help
  "
  Command copy - Copy the database. This can be done regardless of whether it is
  currently in use.

  Required option:
      -d --dir PATH   Path to the source database directory
  Optional option:
      -c --compact    Compact while copying. Only pages in use will be copied.
  Required argument:
      Path to the destination directory.

  Examples:
      dtlv -d /data/companydb -c copy /backup/companydb-2021-02-14")

(def ^:private drop-help
  "
  Command drop - Drop or clear the content of sub-database(s).

  Required option:
      -d --dir PATH   Path to the database directory
  Optional option:
      -D --delete     Delete the sub-database, not just empty it.
  Required argument:
      Name(s) of the sub-database(s)

  Examples:
      dtlv -d /data/companydb -D drop sales")

(def ^:private exec-help
  "
  Command exec - Execute database transactions or queries.

  Required argument:
      The code to be executed. The code needs to be wrapped in single quotes,
      so that the shell passes them through to Datalevin. Replace ' in query
      with (quote ...). Escape \" with \\.

  Examples:
      dtlv exec '(def conn (get-conn \"/data/companydb\")) \\
                 (transact! conn [{:name \"Datalevin\"}]) \\
                 (q (quote [:find ?e ?n :where [?e :name ?n]]) @conn) \\
                 (close conn)'")

(defn- repl-help []
  (println "")
  (println "In addition to some Clojure core functions, the following functions are available:")
  (doseq [ns    i/user-facing-ns
          :when (not= ns 'datalevin.constants)
          :let  [fs (->> ns
                         ns-publics
                         (i/user-facing-map ns)
                         keys
                         (sort-by name)
                         (partition 4 4 nil))]]
    (println "")
    (println "In namespace" ns)
    (println "")
    (doseq [f4 fs]
      (doseq [f f4]
        (printf "%-22s" (name f)))
      (println "")))
  (println "")
  (println "Can call function without namespace: (<function name> <arguments>)")
  (println "")
  (println "Type (doc <function name>) to read documentation of the function")
  "")

(def ^:private repl-header
  "
  Type (help) to see available functions. some Clojure core functions are also available.
  Type (exit) to exit.
  ")

(defn- usage [options-summary]
  (->> [version-str
        ""
        "Usage: dtlv [options] [command] [arguments]"
        ""
        "Commands:"
        "  copy  Copy a database, regardless of whether it is now in use"
        "  drop  Drop or clear a database"
        "  dump  Dump the content of a database to standard output"
        "  exec  Execute database transactions or queries"
        "  help  Show help messages"
        "  load  Load data from standard input into a database"
        "  repl  Enter an interactive shell"
        "  serv  Run as a server"
        "  stat  Display statistics of database"
        ""
        "Options:"
        options-summary
        ""
        "Type 'dtlv help <command>' to read about a specific command."
        ""
        ]
       (s/join \newline)))

(defn- error-msg [errors]
  (s/join \newline ["The following errors occurred while parsing your command:"
                    (s/join \newline errors)]))

(def default-root-dir
  (if (u/windows?)
    "C:\\ProgramData\\Datalevin"
    "/var/lib/datalevin"))

(def ^:private cli-opts
  [["-a" "--all" "Include all of the sub-databases"]
   ["-c" "--compact" "Compact while copying"]
   ["-d" "--dir PATH" "Path to the database directory"]
   ["-D" "--delete" "Delete the sub-database, not just empty it"]
   ["-f" "--file PATH" "Path to the specified file"]
   ["-g" "--datalog" "Dump/load as a Datalog database"]
   ["-h" "--help" "Show usage"]
   ["-i" "--idle-timeout IDLE_TIMEOUT" "Server session idle timeout in ms"
    :default c/default-idle-timeout
    :parse-fn #(Long/parseLong %)
    :validate [#(<= 0 ^long %) "Must be a natural number"]]
   ["-l" "--list" "List the names of sub-databases instead of the content"]
   ["-n" "--nippy" "Dump/load database in nippy binary format"]
   ["-p" "--port PORT" "Server listening port number"
    :default c/default-port
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-r" "--root ROOT" "Server root data directory"
    :default default-root-dir]
   ["-v" "--verbose" "Show verbose server debug log"]
   ["-V" "--version" "Show Datalevin version and exit"]])

(defn ^:no-doc validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]}
        (parse-opts args cli-opts)

        command (first arguments)
        pod?    (= "true" (System/getenv "BABASHKA_POD"))]
    (cond
      pod?               {:command "pods"}
      (:version options) {:exit-message version-str :ok? true}
      (:help options)    {:exit-message (usage summary) :ok? true}
      errors             {:exit-message (str (error-msg errors)
                                             \newline
                                             (usage summary))}
      (commands command) {:command   command
                          :options   options
                          :arguments (rest arguments)
                          :summary   summary}
      (nil? command)     {:command "repl"}
      :else              {:exit-message (usage summary)})))

(defn- exit
  ([]
   (exit 0))
  ([status]
   (System/exit status))
  ([status msg]
   (println msg)
   (System/exit status)))

(defn- dtlv-help [arguments summary]
  (if (seq arguments)
    (let [command (s/lower-case (first arguments))]
      (exit 0 (case command
                "repl" (repl-help)
                "exec" exec-help
                "copy" copy-help
                "drop" drop-help
                "dump" dump-help
                "load" load-help
                "stat" stat-help
                (str "Unknown command: " command))))
    (exit 0 (usage summary))))

(defn exec
  "Execuate code that passed in as a seq of strings."
  [arguments]
  (i/exec-code (s/join (if (seq arguments)
                         arguments
                         (doall (line-seq (BufferedReader. *in*)))))))

(defn- dtlv-exec [arguments]
  (try
    (exec arguments)
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Execution error: " (.getMessage e)))))
  (exit 0))

(defn copy
  "Copy a database. `src-dir` is the source data directory path. `dest-dir` is
  the destination data directory path. Will compact while copying if
  `compact?` is true."
  [src-dir dest-dir compact?]
  (let [txlog-dir (str src-dir u/+separator+ "txlog")
        opts      (when (u/file-exists txlog-dir)
                    {:txn-log? true})
        lmdb      (l/open-kv src-dir opts)]
    (try
      (if/copy lmdb dest-dir compact?)
      (finally
        (if/close-kv lmdb)))))

(defn- run-copy [{:keys [dir compact]} arguments]
  (assert dir (s/join \newline
                      ["Missing source data directory path." copy-help]))
  (assert (seq arguments)
          (s/join \newline
                  ["Missing destination data directory path." copy-help]))
  (let [copy-meta (copy dir (first arguments) compact)]
    (when (map? copy-meta)
      (println "Copy metadata:" (pr-str copy-meta)))))

(defn- dtlv-copy [options arguments]
  (try
    (run-copy options arguments)
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Copy error: " (.getMessage e)))))
  (exit 0))

(defn drop
  "Drop (when `delete` is true) or clear (when `delete` is false) the list of
  sub-database(s) named by `dbis` from the database at the data directory path
  `dir`."
  [dir dbis delete]
  (let [lmdb (l/open-kv dir)]
    (if delete
      (doseq [dbi dbis]
        (if/drop-dbi lmdb dbi))
      (doseq [dbi dbis]
        (if/clear-dbi lmdb dbi)))
    (if/close-kv lmdb)))

(defn- dtlv-drop [{:keys [dir delete]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." drop-help]))
  (assert (seq arguments)
          (s/join \newline ["Missing sub-database name." drop-help]))
  (try
    (drop dir arguments delete)
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Drop error: " (.getMessage e)))))
  (exit 0))

(defn dump
  "Dump database content. `src-dir` is the database directory path.

  The content will be written to `dest-file` if given, or to stdout.

  If `list?` is true, will list the names of the key-value sub-databases only,
  not the content.

  If `datalog?` is true, will dump the whole database as a Datalog store,
  including the schema and all the datoms. By default, datoms are printed as
  strings. If there are unprintable values in DB, set `nippy?` to `true`.

  If `all?` is true, will dump raw data of all the key-value sub-databases,
  unless `datalog?` is true.

  If `nippy?` is true, dump is in nippy binary format, and `dest-file` must be
  given.

  If `dbis` is not empty, will dump raw data of only the named key-value sub-databases,
  unless `datalog?` is true."
  ([src-dir dest-file dbis list? datalog? all?]
   (dump src-dir dest-file dbis list? datalog? all? false))
  ([src-dir dest-file dbis list? datalog? all? nippy?]
   (assert (if nippy? dest-file true) "dest-file must be given for nippy")
   (let [f (when dest-file (FileOutputStream. ^String dest-file))
         w (when (and f (not nippy?))
             (BufferedWriter. (OutputStreamWriter. f)))
         d (when (and f nippy?) (DataOutputStream. f))
         o *out*]
     (binding [*out* (or w o)]
       (cond
         list?      (let [lmdb (l/open-kv src-dir)]
                      (l/dump-dbis-list lmdb d)
                      (if/close-kv lmdb))
         datalog?   (let [conn (d/create-conn src-dir)]
                      (dump/dump-datalog conn d)
                      (d/close conn))
         all?       (let [lmdb (l/open-kv src-dir)]
                      (l/dump-all lmdb d)
                      (if/close-kv lmdb))
         (seq dbis) (let [lmdb (l/open-kv src-dir)]
                      (doseq [dbi dbis] (l/dump-dbi lmdb dbi d))
                      (if/close-kv lmdb))
         :else      (binding [*out* o] (println dump-help))))
     (when w (.flush w) (.close w))
     (when d (.flush d) (.close d)))))

(defn- dtlv-dump [{:keys [dir all file datalog list]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." dump-help]))
  (try
    (dump dir file arguments list datalog all)
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Dump error: " (.getMessage e)))))
  (exit 0))

(defn load
  "Load content into the database at data directory path `dir`,
  from `src-file` if given, or from stdin.

  If `datalog?` is true, the content are expected to be schema and datoms,
  otherwise they are expected to be key-value data.

  If `nippy?` is true, load a nippy binary file, `src-file` must be given.

  Will load raw data into the named sub-database `dbi` if given. "
  ([dir src-file dbi datalog?]
   (load dir src-file dbi datalog? false))
  ([dir src-file dbi datalog? nippy?]
   (assert (if nippy? src-file true) "src-file must be given for nippy")
   (let [f  (when src-file (FileInputStream. ^String src-file))
         r  (when (and f (not nippy?)) (InputStreamReader. f))
         in (if (and f nippy?)
              (DataInputStream. f)
              (PushbackReader. (or r *in*)))]
     (cond
       datalog? (dump/load-datalog dir in {} {} nippy?)
       dbi      (let [lmdb (l/open-kv dir)]
                  (l/load-dbi lmdb dbi in nippy?)
                  (if/close-kv lmdb))
       :else    (let [lmdb (l/open-kv dir)]
                  (l/load-all lmdb in nippy?)
                  (if/close-kv lmdb)))
     (when f (.close f)))))

(defn- dtlv-load [{:keys [dir file datalog]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." load-help]))
  (try
    (load dir file (first arguments) datalog)
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Load error: " (.getMessage e)))))
  (exit 0))

;; TODO show reader info and free list info as well
(defn- dtlv-stat [{:keys [dir all]} arguments]
  (assert dir (s/join \newline ["Missing data directory path." stat-help]))
  (try
    (let [lmdb (l/open-kv dir)
          dbis (if all (if/list-dbis lmdb) arguments)]
      (if (seq dbis)
        (p/pprint (cond-> []
                    all  (conj {"Main DB" (if/stat lmdb)})
                    true (into (for [dbi  dbis
                                     :let [_ (if/open-dbi lmdb dbi)]]
                                 {dbi (if/stat lmdb dbi)}))))
        (p/pprint {"Main DB" (if/stat lmdb)}))
      (if/close-kv lmdb))
    (catch Throwable e
      (st/print-cause-trace e)
      (exit 1 (str "Stat error: " (.getMessage e)))))
  (exit 0))

(defn- prompt [ctx]
  (let [ns-name (sci/eval-string* ctx "(ns-name *ns*)")]
    (print (str ns-name "> "))
    (flush)))

(defn- handle-error [_ last-error e]
  (binding [*out* *err*] (println (ex-message e)))
  (sci/set! last-error e))

(defn- document [s]
  (when-let [f (i/resolve-var s)]
    (let [m (meta f)]
      (println " -------------------------")
      (println (str (ns-name (:ns m)) "/" (:name m)))
      (println (:arglists m))
      (println (:doc m)))))

(defn- dtlv-repl []
  (println version-str)
  (println repl-header)
  (let [reader     (sci/reader *in*)
        last-error (sci/new-dynamic-var '*e nil
                                        {:ns (sci/create-ns 'clojure.core)})
        ctx        (sci/init (update i/sci-opts :namespaces
                                     merge {'clojure.core {'*e last-error}}))]
    (sci/with-bindings {sci/ns     @sci/ns
                        last-error @last-error}
      (loop []
        (prompt ctx)
        (let [next-form (try (sci/parse-next ctx reader)
                             (catch Throwable e
                               (handle-error ctx last-error e)
                               ::err))]
          (cond
            (#{'(exit) '(quit)} next-form ) (exit)
            (= next-form '(help))           (do (repl-help) (recur))

            (and (list? next-form) (= ((comp name first) next-form) "doc"))
            (do (document (first (next next-form))) (recur))

            :else (when-not (= ::sci/eof next-form)
                    (when-not (= ::err next-form)
                      (let [res (try (i/eval-fn ctx next-form)
                                     (catch Throwable e
                                       (handle-error ctx last-error e)
                                       ::err))]
                        (when-not (= ::err res)
                          (prn res))))
                    (recur))))))))

(defn -main [& args]
  (let [{:keys [command options arguments summary exit-message ok?]}
        (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (case command
        "copy" (dtlv-copy options arguments)
        "drop" (dtlv-drop options arguments)
        "dump" (dtlv-dump options arguments)
        "exec" (dtlv-exec arguments)
        "help" (dtlv-help arguments summary)
        "load" (dtlv-load options arguments)
        "pods" (pod/run)
        "repl" (dtlv-repl)
        "serv" (srv/start (srv/create options))
        "stat" (dtlv-stat options arguments)))))
