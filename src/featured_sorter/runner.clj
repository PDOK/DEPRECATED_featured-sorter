(ns featured-sorter.runner
  (require [clojure.java.jdbc :as jdbc]
           [clojure.data.json :as json]
           [clojure.tools.cli :refer [parse-opts]]
           [clojure.java.io :as io]
           [clojure.tools.logging :as log]
           [cheshire.core :refer :all]
           [clj-time.jdbc]
           [nl.pdok.util.ziptools :as z]
           )
  (:gen-class)
  (:import (java.util.zip ZipFile ZipInputStream)
           (java.io FileInputStream)))

(def ^:dynamic *db-name* "default")
(def ^:dynamic *db-user* "tagger")
(def ^:dynamic *db-password* *db-user*)

(def ^:dynamic *process-database*)
(def ^:dynamic *target-directory*)
(def ^:dynamic *database-directory* "./data")

(def ^:dynamic *new-file-feature-count* "5000")

;(def ^:dynamic *db-options* "DB_CLOSE_DELAY=-1")
;(def tagger-db
;  {:classname   "org.h2.Driver"
;   :subprotocol (str "h2:" *database-directory* "/" *db-name*)
;   ;:subprotocol (str "h2:mem:data")
;   :subname     (str *db-name* ";" *db-options*)
;   :user        *db-user*
;   :password    *db-password*})

(def tagger-db {:subprotocol "postgresql"
                   :subname "//localhost:5432/tagger"
                   :user *db-user*
                   :password *db-password*
                   :transaction? true})

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn create-processing-table [tablename]
  (let [drop-ddl (str "DROP TABLE IF EXISTS" tablename ";")
        create-ddl (str "CREATE TABLE IF NOT EXISTS " tablename " (gid BIGSERIAL PRIMARY KEY, id TEXT, filecontext TEXT);")]
    ;(jdbc/execute! *process-database* [drop-ddl])
    (jdbc/execute! *process-database* [create-ddl])))

(defn list-tables-db []
  (let [ddl (str "SELECT schemaname AS schema_name, tablename AS table_name FROM pg_catalog.pg_tables WHERE tableowner = '" *db-user* "'")]
    (jdbc/query *process-database* [ddl])))

(defn check-if-directory-exists [directory]
  (.exists (io/file directory)))

(defn check-and-make-directory [directory]
  (if (check-if-directory-exists directory)
    true
    (.mkdir (java.io.File. directory))))

(defn select-ids [features]
  (distinct (map #(get %1 :_id) features)))

(defn proces-file-to-db
  ([file]
    (proces-file-to-db file (.getParent file) (.getName file) (z/is-zip file)))
  ([file fileparent filename compressed]
   (let [file-to-proces (json/read-str (slurp file) :key-fn keyword)
         tablename (clojure.string/upper-case (name (-> file-to-proces :features first :_collection)))
         ids-in-file (-> file-to-proces :features select-ids)]
     (create-processing-table tablename)
     (jdbc/insert-multi! *process-database* tablename (into [] (map #(hash-map :id %1, :filecontext (generate-string {:path (str fileparent) :file (str filename) :compressed compressed})) ids-in-file))))))

(defn proces-zipfile-to-db [file]
  (let [zip (ZipFile. file)
        entries (z/list-entries zip)]
    (doall (map #(proces-file-to-db (z/get-entry zip %1) (str (.getParent file) "\\" (.getName file)) (.getName %1) true) entries))
    ))

(defn proces-file [file]
  (log/info "processing: " (.getName file))
  (let [zipped? (z/is-zip file)]
    (if (= true zipped?)
      (proces-zipfile-to-db file)
      (proces-file-to-db file))))

(defn get-content-from-sourcefiles [files]
  (flatten (doall (for [file files]
                    (:features (json/read-str (slurp file) :key-fn keyword))))))

(defn filter-only-features-for-file [features ids]
  (filter #(let [feature %1]
             (if (.contains (into [] ids) (:_id feature))
               feature
               )) features))

(defn group-features-by-id [features ids]
  (for [id ids]
    (filter #(= id (:_id %1)) features)))

(defn mark-change [features]
  (reduce-kv (fn [result key feature]
               (let [previous (last result)]
                 (conj result (assoc feature :_validity (:_valid_from feature)
                                             :_current_validity (:_valid_from previous)
                                             :_action "change"))))
             []
             (into [] features)))

(defn mark-new [features]
  (concat (conj () (-> features
                       first
                       (assoc :_action "new"
                              :_validity (:_valid_from (-> features first)))))
          (-> features rest)))

(defn mark-close [features]
  (let [last-feature (last features)]
    (if (not (some nil? (vals (select-keys last-feature [:_valid_from :_valid_to]))))
      (if (not= "new" (:_action last-feature))
        (concat features
                (conj () (-> features
                             last
                             (assoc :_action "close"
                                    :_validity (:_valid_to last-feature)
                                    :_current_validity (:_valid_from last-feature)))))
        features)
      features)))

(defn remove-keys [keys features]
  (map #(apply dissoc %1 keys) features))

(defn write-json-to-file [featuretype filename features]
  (check-and-make-directory (str *target-directory* "/" featuretype ))
  (with-open [writer (io/writer (str *target-directory* "/" featuretype "/" filename) :append true)]
    (.write writer (str "{\"dataset\":\"" featuretype "\",\n\"features\":["))
    (let [first? (ref true)]
      (doseq [f features]
        (if-not @first?
          (.write writer ",\n")
          (dosync (ref-set first? false)))
        (.write writer (cheshire.core/generate-string f)))
      (.write writer "]}"))))

(defn associate-features [feature-group]
  (->> feature-group
       (sort-by :_valid_from)
       (mark-change)
       (mark-new)
       (mark-close)
       (remove-keys (into [] [:_valid_from :_valid_to]))))

(defn write-features-to-file [tablename filename features]
  (->> features
       (write-json-to-file tablename filename))
  )

(defn determine-content-newfile [tablename filename]
  (log/info (str "create file: " filename))
  (let [sql (str "SELECT id, filecontext FROM " tablename " WHERE newfile = '" filename "';")
        select-features-for-newfile (jdbc/query *process-database* [sql])
        _ (println select-features-for-newfile)
        ids (map #(get %1 :id) select-features-for-newfile)
        distinct-source-files (-> (map #(clojure.string/split (:files %1) #",")
                                       (distinct select-features-for-newfile))
                                  flatten
                                  distinct)]
    (write-features-to-file (clojure.string/lower-case tablename)
                            (clojure.string/lower-case filename)
                            (flatten (doall (map associate-features (-> distinct-source-files
                                                                        get-content-from-sourcefiles
                                                                        (filter-only-features-for-file ids)
                                                                        (group-features-by-id ids))))))))

(defn determine-content-newfiles-for-feature [tablename]
  (log/info (str "proces feature to files: " tablename))
  (let [sql (str "SELECT DISTINCT newfile FROM " tablename ";")]
    (doall (map #(determine-content-newfile tablename (:newfile %1)) (jdbc/query *process-database* [sql])))))

(defn write-new-files [target-dir]
  (binding [*target-directory* target-dir]
    (doall (map #(determine-content-newfiles-for-feature (:table_name %1)) (list-tables-db)))))

(defn determine-new-filename-and-contentsource [tablename]
  (let [agg-tablename (str tablename"_agg")
        create-new-table (str "CREATE TABLE " agg-tablename " (gid BIGSERIAL, id TEXT PRIMARY KEY, filecontext TEXT[], newfile TEXT);")
        agg-table (str "INSERT INTO " agg-tablename " (id, filecontext, newfile) SELECT id, array_agg(filecontext) filecontext, null newfile FROM " tablename " GROUP BY id ORDER BY id;")
        update-newfile-column (str "UPDATE " agg-tablename " y SET newfile = x.newfile FROM (SELECT '" tablename "-' ||LPAD((((row_number() over () -1) / " *new-file-feature-count* ") + 1)::text, 6, '0' )||'.json' newfile, id FROM " agg-tablename ") x WHERE y.id = x.id;")
        create-newfile-index (str "CREATE INDEX " tablename "_idx ON " agg-tablename "(newfile)")
        drop-old-table (str "DROP TABLE " tablename ";")
        alter-tablename (str "ALTER TABLE " agg-tablename " RENAME TO " tablename ";")]
    (jdbc/execute! *process-database* [create-new-table])
    (jdbc/execute! *process-database* [agg-table])
    (log/info (str "data aggregate for: " tablename))
    (jdbc/execute! *process-database* [update-newfile-column])
    (jdbc/execute! *process-database* [create-newfile-index])
    (jdbc/execute! *process-database* [drop-old-table])
    (jdbc/execute! *process-database* [alter-tablename])
    (log/info (str "new files determined for: " tablename))))

(defn database-files []
  "Move to seperate h2 impl of a datastore protocol"
  (let [database-directory *database-directory*
        database-file (str *database-directory* "/" *db-name*)]
    (if (.exists (io/as-file database-directory))
      (do
        (if (.exists (io/as-file database-directory))
          (do
            (io/delete-file database-file)))
        (io/delete-file *database-directory*)))))

(defn proces-files [files]
  (doall (for [file files]
           (proces-file file)))
  (doall (map #(determine-new-filename-and-contentsource (:table_name %1)) (list-tables-db))))

(defn proces-json-from-filesystem [source-dir target-dir]
  (log/info "start processing all files for filesystem")
  (binding [*process-database* tagger-db]
    (let [files-for-directory (file-seq (io/file source-dir))
          files-to-proces (filter (fn [file-or-directory] (not (.isDirectory file-or-directory))) files-for-directory)]
      (proces-files files-to-proces)
      (write-new-files target-dir)
      (log/info "done processing all files for filesystem"))))

(defn implementation-version []
  (if-let [version (System/getProperty "featured-sorter.version")]
    version
    (-> ^java.lang.Class (eval 'featured-sorter.runner) .getPackage .getImplementationVersion)))

(def cli-options
  [["-s" "--source" "source directory"]
   ["-t" "--target" "target directory"]
   ["-v" "--version"]])

(defn -main [& args]
  (let [{:keys [options arguments errors]} (parse-opts args cli-options)]
    (cond
      (:version options)
        (exit 0 (implementation-version))
      :else
        (if (= (count arguments) 2)
          (let [source-dir (first arguments)
                target-dir (second arguments)]
            (if (check-if-directory-exists source-dir)
              (if (check-and-make-directory target-dir)
                (proces-json-from-filesystem source-dir target-dir)
                (exit 0 "Cannot make target dir"))
              (exit 0 "No source dir")))
          (exit 0 "Something went wrong")))))

