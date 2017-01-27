(ns featured-sorter.read-files
  (:require [clojure.java.jdbc :as jdbc]
           [clojure.data.json :as json]
           [clojure.tools.logging :as log]
           [featured-sorter.postgres :as pg]
           [nl.pdok.util.ziptools :as z])
  (:import (java.util.zip ZipFile ZipInputStream)
           (java.io FileInputStream)))

(def ^:dynamic *new-file-feature-count* "5000")

(def ^:dynamic *process-database*)
(def ^:dynamic *db-schemaname*)

(defn create-processing-table [tablename]
  (let [schema-exists (pg/check-if-schema-exists *process-database* *db-schemaname*)
        table-exists (pg/check-if-table-exists *process-database* *db-schemaname* tablename)]
    (if (not schema-exists)
      (pg/create-schema *process-database* *db-schemaname*))
    (if (not table-exists)
      (pg/create-processing-table *process-database* *db-schemaname* tablename))))

(defn select-ids [features]
  (distinct (map #(get %1 :_id) features)))

(defn proces-file-to-db
  ([file]
   (proces-file-to-db file (.getParent file) (.getName file) (z/is-zip file)))
  ([file fileparent filename compressed]
   (let [file-to-proces (json/read-str (slurp file) :key-fn keyword)
         tablename (str (clojure.string/lower-case (name (-> file-to-proces :features first :_collection))))
         ids-in-file (-> file-to-proces :features select-ids)]
     (create-processing-table tablename)
     (log/info (str "write data to table: " tablename))
     (jdbc/insert-multi! *process-database* tablename (into [] (map #(hash-map :id %1, :filecontext {:path (str fileparent) :file (str filename) :compressed compressed}) ids-in-file))))))

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

(defn determine-new-filename-and-contentsource [tablename]
  (let [agg-tablename (str tablename"_agg")
        create-new-table (str "CREATE TABLE " agg-tablename " (gid BIGSERIAL, id TEXT PRIMARY KEY, filecontext JSON, newfile TEXT);")
        agg-table (str "INSERT INTO " agg-tablename " (id, filecontext, newfile) SELECT id, array_to_json(array_agg(filecontext)) filecontext, null newfile FROM " tablename " GROUP BY id ORDER BY id;")
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

(defn proces-files [files db schemaname]
  (binding [*process-database* db
            *db-schemaname* schemaname]
    (doall (for [file files]
             (proces-file file)))
    (doall (map #(determine-new-filename-and-contentsource (:table_name %1)) (pg/list-tables-db *process-database* *db-schemaname*)))))