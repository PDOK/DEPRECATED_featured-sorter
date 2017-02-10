(ns featured-sorter.sorter
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [featured-sorter.postgres :as pg]))

(def ^:dynamic *process-database*)
(def ^:dynamic *db-schemaname*)
(def ^:dynamic *new-file-feature-count* 5000)

(defn- determine-new-filename-and-contentsource [tablename]
  (let [agg-tablename (str tablename"_agg")
        create-new-table (str "CREATE TABLE " agg-tablename " (gid BIGSERIAL, id TEXT PRIMARY KEY, filecontext JSON, newfile TEXT);")
        agg-to-table (str "INSERT INTO " agg-tablename " (id, filecontext, newfile) SELECT id, array_to_json(array_agg(filecontext)) filecontext, null newfile FROM " tablename " GROUP BY id ORDER BY id;")
        update-newfile-column (str "UPDATE " agg-tablename " y SET newfile = x.newfile FROM (SELECT '" tablename "-' ||LPAD((((row_number() over () -1) / " *new-file-feature-count* ") + 1)::text, 6, '0' )||'.json' newfile, id FROM " agg-tablename ") x WHERE y.id = x.id;")
        create-newfile-index (str "CREATE INDEX " tablename "_idx ON " agg-tablename "(newfile)")
        drop-table (str "DROP TABLE IF EXISTS " tablename ";")
        rename-table (str "ALTER TABLE " agg-tablename " RENAME TO " tablename ";")]
    (jdbc/execute! *process-database* [create-new-table])
    (jdbc/execute! *process-database* [agg-to-table])
    (log/debug (str "data aggregate for: " tablename))
    (jdbc/execute! *process-database* [update-newfile-column])
    (jdbc/execute! *process-database* [create-newfile-index])
    (jdbc/execute! *process-database* [drop-table])
    (jdbc/execute! *process-database* [rename-table])
    (log/debug (str "new files determined for: " tablename))))

(defn sort-files [db schemaname]
  (binding [*process-database* db
            *db-schemaname* schemaname]
    (doall (map #(determine-new-filename-and-contentsource (:table_name %1)) (pg/list-tables-db *process-database* *db-schemaname*)))))
