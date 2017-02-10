(ns featured-sorter.postgres
  (:require [clojure.java.jdbc :as j]
            [clojure.tools.logging :as log]
            [cheshire.core :as json])
  (:import (org.postgresql.util PGobject)
           (java.sql Types)))

(extend-protocol j/ISQLParameter
  clojure.lang.PersistentArrayMap
  (set-parameter [v ^java.sql.PreparedStatement statement ^long index]
    (let [conn (.getConnection statement)]
      (.setObject statement index (doto (PGobject.)
                                    (.setType "json")
                                    (.setValue (json/generate-string v)))))))

(extend-protocol j/IResultSetReadColumn
  PGobject
  (result-set-read-column [v _ _]
    (let [type  (.getType v)
          value (.getValue v)]
          (case type
            "json" (json/parse-string value)
            :else value))))

(defn create-schema [db schemaname]
  (log/debug (str "creating schema: " schemaname ))
  (let [create-schema-ddl (str "CREATE SCHEMA IF NOT EXISTS " schemaname";")]
    (j/execute! db [create-schema-ddl])))

(defn check-if-schema-exists [db schemaname]
  (log/debug (str "check if schema exists: " schemaname ))
  (let [check-if-schema-exists-ddl (str "SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname = '" schemaname"');")]
    (-> (j/query db [check-if-schema-exists-ddl])
        first
        :exists)))

(defn drop-schema [db schemaname]
  (log/debug (str "drop schema: " schemaname ))
  (let [drop-schema-ddl (str "DROP SCHEMA IF EXISTS " schemaname" CASCADE;")]
    (j/execute! db [drop-schema-ddl])))

(defn list-tables-db [db schemaname]
  (let [ddl (str "SELECT schemaname AS schema_name, tablename AS table_name FROM pg_catalog.pg_tables WHERE schemaname = '" schemaname "'")]
    (j/query db [ddl])))

(defn create-processing-table [db schemaname tablename]
  (log/debug (str "creating table: " schemaname "." tablename))
  (let [create-ddl (str "CREATE TABLE IF NOT EXISTS " schemaname "." tablename " (gid BIGSERIAL PRIMARY KEY, id TEXT, filecontext JSON);")]
    (j/execute! db [create-ddl])))

(defn drop-table [db schemaname tablename]
  (log/debug (str "drop table: " schemaname "."tablename))
  (let [drop-ddl (str "DROP TABLE IF EXISTS" schemaname "." tablename ";")]
    (j/execute! db [drop-ddl])))

(defn check-if-table-exists [db schemaname tablename]
  (log/debug (str "check if table exists: " schemaname "." tablename))
  (let [exists-ddl (str "SELECT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname = '" schemaname "' AND tablename = '" tablename "');")]
    (-> (j/query db [exists-ddl])
        first
        :exists)))
