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
  (let [check-if-schema-exists-ddl (str "SELECT EXISTS (SELECT * FROM pg_catalog.pg_namespace WHERE nspname = '" schemaname "');")]
    (-> (j/query db [check-if-schema-exists-ddl])
        first
        :exists)))

(defn drop-schema [db schemaname]
  (log/debug (str "drop schema: " schemaname ))
  (let [drop-schema-ddl (str "DROP SCHEMA IF EXISTS \"" schemaname "\" CASCADE;")]
    (j/execute! db [drop-schema-ddl])))

(defn disable-dataset [db app-schema meta-table dataset version]
  (let [update-meta-table (str "UPDATE \"" app-schema "\".\"" meta-table "\" SET cleaned = true WHERE dataset = '" dataset "' AND version = '" version "';")]
    (j/execute! db [update-meta-table])))

(defn create-app-meta-table [db schemaname tablename]
  (log/debug (str "create metadata table: " schemaname ))
  (let [create-metatable-ddl (str "CREATE TABLE IF NOT EXISTS \"" schemaname "\"." tablename " (id BIGSERIAL PRIMARY KEY, schemaname TEXT, dataset TEXT, version TEXT, cleaned BOOLEAN DEFAULT false, CREATED_ON TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);")]
    (j/execute! db [create-metatable-ddl])))

(defn create-featuretype-table [db schemaname tablename]
  (log/debug (str "create featuretype table: " tablename ))
  (let [create-featuretype-table-ddl (str "CREATE TABLE IF NOT EXISTS \"" schemaname "\"." tablename "(id BIGSERIAL PRIMARY KEY, featuretype TEXT, tablename TEXT, CREATED_ON TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")]
    (j/execute! db [create-featuretype-table-ddl])))

(defn create-document-table [db schemaname tablename]
  (log/debug (str "create document table: " tablename ))
  (let [create-document-table-ddl (str "CREATE TABLE IF NOT EXISTS \"" schemaname "\"." tablename "(id BIGSERIAL PRIMARY KEY, documentname TEXT, document JSONB, CREATED_ON TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")]
    (j/execute! db [create-document-table-ddl])))

(defn register [db schemaname tablename dataset version]
  (log/debug (str "register dataset with sorter"))
  (let [register-dataset-sql (str "INSERT INTO \"" schemaname "\"." tablename " (schemaname, dataset, version) VALUES ('" (str dataset "." version) "', '" dataset "', '" version "');")]
    (j/execute! db [register-dataset-sql])))

(defn get-schemaname [db schemaname tablename dataset version]
  (log/debug (str "select schemaname dataset: " dataset " version: " version))
  (let [get-schemaname-sql (str "SELECT schemaname FROM \"" schemaname "\"." tablename " WHERE dataset = '" dataset "' AND version = '" version "' AND cleaned = false;" )]
    (:schemaname (first (j/query db [get-schemaname-sql])))))

(defn list-featuretypes [db featuretype-schema featuretype-table]
  (log/debug (str "list featuretypes dataset: " featuretype-schema))
  (let [list-featuretypes-sql (str "SELECT featuretype FROM \"" featuretype-schema "\"." featuretype-table ";")]
    (j/query db [list-featuretypes-sql])))

(defn list-stores [db schemaname tablename]
  (log/debug (str "list all stores"))
  (let [list-stores-ddl (str "SELECT DISTINCT dataset FROM \"" schemaname "\"."  tablename "; ")]
    (j/query db [list-stores-ddl])))

(defn list-versions [db schemaname tablename dataset]
  (log/debug (str "list all version of dataset: " dataset))
  (let [list-versions-ddl (str "SELECT version FROM \"" schemaname "\"."  tablename " WHERE dataset = '" dataset "';")]
    (j/query db [list-versions-ddl])))

(defn list-tables-db [db schemaname]
  (let [ddl (str "SELECT schemaname AS schema_name, tablename AS table_name FROM pg_catalog.pg_tables WHERE schemaname = '" schemaname "'")]
    (j/query db [ddl])))

(defn create-processing-table [db schemaname tablename]
  (log/debug (str "creating table: " schemaname "." tablename))
  (let [create-ddl (str "CREATE TABLE IF NOT EXISTS \"" schemaname "\"." tablename " (gid BIGSERIAL PRIMARY KEY, id TEXT, filecontext JSON);")]
    (j/execute! db [create-ddl])))

(defn drop-table [db schemaname tablename]
  (log/debug (str "drop table: " schemaname "."tablename))
  (let [drop-ddl (str "DROP TABLE IF EXISTS \"" schemaname "\"." tablename ";")]
    (j/execute! db [drop-ddl])))

(defn check-if-table-exists [db schemaname tablename]
  (log/debug (str "check if table exists: \"" schemaname "\"." tablename))
  (let [exists-ddl (str "SELECT EXISTS (SELECT * FROM pg_catalog.pg_tables WHERE schemaname = '" schemaname "' AND tablename = '" tablename "');")]
    (-> (j/query db [exists-ddl])
        first
        :exists)))
