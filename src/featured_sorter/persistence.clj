(ns featured-sorter.persistence
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [featured-sorter.config :refer :all]
            [featured-sorter.postgres :as pg]
            [featured-sorter.postgres :as p]))

(def ^:dynamic *process-database*)
(def ^:dynamic *db-schemaname*)

;(def tagger-db {:subprotocol "postgresql"
;                :subname "//localhost:5432/tagger"
;                :user "tagger"
;                :password "tagger"
;                :transaction? true})

(defn- create-processing-table [tablename]
  (let [schema-exists (pg/check-if-schema-exists *process-database* *db-schemaname*)
        table-exists (pg/check-if-table-exists *process-database* *db-schemaname* tablename)]
    (if (not schema-exists)
      (pg/create-schema *process-database* *db-schemaname*))
    (if (not table-exists)
      (pg/create-processing-table *process-database* *db-schemaname* tablename))))

(defn write-to-db [database json]
  (binding [*process-database* tagger-db
            *db-schemaname* "bag.123"]
    (let [tablename (:_collection json)
          filecontext (:filecontext json)
          ids (:ids json)]
      (create-processing-table tablename)
      (log/debug (str "write data to table: " tablename))
      (do (jdbc/insert-multi! *process-database* (str "\"" *db-schemaname* "\"." tablename) (into [] (map #(hash-map :id %1, :filecontext filecontext) ids))))
      {:features-processed (count ids) :filecontext filecontext})
    )
  )