(ns featured-sorter.datastore
  (:require [ring.util.json-response :as r]
            [featured-sorter.postgres :as p]
            [featured-sorter.config :refer :all]
            [clojure.data.json :as json]))

(defn init-datastore []
  (p/create-schema tagger-db app-schema)
  (p/create-app-meta-table tagger-db app-schema meta-table))

(defn create-store [http-req]
  (let [request (:body http-req)
        body (json/read-str (slurp request) :key-fn keyword)
        dataset (:dataset body)
        version (:version body)
        schema (str "\"" dataset "." version "\"")]
    (p/register tagger-db app-schema meta-table dataset version)
    (p/create-schema tagger-db schema)
    (p/create-featuretype-table tagger-db schema featuretype-table)
    (p/create-document-table tagger-db schema document-table))
  (r/json-response {:action "OK"}))

(defn list-stores []
  (let [stores (p/list-stores tagger-db app-schema meta-table)]
    (r/json-response stores)))

(defn list-featuretypes [dataset version]
  (let [featuretype-schema (p/get-schemaname tagger-db app-schema meta-table dataset version)]
    (if (some? featuretype-schema)
      (r/json-response (p/list-featuretypes tagger-db featuretype-schema featuretype-table))
      (r/json-response {:dataset "not found"}))
    ))

(defn list-versions [dataset]
  (let [versions (map #(:version %1) (p/list-versions tagger-db app-schema meta-table dataset))]
    (r/json-response {"dataset" dataset "versions" versions})))

(defn delete-store [dataset version http-req]
  (let [schema (p/get-schemaname tagger-db app-schema meta-table dataset version)]
    (p/disable-dataset tagger-db app-schema meta-table dataset version)
    (p/drop-schema tagger-db schema))
  (r/json-response {:action "OK"}))