(ns featured-sorter.config)

"TODO: "
"something with reading config file"
"something with pushing db(/store) config throgh webservices"

(def tagger-db {:subprotocol "postgresql"
                :subname "//localhost:5432/feature-sorter"
                :user "pdok_sorter"
                :password "pdok_sorter"
                :transaction? true})

(def app-schema "app")

(def meta-table "datasets")

(def featuretype-table "featuretypes")

(def document-table "documents")