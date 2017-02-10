(ns featured-sorter.runner
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [nl.pdok.util.ziptools :as z]
            [nl.pdok.util.filesystem :as f]
            [featured-sorter.processor :as p])
  (:gen-class))

(def ^:dynamic *db-user* "tagger")
(def ^:dynamic *db-password* *db-user*)
;(def ^:dynamic *db-schemaname* *db-user*)

(def tagger-db {:subprotocol "postgresql"
                   :subname "//localhost:5432/tagger"
                   :user *db-user*
                   :password *db-password*
                   :transaction? true})

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn implementation-version []
  (if-let [version (System/getProperty "featured-sorter.version")]
    version
    (-> ^java.lang.Class (eval 'featured-sorter.runner) .getPackage .getImplementationVersion)))

(def cli-options
  [["-h" "--help"]
   ["-v" "--version"]
   ["-s" "--source" "source directory"]
   ["-t" "--target" "target directory"]])

(def cli-usage
  (->> ["FEATURE-SORTER"
        ""
        "Usage: program-name [options]"
        ""
        "Options:"
        cli-options
        ""]
       (s/join \newline)
       ))

(defn -main [& args]
  (let [{:keys [options arguments errors]} (parse-opts args cli-options)]
    (cond
      (:version options)
      (exit 0 (implementation-version))
      (:help options)
      (exit 0 cli-usage))
    ;(= (= true (:target options)) (= true (:source options)))
    ;  (proces-json-from-filesystem )
    ;(println (:source options) (:target options))
    ;(if (not= 0 (count arguments))
    ;  (= (= true (:target options)) (= true (:source options)))
    ;    (proces-json-from-filesystem (:source options) (:target options) )
    ;  )
    (if (= (count arguments) 2)
      (let [source-dir (first arguments)
            target-dir (second arguments)]
        (if (f/check-if-directory-exists source-dir)
          (if (f/check-and-make-directory target-dir)
            (p/proces-json-from-filesystem source-dir target-dir tagger-db)
            (exit 0 "Cannot make target dir"))
          (exit 0 "No source dir")))
      (exit 0 "Something went wrong"))
    ))


