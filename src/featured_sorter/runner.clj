(ns featured-sorter.runner
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [nl.pdok.util.ziptools :as z]
            [featured-sorter.postgres :as pg]
            [featured-sorter.read-files :as pf]
            [featured-sorter.write-files :as wf])
  (:gen-class))

(def ^:dynamic *db-user* "tagger")
(def ^:dynamic *db-password* *db-user*)
(def ^:dynamic *db-schemaname* *db-user*)

(def ^:dynamic *process-database*)

(def tagger-db {:subprotocol "postgresql"
                   :subname "//localhost:5432/tagger"
                   :user *db-user*
                   :password *db-password*
                   :transaction? true})

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn remove-database []
  (pg/drop-schema *process-database* *db-schemaname*))

(defn proces-json-from-filesystem [source-dir target-dir]
  (log/info "start processing all files for filesystem")
  (binding [*process-database* tagger-db]
    (let [files-for-directory (file-seq (io/file source-dir))
          files-to-proces (filter (fn [file-or-directory] (not (.isDirectory file-or-directory))) files-for-directory)]
      (pf/proces-files files-to-proces *process-database* *db-schemaname*)
      (wf/write-new-files *process-database* *db-schemaname* target-dir)
      ;(remove-database)
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
            (if (wf/check-if-directory-exists source-dir)
              (if (wf/check-and-make-directory target-dir)
                (proces-json-from-filesystem source-dir target-dir)
                (exit 0 "Cannot make target dir"))
              (exit 0 "No source dir")))
          (exit 0 "Something went wrong")))))


