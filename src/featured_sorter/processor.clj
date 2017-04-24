(ns featured-sorter.processor
  (:require [featured-sorter.postgres :as pg]
            [featured-sorter.reader :as r]
            [featured-sorter.sorter :as s]
            [featured-sorter.filter :as f]
            [featured-sorter.persistence :as p]
            [featured-sorter.writer :as w]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go go-loop chan]]))

(def ^:dynamic *process-database*)



(defn init-channels []
  ())

(defn file-splitter [in]
  (let [out (chan)]
    (go (while true (doseq [files (<! in)] (>! out files))))
    out))

(defn file-reader [in]
  (let [out (chan)]
    (go (while true (>! out (r/read-file (<! in)))))
    out))

(defn feature-filter [in]
  (let [out (chan)]
    (go (while true (>! out (f/filter-feature-ids (<! in)))))
    out))

(defn feature-writer [in]
  (let [out (chan)]
    (go (while true (>! out (p/write-to-db *process-database* (<! in)))))
    out))

(defn proces-logger [in]
  (go (while true (let [info (<! in)]
                    (log/info (str "finished file: " (:path (:filecontext info)) "\\" (:file (:filecontext info))))
                    (log/info (str "features-processed: " (:features-processed info)))))))

(def in-chan (chan))
(def file-reader-out (file-reader in-chan))
(def file-splitter-out (file-splitter file-reader-out))
(def feature-filter-out (feature-filter file-splitter-out))
(def feature-writer-out (feature-writer feature-filter-out))
(proces-logger feature-writer-out)

(defn proces-json-from-filesystem [source-dir target-dir tagger-db]
  (binding [*process-database* tagger-db]
    (log/info "start processing all files for filesystem")
    (let [files-for-directory (file-seq (io/file source-dir))
          files-to-proces (filter (fn [file-or-directory] (not (.isDirectory file-or-directory))) files-for-directory)]

      (doall (map #(>!! in-chan %1) files-to-proces))

      (Thread/sleep 5000)

      (s/sort-files *process-database* "tagger")

      (w/write-new-files *process-database* "tagger" target-dir)

      (log/info "done processing all files for filesystem")
      )
    )
  )

(defn proces-file-from-upload [dataset version request]
  (let [file (:tempfile (second (first (:params request))))]
    (>!! in-chan file)))