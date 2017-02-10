(ns featured-sorter.reader
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log]
            [featured-sorter.postgres :as pg]
            [nl.pdok.util.ziptools :as z]
            [clojure.core.async :as a
             :refer [>! <! >!! <!! go chan buffer close! thread
                     alts! alts!! timeout]])
  (:import (java.util.zip ZipFile ZipInputStream)
           (java.io FileInputStream)))

(defn- proces-file
  ([file]
   (proces-file file (.getParent file) (.getName file) (z/is-zip file)))
  ([file fileparent filename compressed]
   (log/debug (str "processing: " fileparent "\\"  filename))
   (let [file-to-proces (json/read-str (slurp file) :key-fn keyword)
         collection (str (clojure.string/lower-case (name (-> file-to-proces :features first :_collection))))]
     {:_collection collection :filecontext {:path (str fileparent) :file (str filename) :compressed compressed} :features (-> file-to-proces :features)})))

(defn- proces-zipfile [file]
  (with-open [zip (ZipFile. file)]
    (let [entries (z/list-entries zip)]
      (doall (for [entry entries]
               (proces-file (z/get-entry zip entry) (str (.getParent file) "\\" (.getName file)) (.getName entry) true))))))

(defn read-file [file]
  (log/debug "processing: " (.getName file))
  (let [zipped? (z/is-zip file)]
    (if (= true zipped?)
      (proces-zipfile file)
      (seq [(proces-file file)]))))
