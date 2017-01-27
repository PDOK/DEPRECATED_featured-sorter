(ns featured-sorter.write-files
  (:require [featured-sorter.postgres :as pg]
            [clojure.tools.logging :as log]
            [clojure.java.jdbc :as jdbc]
            [clojure.java.io :as io]
            [clojure.data.json :as json]
            [nl.pdok.util.ziptools :as z])
  (:import (java.util.zip ZipFile)))

(def ^:dynamic *process-database*)
(def ^:dynamic *db-schemaname*)

(def ^:dynamic *target-directory*)

(defn check-if-directory-exists [directory]
  (.exists (io/file directory)))

(defn check-and-make-directory [directory]
  (if (check-if-directory-exists directory)
    true
    (.mkdir (java.io.File. directory))))

(defn write-json-to-file [featuretype filename features]
  (check-and-make-directory (str *target-directory* "/" featuretype ))
  (with-open [writer (io/writer (str *target-directory* "/" featuretype "/" filename) :append true)]
    (.write writer (str "{\"dataset\":\"" featuretype "\",\n\"features\":["))
    (let [first? (ref true)]
      (doseq [f features]
        (if-not @first?
          (.write writer ",\n")
          (dosync (ref-set first? false)))
        (.write writer (cheshire.core/generate-string f)))
      (.write writer "]}"))))

(defn write-features-to-file [tablename filename features]
  (->> features
       (write-json-to-file tablename filename)))

(defn mark-change [features]
  (reduce-kv (fn [result key feature]
               (let [previous (last result)]
                 (conj result (assoc feature :_validity (:_valid_from feature)
                                             :_current_validity (:_valid_from previous)
                                             :_action "change"))))
             []
             (into [] features)))

(defn mark-new [features]
  (concat (conj () (-> features
                       first
                       (assoc :_action "new"
                              :_validity (:_valid_from (-> features first)))))
          (-> features rest)))

(defn mark-close [features]
  (let [last-feature (last features)]
    (if (not (some nil? (vals (select-keys last-feature [:_valid_from :_valid_to]))))
      (if (not= "new" (:_action last-feature))
        (concat features
                (conj () (-> features
                             last
                             (assoc :_action "close"
                                    :_validity (:_valid_to last-feature)
                                    :_current_validity (:_valid_from last-feature)))))
        features)
      features)))

(defn remove-keys [keys features]
  (map #(apply dissoc %1 keys) features))

(defn associate-features [feature-group]
  (->> feature-group
       (sort-by :_valid_from)
       (mark-change)
       (mark-new)
       (mark-close)
       (remove-keys (into [] [:_valid_from :_valid_to]))))

(defmulti get-content-from-source :compressed)

(defmethod get-content-from-source true [filecontext]
  (let [file (java.io.File. (str (:path filecontext)))
        zip (ZipFile. file)
        entry (z/get-entry zip (first (filter #(= (:file filecontext) (.getName %1)) (z/list-entries zip))))]
    (:features (json/read-str (slurp entry) :key-fn keyword))))

(defmethod get-content-from-source false [filecontext]
  (:features (json/read-str (slurp (str (:path filecontext) "\\" (:file filecontext))) :key-fn keyword)))

(defn get-content-from-sourcefiles [files]
  (flatten (doall (for [file files]
                    (get-content-from-source (clojure.walk/keywordize-keys file))))))

(defn filter-only-features-for-file [features ids]
  (filter #(let [feature %1]
             (if (.contains (into [] ids) (:_id feature))
               feature
               )) features))

(defn group-features-by-id [features ids]
  (for [id ids]
    (filter #(= id (:_id %1)) features)))

(defn determine-content-newfile [tablename filename]
  (log/info (str "create file: " filename))
  (let [sql (str "SELECT id, filecontext FROM " tablename " WHERE newfile = '" filename "';")
        select-features-for-newfile (jdbc/query *process-database* [sql])
        ids (map #(get %1 :id) select-features-for-newfile)
        distinct-source-files (flatten (-> (map #(:filecontext %1)
                                                (distinct select-features-for-newfile))
                                           flatten
                                           distinct))]
    (write-features-to-file (clojure.string/lower-case tablename)
                            (clojure.string/lower-case filename)
                            (flatten (doall (map associate-features (-> distinct-source-files
                                                                        get-content-from-sourcefiles
                                                                        (filter-only-features-for-file ids)
                                                                        (group-features-by-id ids)
                                                                        )))))))

(defn determine-content-newfiles-for-feature [tablename]
  (log/info (str "proces feature to files: " tablename))
  (let [sql (str "SELECT DISTINCT newfile FROM " tablename " ORDER BY newfile ASC;")]
    (doall (map #(determine-content-newfile tablename (:newfile %1)) (jdbc/query *process-database* [sql])))))

(defn write-new-files [db schema target-dir]
  (binding [*process-database* db
            *target-directory* target-dir
            *db-schemaname* schema]
    (doall (map #(determine-content-newfiles-for-feature (:table_name %1)) (pg/list-tables-db *process-database* *db-schemaname*)))))