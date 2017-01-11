(ns featured-sorter.runner
  (require [clojure.java.jdbc :as jdbc]
           [clojure.data.json :as json]
           [clojure.tools.cli :refer [parse-opts]]
           [clojure.string :as str]
           [clojure.java.io :as io]
           [cheshire.core :refer :all]
           [clj-time.jdbc])
  (:gen-class)
  (:import (com.sun.xml.internal.bind.v2.model.core ID)))

(def ^:dynamic *db-name* "default")
(def ^:dynamic *db-options* "DB_CLOSE_DELAY=-1")
(def ^:dynamic *db-user* "tagger")
(def ^:dynamic *db-password* *db-user*)

(def ^:dynamic *process-database*)
(def ^:dynamic *target-directory*)

(def ^:dynamic *new-file-feature-count* "1000")

(def tagger-db
  {:classname   "org.h2.Driver"
   :subprotocol "h2:mem"
   :subname     (str *db-name* ";" *db-options*)
   :user        *db-user*
   :password    *db-password*})

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn create-processing-table [tablename]
  (let [ddl (str "CREATE TABLE IF NOT EXISTS " tablename " (id VARCHAR, file VARCHAR);")]
    (jdbc/execute! *process-database* [ddl])))

(defn list-tables-db []
  (let [ddl (str "SHOW TABLES;")]
    (jdbc/query *process-database* [ddl])))

(defn check-if-directory-exists [directory]
  (.exists (io/file directory)))

(defn check-and-make-directory [directory]
  (if (check-if-directory-exists directory)
    true
    (.mkdir (java.io.File. directory))
    )
  )

(defn select-ids [features]
  (distinct (map #(get %1 :_id) features)))

(defn proces-json-file-to-db [file]
  (let [file-to-proces (json/read-str (slurp file) :key-fn keyword)
        tablename (clojure.string/upper-case (name (-> file-to-proces :features first :_collection)))
        ids-in-file (-> file-to-proces :features select-ids)]
    (create-processing-table tablename)
    (jdbc/insert-multi! *process-database* tablename (into [] (map #(hash-map :id %1, :file (str (.getParent file) "/" (.getName file))) ids-in-file)))))

(defn get-content-from-sourcefiles [files]
  (flatten (doall (for [file files]
                    (:features (json/read-str (slurp file) :key-fn keyword))))))

(defn filter-only-features-for-file [features ids]
  (filter #(let [feature %1]
             (if (.contains (into [] ids) (:_id feature))
               feature
               )) features))

(defn group-features-by-id [features ids]
  (for [id ids]
    (filter #(= id (:_id %1)) features)))

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

(defn associate-features [feature-group]
  (->> feature-group
       (sort-by :_valid_from)
       (mark-change)
       (mark-new)
       (mark-close)
       (remove-keys (into [] [:_valid_from :_valid_to]))))

(defn write-features-to-file [tablename filename features]
  (->> features
      (write-json-to-file tablename filename))
  )

(defn determine-content-newfile [tablename filename]
  (let [sql (str "SELECT ID, FILES FROM " tablename " WHERE NEWFILE = '" filename "';")
        select-features-for-newfile (jdbc/query *process-database* [sql])
        ids (map #(get %1 :id) select-features-for-newfile)
        distinct-source-files (-> (map #(clojure.string/split (:files %1) #",")
                                       (distinct select-features-for-newfile))
                                  flatten
                                  distinct)]
    (write-features-to-file (clojure.string/lower-case tablename)
                            (clojure.string/lower-case filename)
                            (flatten (doall (map associate-features (-> distinct-source-files
                                                                        get-content-from-sourcefiles
                                                                        (filter-only-features-for-file ids)
                                                                        (group-features-by-id ids))))))))

(defn determine-content-newfiles-for-feature [tablename]
  (let [sql (str "SELECT DISTINCT NEWFILE FROM " tablename ";")]
    (doall (map #(determine-content-newfile tablename (:newfile %1)) (jdbc/query *process-database* [sql])))))

(defn write-new-files [target-dir]
  (binding [*process-database* tagger-db
            *target-directory* target-dir]
    (doall (map #(determine-content-newfiles-for-feature (:table_name %1)) (list-tables-db)))))

(defn determine-new-filename-and-contentsource [tablename]
  (let [agg-tablename (str tablename"_AGG")
        create-new-table (str "CREATE TABLE " agg-tablename " (ID VARCHAR, FILES VARCHAR, NEWFILE VARCHAR);")
        agg-table (str "INSERT INTO " agg-tablename " (ID, FILES, NEWFILE) SELECT ID, GROUP_CONCAT(FILE SEPARATOR ',') FILES, null NEWFILE FROM " tablename " GROUP BY ID ORDER BY ID, FILES;")
        update-newfile-column (str "UPDATE " agg-tablename " SET NEWFILE = '" tablename "-' ||LPAD((((ROWNUM() -1) / " *new-file-feature-count* ") + 1), 6, 0 )||'.json';")
        drop-old-table (str "DROP TABLE " tablename ";")
        alter-tablename (str "ALTER TABLE " agg-tablename " RENAME TO " tablename ";")]
    (jdbc/execute! *process-database* [create-new-table])
    (jdbc/execute! *process-database* [agg-table])
    (jdbc/execute! *process-database* [update-newfile-column])
    (jdbc/execute! *process-database* [drop-old-table])
    (jdbc/execute! *process-database* [alter-tablename])))

(defn proces-json-files [files]
  (binding [*process-database* tagger-db]
    (doall (for [file files]
             (proces-json-file-to-db file)))
    (doall (map #(determine-new-filename-and-contentsource (:table_name %1)) (list-tables-db)))))

(defn proces-json-from-filesystem [source-dir target-dir]
  (let [files-for-directory (file-seq (io/file source-dir))
        files-to-proces (filter (fn [file-or-directory] (not (.isDirectory file-or-directory))) files-for-directory)]
    (proces-json-files files-to-proces)
    (write-new-files target-dir)))

(defn implementation-version []
  (if-let [version (System/getProperty "featured-sorter.version")]
    version
    (-> ^java.lang.Class (eval 'featured-action-tagger.runner) .getPackage .getImplementationVersion)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (str/join \newline errors)))

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
            (if (check-if-directory-exists source-dir)
              (if (check-and-make-directory target-dir)
                (proces-json-from-filesystem source-dir target-dir)
                (exit 0 "Cannot make target dir"))))
          (exit 0 "Something went wrong")))))
