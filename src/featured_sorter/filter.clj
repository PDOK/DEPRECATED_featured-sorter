(ns featured-sorter.filter
  (:require [clojure.tools.logging :as log]))

(defn select-ids [features]
  (distinct (map #(get %1 :_id) features)))

(defn filter-feature-ids [file-to-proces]
  (log/debug (str "filtering file: " (:path (:filecontext file-to-proces)) "\\" (:file (:filecontext file-to-proces))))
  (let [ids-in-file (-> file-to-proces :features select-ids)]
    (assoc (dissoc file-to-proces :features) :ids ids-in-file)))