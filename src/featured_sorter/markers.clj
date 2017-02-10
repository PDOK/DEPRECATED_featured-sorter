(ns featured-sorter.markers)

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