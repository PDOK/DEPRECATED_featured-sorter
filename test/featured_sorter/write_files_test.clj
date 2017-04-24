(ns featured-sorter.write-files-test
  (:require [clojure.test :refer :all]
            [featured-sorter.writer :refer :all]
            [clojure.data.json :as json]
            [featured-sorter.markers :as m]))

(deftest strip-keys-from-features
  (testing "strip keys from a coll of maps"
    (let [keys (into [] [:_valid_from :_valid_to])
          org-f (seq [{:_id 1 :_valid_from "20111111" :_valid_to "20121212"}
                      {:_id 1 :a_value "value" :_valid_from "20101010" :_valid_to "20111111"}])
          new-f (seq [{:_id 1}
                      {:_id 1 :a_value "value"}])]
      (is (= new-f (remove-keys keys org-f))))))

(deftest mark-feature-as-new
  (testing "update a feature as new")
  (let [org-f (seq [{:_id 1 :_valid_from "2011"}])
        new-f (seq [{:_id 1 :_valid_from "2011" :_action "new" :_validity "2011"}])]
    (is (= new-f (m/mark-new org-f)))))

(deftest mark-features-as-change
  (testing "update features as change")
  (let [org-f (seq [{:_id 1 :_valid_from "2011"}
                    {:_id 1 :_valid_from "2012"}])
        new-f (seq [{:_id 1 :_valid_from "2011" :_validity "2011" :_current_validity nil :_action "change"}
                    {:_id 1 :_valid_from "2012" :_validity "2012" :_current_validity "2011" :_action "change"}])]
    (is (= new-f (m/mark-change org-f)))))

(deftest mark-features-as-new-or-change
  (testing "update features as new and change")
  (let [org-f (seq [{:_id 1 :_valid_from "2011"}
                    {:_id 1 :_valid_from "2012"}])
        new-f (seq [{:_id 1 :_valid_from "2011" :_validity "2011" :_current_validity nil :_action "new"}
                    {:_id 1 :_valid_from "2012" :_validity "2012" :_current_validity "2011" :_action "change"}])]
    (is (= new-f (m/mark-new (m/mark-change org-f))))))

(deftest mark-feature-as-close
  (testing "update a feature as a close")
  (let [org-f (seq [{:_id 1 :_valid_from "2011" :_valid_to "2012" :_action "change" :_validity "2011"}])
        new-f (seq [{:_id 1 :_valid_from "2011" :_valid_to "2012" :_action "change" :_validity "2011"}
                    {:_id 1 :_valid_from "2011" :_valid_to "2012" :_action "close" :_validity "2012" :_current_validity "2011"}])]
    (is (= new-f (m/mark-close org-f)))))

(deftest group-by-ids
  (testing "remove features from coll that don't match with the given ids and group the rest by id")
  (let [org-f (seq [{:_id 1} {:_id 2} {:_id 3} {:_id 4} {:_id 2} {:_id 3}])
        new-f (seq [(seq [{:_id 2} {:_id 2}])
                    (seq [{:_id 3} {:_id 3}])
                    (seq [{:_id 4}])])]
    (is (= new-f (group-features-by-id org-f [2 3 4])))))

(def proces-feature-group-file-org "dev-resources/runner-test/proces-feature-group-org.json")
(def proces-feature-group-file-result "dev-resources/runner-test/proces-feature-group-result.json")

(deftest proces-feature-group
  (testing "processing a group of features")
  (let [org-f (associate-features (into () (json/read-str (slurp proces-feature-group-file-org) :key-fn keyword)))
        result-f (sort-by :_validity (into () (json/read-str (slurp proces-feature-group-file-result) :key-fn keyword)))]
    (is (= result-f org-f))))