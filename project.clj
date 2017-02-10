(def version (slurp "VERSION"))
(def artifact-name (str "featured-sorter-" version))
(def uberjar-name (str artifact-name "-standalone.jar"))
(def webjar-name (str artifact-name "-web.jar"))
(def uberwar-name (str artifact-name ".war"))
(def git-ref (clojure.string/replace (:out (clojure.java.shell/sh "git" "rev-parse" "HEAD"))#"\n" "" ))

(defproject featured-sorter version
  :min-lein-version "2.5.4"
  :description "FIXME: write description"
  :url "http://github.com/PDOK/featured-sorter"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.0-alpha1"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/tools.cli "0.3.3"]
                 [org.clojure/core.async "0.2.395"]
                 [cheshire "5.6.3"]
                 [com.h2database/h2 "1.4.193"]
                 [org.postgresql/postgresql "9.4.1209.jre7"]
                 [org.clojure/tools.logging "0.3.1"]
                 [log4j/log4j "1.2.17"]
                 [nl.pdok/pdok-util "1.0-SNAPSHOT"]
                 [clj-time "0.12.2"]]
  :main ^:skip-aot featured-sorter.runner
  :jvm-opts ["-Xmx2g"]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
