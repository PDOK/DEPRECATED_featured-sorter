(ns featured-sorter.api
  (:require
    [featured-sorter.datastore :as d]
    [featured-sorter.processor :as p]
    [clojure.data.json :as json]
    [compojure.core :refer :all]
    [compojure.route :as route]
    [ring.util.json-response :as r]
    [ring.middleware.multipart-params :as mp]
    [ring.middleware.defaults :refer :all]
    [ring.middleware.json :refer :all]
    [clojure.core.async :as a
     :refer [>! <! >!! <!! go go-loop chan]]))

(defn wrap-exception-handling
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch Exception e
        {:status 400 :body (.getMessage e)}))))

(def in-chan (chan))

(defn init! []
  (d/init-datastore)
  (p/init-channels)
  )

(defn api-routes []
  (defroutes api-routes
             (context "/sorter" []
                      (GET "/info" [] (r/json-response {:version 1}))
                      (POST "/datasets" [] (partial d/create-store))
                      (GET "/datasets" []  (d/list-stores))
                      (GET "/dataset/:dataset" [dataset] (d/list-versions dataset))
                      (GET "/dataset/:dataset/:version" [dataset version] (d/list-featuretypes dataset version))
                      (DELETE "/dataset/:dataset/:version" [dataset version] (partial d/delete-store dataset version))

                      (mp/wrap-multipart-params
                        (POST "/dataset/:dataset/:version" [dataset version] (partial p/proces-file-from-upload dataset version)))
                      )
             (route/not-found "NOT FOUND")))

(def api
  (-> (api-routes)
      (wrap-json-body {:keywords? true :bigdecimals? true})
      (wrap-json-response)
      (wrap-defaults api-defaults)
      (wrap-exception-handling)
      (routes)
      ))