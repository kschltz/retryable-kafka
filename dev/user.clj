(ns user
 (:require [pgo.commons.kafka.retryable.consumer :as rcons]
           [jackdaw.client :as j.cli]
           [clojure.tools.logging :as log]
           [jackdaw.data :as jd]
           [clojure.datafy :refer [datafy]]
           [jackdaw.serdes :refer [string-serde]])
 (:import (no.finn.retriableconsumer ReliablePoolBuilder)
          (java.util.function Function Consumer)
          (java.util UUID)
          (org.apache.kafka.clients.consumer ConsumerRecord)
          (java.time LocalDateTime)))

(def bootstrap-servers
 "localhost:9092")
(def unified-topic
 {:topic-name         "dev.unified-transaction"
  :partition-count    1
  :replication-factor 1
  :topic-config       {}
  :key-serde          (string-serde)
  :value-serde        (string-serde)})

(def unified-factory
 (rcons/client-factory
  {:group-id    "retryable-test"
   :topic-props unified-topic
   :cons-props  {"group.id"           "retryable"
                 "bootstrap.servers"  bootstrap-servers
                 "enable.auto.commit" false}
   :prod-props  {"bootstrap.servers" bootstrap-servers}}))

(def a (atom #{}))
(add-watch a :prn #(prn %4))

(defn ^Function as-function [f]
 (reify Function
  (apply [_ arg] (f arg))))

(defn ^Consumer as-consumer
 [f]
 (reify Consumer
  (accept [_ t] (f t))))


(defn explode-when [pred]
 (as-function
  (fn [^ConsumerRecord r]
   (boolean
    (let [v (.value r)]
     (if (pred v)
      (do (prn "Must explode" (LocalDateTime/now))
          (throw (ex-info "boom" {:v v})))
      (swap! a conj v)))))))

(defn send! [m & {:keys [topic]}]
 (with-open [p (.producer unified-factory)]
  @(j.cli/produce! p
                   topic
                   (str (UUID/randomUUID))
                   (str m))))

(defn to-dlx []
 (as-consumer
  (fn [^ConsumerRecord r]
   (let [{:keys [timestamp
                 timestamp-type
                 value
                 key
                 headers]} (datafy r)]
    (with-open [p (.producer unified-factory)]
     @(j.cli/produce! p
                      {:topic-name "dlx.dump"}
                      nil
                      timestamp
                      key
                      value
                      headers))))))

(defn reliable-consumption [topics]
 (let [pool (-> (ReliablePoolBuilder. unified-factory)
                (.topics (mapv :topic-name topics))
                (.retryThrottleMillis 1000)
                (.retryPeriodMillis 5000)
                (.expiredHandler (to-dlx))
                (.processingFunction (explode-when #(= % "boom")))
                .build
                .monitor)]
  (.start pool)
  pool))


(comment
 "To test, spin up a kafka broker
  in localhost:9092 before loading up this namespace"


 (def pool (reliable-consumption [unified-topic]))
 (.start pool)
 (.close pool)

 (send! "My msg1" :topic unified-topic)                                          ;; will end up in atom a
 (deref a)
 (send! "boom" :topic unified-topic)                                             ;;Will end up in retry
 )