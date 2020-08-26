(ns user
 (:require [pgo.commons.kafka.retryable.consumer :as rcons]
           [jackdaw.client :as j.cli]
           [clojure.tools.logging :as log]
           [jackdaw.data :as jd]
           [jackdaw.serdes :refer [string-serde]])
 (:import (no.finn.retriableconsumer ReliablePoolBuilder)
          (java.util.function Function)
          (java.util UUID)
          (org.apache.kafka.clients.consumer ConsumerRecord)))

(def bootstrap-servers
 "localhost:9092")
(def topic {:topic-name         "dev.unified-transaction"
            :partition-count    1
            :replication-factor 1
            :topic-config       {}
            :key-serde          (string-serde)
            :value-serde        (string-serde)})



(def unified-factory
 (rcons/client-factory
  {:group-id    "retryable-test"
   :topic-props topic
   :cons-props  {"group.id"           "retryable"
                 "bootstrap.servers"  bootstrap-servers
                 "enable.auto.commit" false}
   :prod-props  {"bootstrap.servers" bootstrap-servers}}))

(def a (atom #{}))

(defn ^Function as-function [f]
 (reify Function
  (apply [_ arg] (f arg))))


(defn explode-when [pred]
 (as-function
  (fn [^ConsumerRecord r]
   (boolean
    (let [v (.value r)]
     (if (pred v)
      (do (prn "Must explode")
          (throw (ex-info "boom" {:v v})))
      (swap! a conj v)))))))

(defn send! [m]
 (with-open [p (.producer unified-factory)]
  @(j.cli/produce! p
                   topic
                   (str (UUID/randomUUID))
                   (str m))))

(defn reliable-consumption [topics]
 (let [pool (-> (ReliablePoolBuilder. unified-factory)
                (.topics (mapv :topic-name topics))
                (.retryThrottleMillis 100)
                (.retryPeriodMillis 500)
                (.processingFunction (explode-when #(= % "boom")))
                .build
                .monitor)]
  (.start pool)
  pool))

(def consumers-poll
 (reliable-consumption [topic]))