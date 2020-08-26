(ns pgo.commons.kafka.retryable.consumer
 (:require [jackdaw.client :as jd.cli])
 (:import (no.finn.retriableconsumer KafkaClientFactory)))


(defn client-factory
 [{:keys [cons-props
          prod-props]}]
 (proxy [KafkaClientFactory] []
  (consumer []
   (jd.cli/consumer cons-props))
  (producer []
   (jd.cli/producer prod-props))))