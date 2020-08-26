(ns pgo.commons.kafka.retryable.consumer
 (:require [jackdaw.client :as jd.cli])
 (:import (no.finn.retriableconsumer KafkaClientFactory)))


(defn client-factory
 [{:keys [group-id
          topic-props
          cons-props
          prod-props]}]
 (proxy [KafkaClientFactory] []
  (groupId [] group-id)
  (consumer []
   (jd.cli/consumer cons-props topic-props))
  (producer []
   (jd.cli/producer prod-props topic-props))))