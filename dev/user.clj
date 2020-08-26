(ns user
 (:require [pgo.commons.kafka.retryable.consumer :as rcons]))


(rcons/client-factory {:cons-props {}})
