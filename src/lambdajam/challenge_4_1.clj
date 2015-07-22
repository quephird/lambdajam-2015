(ns lambdajam.challenge-4-1
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times-three]
   [:times-three :write-segments]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/ident :core.async/read-from-chan
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :times-three
       :onyx/fn :lambdajam.challenge-4-1/times-three
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by 3"}

      {:onyx/name :write-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn times-three [segment]
  (update-in segment [:n] (partial * 3)))

;;; Lifecycles ;;;

(def logger (agent nil))

;; <<< BEGIN FILL ME IN >>>
(defn log-after-batch [event lifecycle]
  (send logger (fn [_]
                 (let [segments (:onyx.core/batch event)]
                   (doseq [segment segments]
                     (println (:message segment))))))
  {})

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})


(def logger-lifecycle
  {:lifecycle/after-batch log-after-batch})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :times-three
   :lifecycle/calls :lambdajam.challenge-4-1/logger-lifecycle
   :onyx/doc "Logs messages about the batch"}

  {:lifecycle/task :read-segments
   :lifecycle/calls :lambdajam.challenge-4-1/reader-lifecycle
   :core.async/id (java.util.UUID/randomUUID)
   :onyx/doc "Injects the core.async reader channel"}

  {:lifecycle/task :read-segments
   :lifecycle/calls :onyx.plugin.core-async/reader-calls
   :onyx/doc "core.async plugin base lifecycle"}

  {:lifecycle/task :write-segments
   :lifecycle/calls :lambdajam.challenge-4-1/writer-lifecycle
   :core.async/id (java.util.UUID/randomUUID)
   :onyx/doc "Injects the core.async writer channel"}

  {:lifecycle/task :write-segments
   :lifecycle/calls :onyx.plugin.core-async/writer-calls
   :onyx/doc "core.async plugin base lifecycle"}
   ])

;; <<< END FILL ME IN >>>
