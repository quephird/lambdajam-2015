(ns lambdajam.challenge-2-1
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times-three]
   [:times-three :plus-fifty]
   [:plus-fifty :write-segments]])

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

      ;; <<< BEGIN FILL ME IN >>>
      {:onyx/name :times-three
       :onyx/fn :lambdajam.challenge-2-1/times-three
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by 3"}

       {:onyx/name :plus-fifty
        :onyx/fn :lambdajam.challenge-2-1/plus-fifty
        :onyx/type :function
        :onyx/batch-size batch-size
        :onyx/batch-timeout batch-timeout
        :onyx/doc "Adds :n in the segment to 50"}
      ;; <<< END FILL ME IN >>>

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

(defn plus-fifty [segment]
  (update-in segment [:n] (partial + 50)))

;;; Lifecycles ;;;

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :lambdajam.challenge-2-1/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-2-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])
