(ns lambdajam.challenge-3-1
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :upper-case]
   [:upper-case :interpose-pipe]
   [:interpose-pipe :interpose-space]
   [:interpose-space :write-segments]])

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

      {:onyx/name :upper-case
       :onyx/fn :lambdajam.challenge-3-1/upper-case
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Converts :name to all upper-case letters."}

      {:onyx/name :interpose-pipe
       :onyx/fn :lambdajam.challenge-3-1/interpose-pipe
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Interposes the pipe character (|) between all chars in :name"}

      {:onyx/name :interpose-space
       :onyx/fn :lambdajam.challenge-3-1/interpose-space
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Interposes a single space character between all chars in :name"}

      {:onyx/name :write-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

;; <<< BEGIN FILL ME IN >>>
(defn upper-case [{:keys [name]}]
  {:name (clojure.string/upper-case name)})

(defn interpose-pipe [{:keys [name]}]
  {:name (->> name
           (interpose '|)
           (apply str))})

(defn interpose-space [{:keys [name]}]
  {:name (->> name
           (interpose \space)
           (apply str))})
;; <<< END FILL ME IN >>>

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
    :lifecycle/calls :lambdajam.challenge-3-1/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-3-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])
