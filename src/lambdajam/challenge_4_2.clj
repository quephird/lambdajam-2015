(ns lambdajam.challenge-4-2
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :write-segments]])

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

      {:onyx/name :identity
       :onyx/fn :lambdajam.challenge-4-2/my-identity-fn
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Returns the segment"}

      {:onyx/name :write-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn my-identity-fn [segment]
  segment)

(def max-val (atom nil))

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn reset-atom [event lifecycle]
  (reset! max-val nil)
  {})

(defn find-max [{segments :onyx.core/batch} lifecycle]
  (let [new-max-val (if (not (zero? (count segments)))
                      (->> segments
                        (map #(get-in % [:message :n]))
                        (apply max))
                      nil)]
    (if (and (or (nil? @max-val) (> new-max-val @max-val))
             (not (nil? new-max-val)))
      (reset! max-val new-max-val))
    {}))

(defn print-atom [event lifecycle]
  (println "Maximum value was:" (str @max-val))
  {})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

;;; Lifecycles ;;;

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def identity-lifecycle
  {:lifecycle/before-task-start reset-atom
   :lifecycle/after-batch       find-max
   :lifecycle/after-task-stop   print-atom})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def logger (agent nil))

;; <<< BEGIN FILL ME IN >>>

(defn build-lifecycles []
  [
  {:lifecycle/task :identity
   :lifecycle/calls :lambdajam.challenge-4-2/identity-lifecycle
   :onyx/doc "Determines the max value of all segments processed"}

  {:lifecycle/task :read-segments
   :lifecycle/calls :lambdajam.challenge-4-2/reader-lifecycle
   :core.async/id (java.util.UUID/randomUUID)
   :onyx/doc "Injects the core.async reader channel"}

  {:lifecycle/task :read-segments
   :lifecycle/calls :onyx.plugin.core-async/reader-calls
   :onyx/doc "core.async plugin base lifecycle"}

  {:lifecycle/task :write-segments
   :lifecycle/calls :lambdajam.challenge-4-2/writer-lifecycle
   :core.async/id (java.util.UUID/randomUUID)
   :onyx/doc "Injects the core.async writer channel"}

  {:lifecycle/task :write-segments
   :lifecycle/calls :onyx.plugin.core-async/writer-calls
   :onyx/doc "core.async plugin base lifecycle"}
   ])


;; <<< END FILL ME IN >>>
