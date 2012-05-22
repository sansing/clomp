(ns clomp.core
  (:refer-clojure :exclude [send])
  (:use [clojure.string :only [join split]])
  (:require [clojure.java.io :as io])
  (:import (java.net Socket)))

(def ^:dynamic *session-id* nil)
(def ^:dynamic *connection* nil)

(deftype StompSocket [socket reader writer])

(defn clomp [uri port]
  (let [sock (java.net.Socket. uri port)]
    (StompSocket. sock (io/reader sock) (io/writer sock))))

(defn- send-frame [out command headers & [body]]
  (binding [*out* (.writer out)]
    (println command)
    (println (join "\n" (for [[k v] headers] (str (name k) ":" v))))
    (if body (println (str "content-length:" (count body))))
    (println)
    (if body (print body))
    (print "\0")
    (flush)))

(defn- read-headers []
  (loop [headers {}]
    (let [[key val] (split (read-line) #":" 2)]
      (if (and key val)
        (recur (assoc headers (keyword key) val))
        headers))))

(defn- read-body [in length]
  (if length
    (let [length (Integer/parseInt length)
          buffer (char-array length)]
      (loop [offset 0]
        (if (< offset length)
          (recur (+ (.read in buffer offset (- length offset))
                    offset ))))
      (.read in) ;; consume \0
      (String. buffer))
    (loop [string ""]
      (let [c (.read in)]
        (if (= 0 c)
          string
          (recur (str string (char c))))))))

(defn- read-command []
  (let [command (read-line)]
    (if (empty? command)
      (read-command)
      (keyword command))))

(defrecord Frame [type headers body])

(defn receive [in & [type]]
  (binding [*in* (.reader in)]
    (let [type    (read-command)
          headers (read-headers)
          body    (read-body *in* (:content-length headers))
          frame   (Frame. type headers body)]
      (if (and type (not= type (:type frame)))
        (throw (Exception. (str (format  "expected %s frame, got %s\n" type (:type frame))
                                (get-in frame [:headers :message]) "\n" (:body frame))))
        frame))))

(defn connect     [s headers]      (send-frame s "CONNECT"     headers) (receive s :CONNECTED))
(defn send        [s headers body] (send-frame s "SEND"        headers body))
(defn subscribe   [s headers]      (send-frame s "SUBSCRIBE"   headers))
(defn unsubscribe [s headers]      (send-frame s "UNSUBSCRIBE" headers))
(defn begin       [s headers]      (send-frame s "BEGIN"       headers))
(defn commit      [s headers]      (send-frame s "COMMIT"      headers))
(defn abort       [s headers]      (send-frame s "ABORT"       headers))
(defn ack         [s headers]      (send-frame s "ACK"         headers))
(defn disconnect  [s]              (send-frame s "DISCONNECT"  {}))

(defmacro with-connection [s headers & forms]
  `(let [frame# (connect ~s ~headers)]
     (binding [*connection* ~s
               *session-id* (get-in frame# [:headers :session])]
       (try ~@forms
            (finally (disconnect ~s))))))
