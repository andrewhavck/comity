(ns comity.network
  (:require [clojure.core.async :refer [>! <! chan go close! sliding-buffer]])
  (:import (java.net DatagramPacket InetAddress MulticastSocket)))

(def port 10001)
(def group (InetAddress/getByName "224.1.0.0"))
(def sock-buff-size 1000)
(def chan-buff-size 25)

(defprotocol Socket
  (send! [this msg])
  (receive! [this])
  (close [this]))

(defrecord Multicast [socket]
  Socket
  (send! [_ msg] (.send socket (DatagramPacket. (.getBytes msg) (.length msg) group port)))
  (receive! [_]
    (let [recv (DatagramPacket. (byte-array sock-buff-size) sock-buff-size)]
      (.receive socket recv)
      (String. (.getData recv))))
  (close [_] (.close socket)))

(defprotocol Client
  (out [this msg])
  (in [this])
  (end [this]))

(defrecord AsyncClient [in out crtl]
  Client
  (out [_ msg] (go (>! out msg)))
  (in [_] (go (<! in)))
  (end [_] (go (>! crtl :end))))

(defn multicast-socket []
  (let [socket (MulticastSocket. port)]
    (.joinGroup socket group)
    (->Multicast socket)))

(defn start []
  (let [[in out crtl] (repeatedly 3 #(chan (sliding-buffer chan-buff-size)))
        client (->AsyncClient in out crtl)
        socket (multicast-socket)]
        (go (while true (send! socket (<! out))))
        (go 
         (while true 
           (if (= :end (<! crtl))
             (do
               (doseq [c [in out crtl]] (close! c))
               (close socket)))))
        client))

(defn broadcast! [msg] (send! (multicast-socket) msg))
(defn read! [] (receive! (multicast-socket)))

