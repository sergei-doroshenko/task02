(ns task02.network
  (:use [task02 helpers query])
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net Socket ServerSocket InetAddress InetSocketAddress SocketTimeoutException]
           (java.io PrintWriter BufferedReader InputStreamReader)))

;; Объявить переменную для синхронизации между потоками. Воспользуйтесь promise
(def ^:private should-be-finished (promise))

;; Hint: *in*, *out*, io/writer, io/reader, socket.getOutputStream(), socket.getInputStream(), socket.close(), binding
;;       deliver, prn
(defn handle-request [^Socket sock]
  ;; переопределить *in* & *out* чтобы они указывали на входной и выходной потоки сокета
  (binding [*in* (BufferedReader. (InputStreamReader. (.getInputStream sock)))
            *out* (PrintWriter. (.getOutputStream sock) true)]
    (try
      (let [s (read-line)] ;; считать данные из переопределенного *in*
        (if (= (str/lower-case s) "quit")
          ;;; 1) сообщить основному потоку что мы завершаем выполнение.
          ;;; для этого необходимо установить переменную should-be-finished в true
          ;;;
          (deliver should-be-finished true)
          ;;; 2) выполнить запрос при помощи perform-query и записать
          ;;; результат в переопределенный *out*
          (println (perform-query s))
          ))
      (catch Throwable ex
        (println "Exception: " ex))
      (finally
        (.close sock))))) ;;; закрыть сокет


;; Hint: future, deliver
(defn- run-loop [server-sock]
  (try
    (let [^Socket sock (.accept server-sock)]
      ;; выполнить функцию handle-request в отдельном потоке
      (future (handle-request sock))
      )
    (catch SocketTimeoutException ex)
    (catch Throwable ex
      (println "Got exception" ex)
      ;; сообщить основному потоку что мы завершаем выполнение
      ;; для этого необходимо установить переменную should-be-finished в true
      (deliver should-be-finished true)
      )))

(defn run [port]
  (let [sock-addr (InetSocketAddress. port)
        server-socket (doto (ServerSocket.)
                        (.setReuseAddress true)
                        (.setSoTimeout 3000)
                        (.bind sock-addr))]
    (loop [_ (run-loop server-socket)]
      (when-not (realized? should-be-finished) ;; следующий запрос если работа не завершается...
        (recur (run-loop server-socket))))
    (.close server-socket)))

(defn rloop [server-sock]
  (let [^Socket sock (.accept server-sock)
        out (PrintWriter. (.getOutputStream sock) true)
        in (BufferedReader. (InputStreamReader. (.getInputStream sock)))]
    (loop [line (.readLine in)]
      (if-not (or (nil? line) (= line "Bye."))
        (do (println "Server:" line)
            (.println out)
            (recur (.readLine in)))
        (println "Exit.")))))

(defn run2 [port]
  (let [sock-addr (InetSocketAddress. port)
        server-socket (doto (ServerSocket.)
                        (.setReuseAddress true)
                        ;(.setSoTimeout 3000)
                        (.bind sock-addr))]
    (rloop server-socket)
    (.close server-socket)))