(ns task02.query
  (:use [task02 helpers db]
        [clojure.core.match :only (match)]))

(defn prepare-query [query]
  (vec (.split query " ")))

(defn check-match [query, res]
  (match query
         ["select" tbl & _]
         (check-match (vec (drop 2 query)) (conj res (str tbl)))
         ["where" a b c & _]
         (check-match (vec (drop 4 query)) (conj res :where a b c)) ;(make-where-function a b c)
         ["order" _ col & _]
         (check-match (vec (drop 3 query)) (conj res :order-by (keyword col)))
         ["limit" n & _]
         (check-match (vec (drop 2 query)) (conj res :limit (parse-int n)))
         ["join" tbl _ lcol _ rcol & _]
         (check-match (vec (drop 2 query)) (conj res :joins [(keyword lcol) tbl (keyword rcol)]))
         :else (apply list res)))
(println
  (check-match
    (prepare-query "select student where id = 10 order by id limit 2 join subject on id = sid")
    []))
;; where id = 10
(defn make-where-function [key oper val]
  (fn [data] (oper (key data) val)))

(println (apply (make-where-function :age > 20) [{:age 30}])); true
(println (apply (make-where-function :name = "Ivan") [{:name "Ivan"}])) ; true
;; Функция выполняющая парсинг запроса переданного пользователем
;;
;; Синтаксис запроса:
;; SELECT table_name [WHERE column comp-op value] [ORDER BY column] [LIMIT N] [JOIN other_table ON left_column = right_column]
;;
;; - Имена колонок указываются в запросе как обычные имена, а не в виде keywords. В
;;   результате необходимо преобразовать в keywords
;; - Поддерживаемые операторы WHERE: =, !=, <, >, <=, >=
;; - Имя таблицы в JOIN указывается в виде строки - оно будет передано функции get-table для получения объекта
;; - Значение value может быть либо числом, либо строкой в одинарных кавычках ('test').
;;   Необходимо вернуть данные соответствующего типа, удалив одинарные кавычки для строки.
;;
;; - Ключевые слова --> case-insensitive
;;
;; Функция должна вернуть последовательность со следующей структурой:
;;  - имя таблицы в виде строки
;;  - остальные параметры которые будут переданы select
;;
;; Если запрос нельзя распарсить, то вернуть nil

;; Примеры вызова:
;; > (parse-select "select student")
;; ("student")
;; > (parse-select "select student where id = 10")
;; ("student" :where #<function>)
;; > (parse-select "select student where id = 10 limit 2")
;; ("student" :where #<function> :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2")
;; ("student" :where #<function> :order-by :id :limit 2)
;; > (parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; ("student" :where #<function> :order-by :id :limit 2 :joins [[:id "subject" :sid]])
;; > (parse-select "werfwefw")
;; nil
(defn parse-select [^String sel-string]
  (let [result (check-match (prepare-query sel-string) [])]
    (if-not (empty? result) result nil)))

(parse-select "select student")
(parse-select "select student where id = 10 order by id limit 2 join subject on id = sid")
;; Выполняет запрос переданный в строке.  Бросает исключение если не удалось распарсить запрос

;; Примеры вызова:
;; > (perform-query "select student")
;; ({:id 1, :year 1998, :surname "Ivanov"} {:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "select student order by year")
;; ({:id 3, :year 1996, :surname "Sidorov"} {:id 2, :year 1997, :surname "Petrov"} {:id 1, :year 1998, :surname "Ivanov"})
;; > (perform-query "select student where id > 1")
;; ({:id 2, :year 1997, :surname "Petrov"} {:id 3, :year 1996, :surname "Sidorov"})
;; > (perform-query "not valid")
;; exception...
(defn perform-query [^String sel-string]
  (if-let [query (parse-select sel-string)]
    (apply select (get-table (first query)) (rest query))
    (throw (IllegalArgumentException. (str "Can't parse query: " sel-string)))))
