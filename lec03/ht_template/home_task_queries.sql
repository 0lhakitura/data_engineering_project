/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/
SELECT category_id,COUNT(film_id) AS count
FROM pagila.public.film_category
GROUP BY category_id
ORDER BY count DESC;


/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
SELECT first_name, last_name, SUM(rental_rate) as rental
FROM film_actor fa
JOIN actor a ON fa.actor_id=a.actor_id
JOIN film f on fa.film_id = f.film_id
GROUP BY first_name, last_name
ORDER BY rental DESC
LIMIT 10;



/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/

SELECT c.name, MAX(rental_rate) as rental
FROM category c
JOIN film_category a ON c.category_id = a.category_id
JOIN film f on f.film_id = a.film_id
GROUP BY c.name
ORDER BY rental DESC
LIMIT 1;



/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
SELECT title
FROM film f
LEFT JOIN inventory i ON f.film_id=i.film_id
WHERE i.film_id IS NULL;


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT fa.actor_id, first_name, last_name, COUNT(*) AS count
FROM actor a
JOIN film_actor fa on a.actor_id = fa.actor_id
JOIN film_category fc on fa.film_id = fc.film_id
JOIN category c on fc.category_id = c.category_id
WHERE c.name = 'Children'
GROUP BY fa.actor_id, first_name, last_name
ORDER BY count DESC
LIMIT 3;