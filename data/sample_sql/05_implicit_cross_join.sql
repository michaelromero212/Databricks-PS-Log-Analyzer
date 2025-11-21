-- Implicit cross join
SELECT t1.col1, t2.col2
FROM table1 t1
JOIN table2 t2
WHERE t1.id > 100;
