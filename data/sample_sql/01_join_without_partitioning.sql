-- Join without partitioning causing shuffle
SELECT *
FROM transactions t
JOIN customers c
  ON t.customer_id = c.id;
