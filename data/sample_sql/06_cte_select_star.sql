-- Complex CTE with hidden SELECT *
WITH customer_stats AS (
    SELECT * 
    FROM customer_events
    WHERE event_date > '2023-01-01'
),
sales_summary AS (
    SELECT customer_id, SUM(amount) as total_spent
    FROM transactions
    GROUP BY customer_id
)
SELECT c.*, s.total_spent
FROM customer_stats c
JOIN sales_summary s ON c.customer_id = s.customer_id;
