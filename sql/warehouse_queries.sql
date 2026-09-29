-- Current customer dimension rows
SELECT customer_id, customer_name, city, segment FROM dim_customer WHERE is_current=1;
-- Revenue by channel
SELECT channel, ROUND(SUM(revenue),2) revenue FROM fact_order GROUP BY channel ORDER BY revenue DESC;
