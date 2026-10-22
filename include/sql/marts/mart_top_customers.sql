-- Mart Layer: Top Customers (Business Question 5)
-- Answers: Top 10 customers by spend, with loyalty tier (no raw email or phone shown)

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT 
    c.customer_id,
    c.full_name,
    c.loyalty_tier,
    c.email, -- masked: pr***@example.com
    c.phone, -- masked: 98******10
    COUNT(o.order_id) AS total_delivered_orders,
    ROUND(SUM(o.net_revenue), 2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id = c.customer_id
WHERE o.dq_status = 'VALID'
  AND o.order_status = 'DELIVERED'
GROUP BY c.customer_id, c.full_name, c.loyalty_tier, c.email, c.phone
ORDER BY total_spend DESC
LIMIT 10;
