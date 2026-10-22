-- Mart Layer: Delivery SLA (Business Question 3)
-- Answers: Average delivery time per store, and the % of delivered orders within 15 minutes

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT 
    s.store_id,
    s.store_name,
    s.zone,
    COUNT(*) AS total_delivered_orders,
    ROUND(AVG(o.delivery_minutes), 1) AS avg_delivery_minutes,
    MIN(o.delivery_minutes) AS min_delivery_minutes,
    MAX(o.delivery_minutes) AS max_delivery_minutes,
    SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) AS delivered_within_15_min_count,
    ROUND(100.0 * SUM(CASE WHEN o.within_15_min THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_within_15_min
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'VALID'
  AND o.order_status = 'DELIVERED'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY pct_within_15_min DESC;
