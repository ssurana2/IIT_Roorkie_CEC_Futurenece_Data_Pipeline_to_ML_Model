-- Mart Layer: Daily Store KPIs (Business Question 1)
-- Answers: Daily net revenue by zone and by store (delivered orders only, after discount)

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT 
    o.order_date,
    s.zone,
    s.store_id,
    s.store_name,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders_count,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0.0 END), 2) AS total_net_revenue,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue END), 2) AS avg_order_value
FROM stg_orders o
LEFT JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'VALID'
  AND o.order_status = 'DELIVERED'
GROUP BY o.order_date, s.zone, s.store_id, s.store_name
ORDER BY o.order_date DESC, s.zone, total_net_revenue DESC;
