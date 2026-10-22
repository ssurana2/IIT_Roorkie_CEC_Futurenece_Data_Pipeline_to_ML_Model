-- Mart Layer: Category Gross Margin (Business Question 2)
-- Answers: Revenue and gross margin by product category (margin = revenue - quantity * cost_price)

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT 
    COALESCE(p.category, 'Unknown') AS category,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0.0 END), 2) AS total_revenue,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue - (o.quantity * p.cost_price) ELSE 0.0 END), 2) AS gross_margin,
    ROUND(100.0 * SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue - (o.quantity * p.cost_price) ELSE 0.0 END) / 
          NULLIF(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0.0 END), 0), 2) AS gross_margin_pct
FROM stg_orders o
LEFT JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'VALID'
  AND o.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY total_revenue DESC;
