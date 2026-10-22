-- Mart Layer: Payment Reconciliation (Business Question 4)
-- Answers: Revenue by payment mode, and the total amount that doesn't reconcile with the payment gateway

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH reconciled AS (
    SELECT 
        COALESCE(o.payment_mode, p.payment_mode, 'Unknown') AS payment_mode,
        o.order_id,
        o.order_amount,
        o.net_revenue,
        p.payment_id,
        p.amount_paid,
        p.payment_status,
        o.order_status,
        CASE 
            WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL THEN TRUE
            WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN TRUE
            WHEN o.order_status = 'DELIVERED' AND p.payment_status != 'SUCCESS' THEN TRUE
            WHEN p.payment_status = 'SUCCESS' AND ABS(COALESCE(o.order_amount, 0) - COALESCE(p.amount_paid, 0)) > 0.01 THEN TRUE
            WHEN o.order_id IS NULL AND p.payment_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_mismatch,
        CASE 
            WHEN o.order_status = 'DELIVERED' AND p.payment_id IS NULL THEN o.order_amount
            WHEN o.order_status = 'CANCELLED' AND p.payment_status = 'SUCCESS' THEN p.amount_paid
            WHEN o.order_status = 'DELIVERED' AND p.payment_status != 'SUCCESS' THEN o.order_amount
            WHEN p.payment_status = 'SUCCESS' AND ABS(COALESCE(o.order_amount, 0) - COALESCE(p.amount_paid, 0)) > 0.01 THEN ABS(o.order_amount - p.amount_paid)
            WHEN o.order_id IS NULL AND p.payment_id IS NOT NULL THEN p.amount_paid
            ELSE 0.0
        END AS unreconciled_amount
    FROM stg_orders o
    FULL OUTER JOIN stg_payments p ON o.order_id = p.order_id
    WHERE o.dq_status = 'VALID' OR o.dq_status IS NULL
)
SELECT 
    payment_mode,
    ROUND(SUM(CASE WHEN order_status = 'DELIVERED' THEN net_revenue ELSE 0.0 END), 2) AS delivered_revenue,
    COUNT(CASE WHEN is_mismatch THEN 1 END) AS mismatch_count,
    ROUND(SUM(unreconciled_amount), 2) AS total_unreconciled_amount
FROM reconciled
GROUP BY payment_mode
ORDER BY delivered_revenue DESC;
