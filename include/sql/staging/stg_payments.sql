-- Staging Transformation: Payments
-- Standardize payment_mode, cast types, evaluate quality rules, and tag dq_status

CREATE OR REPLACE TABLE stg_payments AS
WITH deduplicated_payments AS (
    SELECT 
        payment_id,
        order_id,
        TRY_CAST(payment_ts AS TIMESTAMP) AS payment_ts,
        CASE 
            WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        TRY_CAST(amount_paid AS DOUBLE) AS amount_paid,
        UPPER(TRIM(payment_status)) AS payment_status,
        gateway_ref,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_payments
)
SELECT 
    payment_id,
    order_id,
    payment_ts,
    payment_mode,
    amount_paid,
    payment_status,
    gateway_ref,
    CASE 
        WHEN rn > 1 THEN 'REJECTED'
        WHEN amount_paid <= 0 AND payment_status != 'REFUNDED' THEN 'REJECTED'
        WHEN payment_ts > TIMESTAMP '2026-12-31 23:59:59' THEN 'REJECTED'
        ELSE 'VALID'
    END AS dq_status,
    CASE 
        WHEN rn > 1 THEN 'Duplicate payment_id'
        WHEN amount_paid <= 0 AND payment_status != 'REFUNDED' THEN 'Non-positive amount for non-refunded payment'
        WHEN payment_ts > TIMESTAMP '2026-12-31 23:59:59' THEN 'Future payment timestamp'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM deduplicated_payments;
