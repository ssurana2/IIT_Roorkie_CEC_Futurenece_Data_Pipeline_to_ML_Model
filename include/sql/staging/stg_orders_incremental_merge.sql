-- Incremental Transformation: MERGE Next-Day Feed into stg_orders
-- Updates existing order records (e.g. 10 status corrections to RETURNED) and inserts 150 new orders

WITH raw_increment_parsed AS (
    SELECT 
        order_id,
        TRY_CAST(order_ts AS TIMESTAMP) AS parsed_ts,
        order_ts AS raw_order_ts,
        customer_id,
        store_id,
        product_id,
        TRY_CAST(quantity AS INTEGER) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        CASE 
            WHEN LOWER(TRIM(payment_mode)) IN ('upi', 'upi ') THEN 'UPI'
            WHEN LOWER(TRIM(payment_mode)) IN ('card') THEN 'Card'
            WHEN LOWER(TRIM(payment_mode)) IN ('cod', 'cashh', 'cash on delivery') THEN 'Cash on Delivery'
            WHEN LOWER(TRIM(payment_mode)) = 'wallet' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        UPPER(TRIM(order_status)) AS order_status,
        channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_orders
    WHERE _source_file = 'orders_2026-10-01_increment.csv'
),
validated_increment AS (
    SELECT 
        o.*,
        ROUND(o.quantity * o.unit_price * (1.0 - o.discount_pct / 100.0), 2) AS expected_amount,
        (p.product_id IS NOT NULL) AS valid_product,
        (s.store_id IS NOT NULL) AS valid_store,
        (o.parsed_ts IS NOT NULL AND o.parsed_ts <= TIMESTAMP '2026-12-31 23:59:59') AS valid_ts
    FROM raw_increment_parsed o
    LEFT JOIN stg_products p ON o.product_id = p.product_id
    LEFT JOIN stg_stores s ON o.store_id = s.store_id
),
prepared_source AS (
    SELECT 
        order_id,
        parsed_ts AS order_ts,
        CAST(parsed_ts AS DATE) AS order_date,
        customer_id,
        store_id,
        product_id,
        quantity,
        unit_price,
        discount_pct,
        order_amount,
        payment_mode,
        order_status,
        channel,
        delivery_minutes,
        CASE 
            WHEN order_status = 'DELIVERED' THEN order_amount 
            ELSE 0.0 
        END AS net_revenue,
        (order_status = 'DELIVERED' AND delivery_minutes <= 15) AS within_15_min,
        CASE 
            WHEN rn > 1 
              OR customer_id IS NULL 
              OR order_status NOT IN ('DELIVERED', 'RETURNED', 'CANCELLED')
              OR (order_status = 'DELIVERED' AND delivery_minutes IS NULL)
              OR discount_pct < 0 OR discount_pct > 100
              OR quantity <= 0
              OR ABS(order_amount - expected_amount) > 0.02
              OR delivery_minutes >= 120
              OR NOT valid_product
              OR NOT valid_store
              OR NOT valid_ts
            THEN 'REJECTED'
            ELSE 'VALID'
        END AS dq_status,
        ARRAY_TO_STRING(
            LIST_FILTER([
                CASE WHEN rn > 1 THEN 'Duplicate order_id' ELSE NULL END,
                CASE WHEN customer_id IS NULL THEN 'Missing customer_id' ELSE NULL END,
                CASE WHEN raw_order_ts IS NULL THEN 'Missing order_ts' ELSE NULL END,
                CASE WHEN NOT valid_ts THEN 'Invalid/unparseable or future timestamp' ELSE NULL END,
                CASE WHEN order_status NOT IN ('DELIVERED', 'RETURNED', 'CANCELLED') THEN 'Invalid order_status: ' || order_status ELSE NULL END,
                CASE WHEN order_status = 'DELIVERED' AND delivery_minutes IS NULL THEN 'Missing delivery_minutes for DELIVERED order' ELSE NULL END,
                CASE WHEN discount_pct < 0 OR discount_pct > 100 THEN 'Discount out of range' ELSE NULL END,
                CASE WHEN quantity <= 0 THEN 'Non-positive quantity' ELSE NULL END,
                CASE WHEN ABS(order_amount - expected_amount) > 0.02 THEN 'Amount mismatch with formula' ELSE NULL END,
                CASE WHEN delivery_minutes >= 120 THEN 'Delivery time >= 120 mins' ELSE NULL END,
                CASE WHEN NOT valid_product THEN 'Invalid product_id: ' || product_id ELSE NULL END,
                CASE WHEN NOT valid_store THEN 'Invalid store_id: ' || store_id ELSE NULL END
            ], x -> x IS NOT NULL),
            '; '
        ) AS failure_reason,
        _loaded_at,
        _source_file,
        _run_id
    FROM validated_increment
    WHERE rn = 1
)
MERGE INTO stg_orders AS target
USING prepared_source AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN 
    UPDATE SET 
        order_status = source.order_status,
        net_revenue = source.net_revenue,
        within_15_min = source.within_15_min,
        dq_status = source.dq_status,
        failure_reason = source.failure_reason,
        _loaded_at = source._loaded_at,
        _source_file = source._source_file,
        _run_id = source._run_id
WHEN NOT MATCHED THEN 
    INSERT (
        order_id, order_ts, order_date, customer_id, store_id, product_id,
        quantity, unit_price, discount_pct, order_amount, payment_mode,
        order_status, channel, delivery_minutes, net_revenue, within_15_min,
        dq_status, failure_reason, _loaded_at, _source_file, _run_id
    )
    VALUES (
        source.order_id, source.order_ts, source.order_date, source.customer_id, source.store_id, source.product_id,
        source.quantity, source.unit_price, source.discount_pct, source.order_amount, source.payment_mode,
        source.order_status, source.channel, source.delivery_minutes, source.net_revenue, source.within_15_min,
        source.dq_status, source.failure_reason, source._loaded_at, source._source_file, source._run_id
    );
