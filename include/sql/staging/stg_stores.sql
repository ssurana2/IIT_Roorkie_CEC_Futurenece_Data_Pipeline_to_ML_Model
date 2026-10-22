-- Staging Transformation: Stores
-- Impute missing zone for S07 (BTM Layout -> South), cast capacity, and check validity

CREATE OR REPLACE TABLE stg_stores AS
SELECT 
    store_id,
    store_name,
    CASE 
        WHEN (zone IS NULL OR TRIM(zone) = '') AND store_id = 'S07' THEN 'South'
        ELSE TRIM(zone)
    END AS zone,
    area,
    pincode,
    TRY_CAST(open_date AS DATE) AS open_date,
    manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
    CASE 
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    CASE 
        WHEN TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0 THEN 'Store capacity <= 0'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM raw_stores;
