-- Staging Transformation: Products
-- Deduplicate, cast types, evaluate cost vs MRP, and flag data quality issues

CREATE OR REPLACE TABLE stg_products AS
WITH ranked_products AS (
    SELECT 
        product_id,
        product_name,
        category,
        sub_category,
        brand,
        TRY_CAST(mrp AS DOUBLE) AS mrp,
        TRY_CAST(cost_price AS DOUBLE) AS cost_price,
        unit,
        TRY_CAST(is_perishable AS BOOLEAN) AS is_perishable,
        TRY_CAST(launch_date AS DATE) AS launch_date,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC) AS rn
    FROM raw_products
)
SELECT 
    product_id,
    product_name,
    COALESCE(category, 'Unknown') AS category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
    CASE 
        WHEN mrp <= 0 OR cost_price <= 0 THEN 'REJECTED'
        WHEN category IS NULL THEN 'REJECTED'
        WHEN cost_price > mrp THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_status,
    CASE 
        WHEN mrp <= 0 THEN 'MRP <= 0'
        WHEN category IS NULL THEN 'Missing category'
        WHEN cost_price > mrp THEN 'Cost price exceeds MRP'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM ranked_products
WHERE rn = 1;
