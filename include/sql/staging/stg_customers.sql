-- Staging Transformation: Customers
-- Deduplicate, cast types, standardize loyalty tier, and mask PII (DPDP Act)

CREATE OR REPLACE TABLE stg_customers AS
WITH ranked_customers AS (
    SELECT 
        customer_id,
        full_name,
        email,
        phone,
        city,
        area,
        pincode,
        TRY_CAST(signup_date AS DATE) AS signup_date,
        CASE 
            WHEN LOWER(TRIM(loyalty_tier)) = 'silver' THEN 'Silver'
            WHEN LOWER(TRIM(loyalty_tier)) = 'bronze' THEN 'Bronze'
            WHEN LOWER(TRIM(loyalty_tier)) = 'gold' THEN 'Gold'
            WHEN LOWER(TRIM(loyalty_tier)) = 'platinum' THEN 'Platinum'
            ELSE 'Bronze'
        END AS loyalty_tier,
        is_active,
        _loaded_at,
        _source_file,
        _run_id,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC, _loaded_at DESC) AS rn
    FROM raw_customers
)
SELECT 
    customer_id,
    full_name,
    -- PII Masking: email (pr***@example.com)
    CASE 
        WHEN email IS NULL OR NOT email LIKE '%@%' THEN 'masked_user@unknown.com'
        ELSE SUBSTRING(SPLIT_PART(email, '@', 1), 1, 2) || '***@' || SPLIT_PART(email, '@', 2)
    END AS email,
    -- PII Masking: phone (98******10)
    CASE 
        WHEN phone IS NULL OR LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '', 'g')) != 10 THEN '99******99'
        ELSE 
            SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 1, 2) || 
            '******' || 
            SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 9, 2)
    END AS phone,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
    CASE 
        WHEN signup_date > DATE '2026-12-31' THEN 'REJECTED'
        ELSE 'VALID'
    END AS dq_status,
    CASE 
        WHEN signup_date > DATE '2026-12-31' THEN 'Future signup date'
        ELSE NULL
    END AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM ranked_customers
WHERE rn = 1;
