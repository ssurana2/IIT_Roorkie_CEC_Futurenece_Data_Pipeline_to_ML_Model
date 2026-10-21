# NammaMart Quick Commerce — ETL & ELT Pipeline Assignment Submission

- **Engineer:** Sumit Kumar Surana
- **Date:** October 2026
- **Module:** FDAIE Module 2 · Data Engineering Foundations
- **Target Systems:** Apache Airflow (TaskFlow API), DuckDB, Pandas

---

## Part 0: Discovery and Data Profiling (FDE Day One)

### 1. Dataset Overview

| File Name | Total Rows | Columns | Entity Type | Primary Key | Key Observations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `orders.csv` | 2,412 | 13 | Fact (OMS) | `order_id` | Duplicate keys, invalid statuses, formula mismatches, null customer IDs |
| `payments.csv` | 2,228 | 7 | Fact (Gateway) | `payment_id` | Duplicate keys, non-positive amounts, future timestamps |
| `customers.csv` | 405 | 10 | Dimension (CRM) | `customer_id` | Duplicate customer IDs, invalid email/phone formats, mixed loyalty casing |
| `products.csv` | 152 | 10 | Dimension (Catalog) | `product_id` | Duplicate keys, null categories, non-positive MRP, cost price > MRP |
| `stores.csv` | 15 | 8 | Dimension (Master) | `store_id` | Missing zone in S07, 0 capacity in S12 |
| `orders_2026-10-01_increment.csv` | 160 | 13 | Incremental Feed | `order_id` | 10 status corrections (RETURNED) for September, 150 new orders |

---

### 2. Comprehensive Data Issues Log

| File | Key Identifier | Column with Issue | Problem Observed | DQ Dimension | Proposed Handling Rule | Severity |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `orders.csv` | `order_id` | `order_id` | Non-unique order IDs (duplicate rows) | **Uniqueness** | Deduplicate by retaining latest timestamp or quarantine duplicates | **Critical** |
| `orders.csv` | 18 rows | `customer_id` | Missing/null customer ID | **Completeness** | Route to quarantine; orders cannot be attributed to customer | **Critical** |
| `orders.csv` | 10 rows | `quantity` | Quantity $\le 0$ | **Accuracy** | Quarantine; order items must have positive quantity | **Critical** |
| `orders.csv` | Multiple | `payment_mode` | Inconsistent casing/typos (`upi`, `UPI `, `CARD`, `cod`, `Cashh`) | **Validity** | Standardize via mapping dictionary (`UPI`, `Card`, `Cash on Delivery`, `Wallet`) | **Warning** |
| `orders.csv` | Multiple | `order_status` | Invalid values (`delivered` in lower case, `Pending??`) | **Validity** | Standardize case to uppercase; quarantine undefined statuses like `Pending??` | **Critical** |
| `orders.csv` | 8 rows | `delivery_minutes` | Null delivery minutes for non-cancelled orders | **Completeness** | Quarantine delivered orders lacking SLA duration; alert ops | **Critical** |
| `orders.csv` | 3 rows | `discount_pct` | Discount percentage $> 100\%$ | **Validity** | Quarantine; discounts must fall in $[0, 100]$ | **Critical** |
| `orders.csv` | 18 rows | `order_amount` | Discrepancy between stored amount and computed formula | **Accuracy** | Recompute and reconcile; quarantine rows exceeding threshold | **Critical** |
| `orders.csv` | 2 rows | `order_ts` | Null order timestamp | **Completeness** | Quarantine; cannot report in daily timeline without timestamp | **Critical** |
| `orders.csv` | 1+ rows | `order_ts` | Future-dated timestamp (`2027-01-01 09:00:00`) | **Timeliness** | Quarantine; orders cannot occur after pipeline run date | **Critical** |
| `orders.csv` | 9 rows | `product_id` | Orphaned foreign key (not in `products.csv`) | **Consistency** | Quarantine order from mart joins; alert catalog team | **Critical** |
| `orders.csv` | 5 rows | `store_id` | Orphaned foreign key (not in `stores.csv`) | **Consistency** | Quarantine order from store reports; alert ops | **Critical** |
| `payments.csv` | `payment_id` | `payment_id` | Duplicate payment IDs (4 duplicate records) | **Uniqueness** | Quarantine duplicate payment IDs to avoid overcounting captured cash | **Critical** |
| `payments.csv` | 10 rows | `amount_paid` | Amount paid $\le 0$ for non-refunded transactions | **Accuracy** | Quarantine; non-refunded payments must be positive | **Critical** |
| `payments.csv` | 1+ rows | `payment_ts` | Future-dated payment timestamp (`2027-01-01`) | **Timeliness** | Quarantine future payment events | **Critical** |
| `payments.csv` | Multiple | `payment_mode` | Casing/spelling anomalies (`upi`, `CARD`, `cashh`) | **Validity** | Standardize values using clean payment mode map | **Warning** |
| `payments.csv` | 8 rows | `order_id` | Orphaned payment (order_id does not exist in OMS) | **Consistency** | Quarantine to payment reconciliation table as unmatched | **Critical** |
| `customers.csv` | `customer_id` | `customer_id` | 5 duplicate customer IDs | **Uniqueness** | Deduplicate customer master on `customer_id` (retain latest profile) | **Critical** |
| `customers.csv` | 4 rows | `email` | Null email addresses | **Completeness** | Retain row but populate with masked placeholder | **Warning** |
| `customers.csv` | 7 rows | `phone` | Phone number not exactly 10 digits | **Validity** | Flag as invalid PII; mask before exposure | **Warning** |
| `customers.csv` | 12 rows | `email` | Malformed email structure (missing `@` or domain) | **Validity** | Flag invalid format; mask before exposure | **Warning** |
| `customers.csv` | Multiple | `city` | Null city values | **Completeness** | Impute with `'Bengaluru'` default or mark as unknown | **Warning** |
| `customers.csv` | Multiple | `loyalty_tier` | Inconsistent casing (`SILVER`, `Silver`, `bronze`, `BRONZE`) | **Consistency** | Standardize to Title Case (`Bronze`, `Silver`, `Gold`, `Platinum`) | **Warning** |
| `customers.csv` | 1+ rows | `signup_date` | Future-dated signup (`2027-01-05`) | **Timeliness** | Quarantine or flag customer profile as future-dated | **Critical** |
| `products.csv` | `product_id` | `product_id` | 2 duplicate product IDs | **Uniqueness** | Deduplicate catalog on `product_id` | **Critical** |
| `products.csv` | 3 rows | `category` | Null product category | **Completeness** | Impute with `'Uncategorized'` or quarantine | **Critical** |
| `products.csv` | 1 row | `mrp` | MRP $\le 0$ | **Accuracy** | Quarantine product; items cannot sell with zero or negative price | **Critical** |
| `products.csv` | 5 rows | `cost_price` | Cost price exceeds MRP (`cost_price > mrp`) | **Accuracy** | Flag as gross negative margin alert to category managers | **Critical** |
| `stores.csv` | `S07` | `zone` | Null/empty zone for BTM Layout | **Completeness** | Impute zone (`South`) based on area master | **Critical** |
| `stores.csv` | `S12` | `capacity_orders_per_day` | Recorded capacity is 0 for Rajajinagar | **Accuracy** | Flag operational configuration defect with store ops | **Critical** |

---

### 3. Referential Integrity & Reconciliation Findings

1. **Foreign Key Integrity:**
   - **9 Orders reference invalid `product_id`s:** These items cannot join against the catalog and would break margin calculations.
   - **5 Orders reference invalid `store_id`s:** These orders cannot be mapped to any store, manager, or zone.
   - **8 Payments reference invalid `order_id`s:** Phantom payments where money was processed but no corresponding order exists in the order management system.
2. **Payment vs. Order Reconciliation:**
   - **156 Cancelled orders with no payment record:** Valid according to business logic (cancelled orders do not capture payment).
   - **27 DELIVERED orders with no payment record:** Severe revenue leakage or missing gateway export files.
   - **1 RETURNED order with no payment record.**

---

### 4. Client Clarification Questions (FDE to Leadership)

1. **Reconciliation of Unpaid Delivered Orders:**
   > *"We uncovered 27 orders marked as `DELIVERED` that have zero corresponding payment transactions in the payment gateway export. Are these collected via offline cash reconciliations not yet synced, or should these be quarantined as uncollected revenue defects?"*
2. **Handling Unmatched Product IDs in Orders:**
   > *"9 orders contain product IDs that do not exist in `products.csv`. For financial margin reports, should these be quarantined completely from CFO numbers, or should we assign them to an `UNKNOWN_PRODUCT` fallback category to preserve revenue reporting while flagging the catalog gap?"*
3. **Thresholds for Operational Outliers & Store Zero Capacity:**
   > *"Store S12 (Rajajinagar) has its daily order capacity set to 0, and 6 delivered orders record delivery times exceeding 60 minutes (against the 10-minute promise). Should orders exceeding 60 minutes be quarantined as corrupted telemetry, or kept in reporting to reflect actual customer SLA breaches?"*

---

<!-- Placeholders for remaining parts -->
## Part A: ETL Pipeline Architecture & Implementation
*(To be populated in Phase 4)*

## Part B: ELT Pipeline Architecture & Implementation
*(To be populated in Phase 5)*

## Part C: Airflow Scheduling & Timezone Strategy
*(To be populated in Phase 6)*

## Part D: Data Quality Matrix & Scorecard

### 1. Data Quality Architectural Rationale
In our data architecture, quality rules are bifurcated based on entity role:
- **Fact Tables (`orders`, `payments`):** Carry transactional and additive financial metrics (revenue, discount, delivery duration, payment amounts). Any defect (duplicate transaction, negative quantity, unverified calculation) directly corrupts CFO dashboards and tax reconciliations. Thus, critical rule failures result in **immediate quarantine** in ETL and exclusion from analytical marts in ELT.
- **Dimension Tables (`customers`, `products`, `stores`):** Provide business context. Minor formatting defects (casing in loyalty tiers, non-standard phone formatting) are treated as **Warnings**—standardized or masked in place to prevent cascade-orphaning valid transactions.

---

### 2. The 6-Dimension Quality Matrix

| # | Dimension | Check Name | Target File | Rule Description | Severity | Action on Failure |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **Completeness** | `check_not_null_critical` | `orders.csv` | `customer_id` and `order_ts` NOT NULL; if `order_status == 'DELIVERED'`, `delivery_minutes` NOT NULL | **Critical** | Route to quarantine |
| 2 | **Completeness** | `check_product_category` | `products.csv` | `category` NOT NULL | **Critical** | Quarantine catalog item |
| 3 | **Completeness** | `check_store_zone` | `stores.csv` | `zone` NOT NULL (e.g. S07 BTM Layout) | **Warning** | Impute `South` via area master |
| 4 | **Uniqueness** | `check_unique_order_id` | `orders.csv` | `order_id` must be unique across rows | **Critical** | Quarantine duplicate rows |
| 5 | **Uniqueness** | `check_unique_payment_id` | `payments.csv` | `payment_id` must be unique across rows | **Critical** | Quarantine duplicate rows |
| 6 | **Uniqueness** | `check_unique_customer_id` | `customers.csv` | Deduplicate on `customer_id` | **Critical** | Retain latest profile |
| 7 | **Uniqueness** | `check_unique_product_id` | `products.csv` | Deduplicate on `product_id` | **Critical** | Deduplicate catalog |
| 8 | **Validity** | `check_order_status` | `orders.csv` | `order_status` in `['DELIVERED', 'RETURNED', 'CANCELLED']`; standardize casing | **Critical** | Standardize case; quarantine invalid (`Pending??`) |
| 9 | **Validity** | `check_discount_range` | `orders.csv` | $0 \le \text{discount\_pct} \le 100$ | **Critical** | Quarantine rows with discount $> 100\%$ |
| 10 | **Validity** | `check_payment_mode` | `orders.csv`, `payments.csv` | Map to `['UPI', 'Card', 'Cash on Delivery', 'Wallet']` | **Warning** | Standardize casing and whitespace |
| 11 | **Validity** | `check_loyalty_tier` | `customers.csv` | Standardize to Title Case `['Bronze', 'Silver', 'Gold', 'Platinum']` | **Warning** | Clean in place |
| 12 | **Validity** | `check_pii_formats` | `customers.csv` | `phone` is 10 digits; `email` matches standard RFC pattern | **Warning** | Log violation; mask before warehouse load |
| 13 | **Accuracy** | `check_positive_quantity` | `orders.csv` | `quantity > 0` | **Critical** | Quarantine non-positive items |
| 14 | **Accuracy** | `check_order_amount_math` | `orders.csv` | Stored `order_amount` equals computed formula within ₹0.02 | **Critical** | Quarantine calculation discrepancies |
| 15 | **Accuracy** | `check_delivery_time_limit` | `orders.csv` | `delivery_minutes < 120` | **Critical** | Quarantine impossible delivery times |
| 16 | **Accuracy** | `check_positive_payment` | `payments.csv` | `amount_paid > 0` for non-refunded transactions | **Critical** | Quarantine non-positive payments |
| 17 | **Accuracy** | `check_cost_vs_mrp` | `products.csv` | `mrp > 0` and `cost_price <= mrp` | **Critical** | Flag negative margin items to category ops |
| 18 | **Accuracy** | `check_store_capacity` | `stores.csv` | `capacity_orders_per_day > 0` | **Warning** | Alert ops on zero capacity (S12) |
| 19 | **Consistency** | `check_fk_order_product` | `orders.csv` | `product_id` exists in `products.csv` | **Critical** | Quarantine orphaned order rows |
| 20 | **Consistency** | `check_fk_order_store` | `orders.csv` | `store_id` exists in `stores.csv` | **Critical** | Quarantine orphaned order rows |
| 21 | **Consistency** | `check_fk_payment_order` | `payments.csv` | `order_id` exists in `orders.csv` | **Critical** | Write to reconciliation as orphaned payment |
| 22 | **Consistency** | `check_payment_reconciliation`| Both | `SUCCESS` payment equals `order_amount` | **Critical** | Log to `etl_payment_reconciliation` |
| 23 | **Timeliness** | `check_no_future_timestamps` | Orders, Payments, Customers | Timestamps must not exceed year 2026 | **Critical** | Quarantine future-dated records (2027) |

---

### 3. Scorecard Schema (`dq_results` in DuckDB)
Every pipeline execution populates `dq_results` to maintain an audit trail across daily runs:

```sql
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR,
    dag_id VARCHAR,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    rows_checked INTEGER,
    rows_failed INTEGER,
    status VARCHAR, -- 'PASSED' or 'FAILED'
    checked_at TIMESTAMP
);
```

---

### 4. PII Protection Specification (DPDP Act Compliance)
In compliance with the Indian Digital Personal Data Protection (DPDP) Act 2023:
- **Email:** First 2 characters preserved, local portion masked with `***`, domain preserved (`priya.rao@example.com` $\to$ `pr***@example.com`).
- **Phone:** First 2 and last 2 digits preserved, middle 6 masked (`9876543210` $\to$ `98******10`).
- **Storage:** Unmasked PII is restricted to raw ingestion and never exposed in staging, marts, or BI views.

---

### 5. Quarantine File Contract
Rejected rows are segregated immediately and exported to:
```text
include/quarantine/<your_name>/<source_file>_failed_<run_id>.csv
```
Every quarantine export includes all original columns plus a newly appended `failure_reason` column detailing the specific rules violated.

## Part E: Data Governance & Compliance
*(To be populated in Phase 7)*

## Part F: Observability & Resilience Chaos Tests
*(To be populated in Phase 6)*

## Part G: CTO Recommendation Memo (ETL vs. ELT)
*(To be populated in Phase 7)*

## Answers to Business Questions
*(To be populated after pipeline execution)*
