# NammaMart Quick Commerce — ETL & ELT Production Data Pipelines

**IIT Roorkee CEC — Data Pipeline to ML Model (Module 2: Data Engineering Foundations)**  
**Author:** Sumit Kumar Surana  
**Environment:** Apache Airflow (Astro CLI Runtime) | Python 3.13.9 | DuckDB | Pandas  

---

## 1. Executive Summary & Client Brief

NammaMart Quick Commerce Pvt Ltd is a 10-minute instant grocery delivery company operating across 15 dark stores in Bengaluru (North, South, East, and West zones). Prior to this project, leadership daily operations reviews at 9:30 AM were crippled by manual Excel reporting:
* **Late delivery:** Operational numbers arrived well after 10:30 AM, forcing leadership to make morning decisions on gut feel.
* **Payment reconciliation failure:** Order revenue consistently failed to balance against payment gateway receipts.
* **Inflated metrics:** Duplicate orders, internal test records, and impossible delivery outliers distorted the 10-minute SLA metric.
* **Privacy & regulatory liability:** Raw customer PII (unmasked names, phone numbers, and emails) circulated via unencrypted spreadsheets.

As a Forward Deployed Engineer (FDE), this repository delivers an enterprise-grade, automated data pipeline system in **Apache Airflow** meeting all ten client acceptance criteria, scheduled to run at **9:00 AM IST, Monday through Saturday**, providing reconciled, audit-proof, and privacy-compliant reporting data before the 9:30 AM leadership stand-up.

---

## 2. Dual Pipeline Architecture: ETL vs. ELT

To resolve the architectural tension between Finance (who demand pre-load data quality gates to guarantee zero corrupt rows reach reporting tables) and Analytics/Audit (who demand complete, immutable raw data retention for historical replay), both **ETL** and **ELT** pipelines were built on the same source dataset.

```
                              ┌────────────────────────────────────────┐
                              │ NammaMart Source Exports (5,372 rows)  │
                              │ orders, payments, customers, products, │
                              │ stores, orders_2026-10-01_increment   │
                              └───────────────────┬────────────────────┘
                                                  │
                   ┌──────────────────────────────┴──────────────────────────────┐
                   ▼                                                             ▼
     ┌───────────────────────────┐                                 ┌───────────────────────────┐
     │    ETL Pipeline (Python)  │                                 │    ELT Pipeline (SQL)     │
     │ nammamart_etl_sumit_...py │                                 │ nammamart_elt_sumit_...py │
     └─────────────┬─────────────┘                                 └─────────────┬─────────────┘
                   │                                                             │
     ┌─────────────┴─────────────┐                                 ┌─────────────┴─────────────┐
     │ Extract: Parallel extract │                                 │ Bronze: Load Raw CSVs     │
     │ Validate: 6 DQ dimensions │                                 │ (raw_orders, raw_pay...)  │
     │ Quarantine: 98 bad rows   │                                 └─────────────┬─────────────┘
     │ Transform: Mask PII, join │                                               │
     └─────────────┬─────────────┘                                 ┌─────────────┴─────────────┐
                   │                                                             │ Silver: Staging SQL Views │
     ┌─────────────┴─────────────┐                                 │ (stg_orders, stg_pay...)  │
     │ Load Star Schema (DuckDB) │                                 │ DQ flags, PII masking     │
     │ etl_fact_orders           │                                 └─────────────┬─────────────┘
     │ etl_dim_customer/prod/stor│                                               │
     └─────────────┬─────────────┘                                 ┌─────────────┴─────────────┐
                   │                                                             │ Gold: Marts & Merge       │
     ┌─────────────┴─────────────┐                                 │ ANSI MERGE incremental    │
     │ Reconciliation & Reports  │                                 │ mart_daily_store_kpis     │
     │ etl_payment_reconciliation│                                 │ mart_category_margin...   │
     └───────────────────────────┘                                 └───────────────────────────┘
```

### A. Python ETL Pipeline (`dags/nammamart_etl_sumit_kumar_surana.py`)
* **Extract:** 5 concurrent TaskFlow tasks extracting source CSVs.
* **Validate & Quarantine:** Evaluates 6 data quality dimensions in pandas; routes **98 defective order rows** directly into `include/quarantine/sumit_kumar_surana/` with detailed `failure_reason` codes (`FORMULA_AMOUNT_MISMATCH`, `NEGATIVE_QUANTITY`, `UNKNOWN_PRODUCT_FK`, etc.).
* **Transform:** Applies PII masking (`pr***@example.com`, `98******21`), standardizes payment and status categoricals, and derives business metrics (`net_revenue`, `gross_margin`, `within_15_min`).
* **Load:** Idempotently populates a star schema (`etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`) in DuckDB.
* **Reconcile:** Conducts outer-join reconciliation against `payments.csv` and generates `etl_payment_reconciliation`.

### B. SQL ELT Medallion Pipeline (`dags/nammamart_elt_sumit_kumar_surana.py`)
* **Bronze (Raw Layer):** Exact raw loads with metadata lineage (`_loaded_at`, `_source_file`, `_run_id`).
* **Silver (Staging Layer):** Pure SQL models (`include/sql/staging/`) applying type casting, regex-based PII masking, categorical harmonization, and data quality status tagging.
* **Gold (Marts Layer):** Pure SQL business models (`include/sql/marts/`) answering all operational and financial questions.
* **Incremental ANSI MERGE:** Merges `orders_2026-10-01_increment.csv` via SQL `MERGE` into staging, updating 10 September return status changes while inserting 150 new October orders without duplication.

### C. Cross-Pipeline Reconciliation Proof
Both pipelines process the exact same 2,412 raw September order records and arrive at **100% identical financial figures** with zero variance:
* Total Delivered Net Revenue: **₹1,137,446.00** across **2,042 delivered orders**.
* Category Net Revenue & Gross Margin Variance: **₹0.00 across all 7 categories**.

---

## 3. Repository Directory Structure

```
.
├── README.md                                  # Root technical documentation & quickstart guide
├── SUBMISSION.md                              # Complete FDE Technical Report (Parts 0–G & Q1–Q6)
├── Dockerfile                                 # Astro Runtime Docker image definition
├── requirements.txt                           # Python package dependencies (duckdb, pandas, pendulum)
├── packages.txt                               # OS-level package dependencies
│
├── dags/
│   ├── nammamart_etl_sumit_kumar_surana.py    # Production ETL TaskFlow DAG
│   └── nammamart_elt_sumit_kumar_surana.py    # Production ELT Medallion DAG
│
├── data-profiling/
│   ├── README.md                              # Guide on running exploratory profiling notebook
│   └── main.ipynb                             # Jupyter notebook with complete data sanity checks
│
├── include/
│   ├── data/nammamart/                        # Immutable source CSV exports (5,372 rows)
│   │   ├── orders.csv
│   │   ├── payments.csv
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── stores.csv
│   │   └── orders_2026-10-01_increment.csv
│   ├── sql/                                   # Production ANSI SQL transformation models
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_payments.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_stores.sql
│   │   │   └── stg_orders_incremental_merge.sql
│   │   └── marts/
│   │       ├── mart_daily_store_kpis.sql
│   │       ├── mart_category_margin.sql
│   │       ├── mart_delivery_sla.sql
│   │       ├── mart_payment_reconciliation.sql
│   │       └── mart_top_customers.sql
│   ├── quarantine/sumit_kumar_surana/         # Rejected records with failure reasons
│   │   └── orders_failed_*.csv
│   └── warehouse_sumit_kumar_surana.duckdb    # DuckDB local persistent warehouse
│
└── assignment/
    ├── ASSIGNMENT.md                          # Original client specification & grading rubric
    └── nammamart_dataset/                     # Reference copy of raw dataset
```

---

## 4. Scheduling & Timezone Rationale

* **Cron Schedule:** `0 9 * * 1-6` (Every Monday through Saturday at 09:00:00).
* **Explicit Timezone:** Defined using `pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")`.
* **Airflow UI Display:** Correctly reflects `03:30 UTC` (UTC is 5 hours and 30 minutes behind Indian Standard Time).
* **Why Default UTC Fails:** If `start_date` or the schedule were left in UTC, a 9:00 AM UTC execution would trigger at **2:30 PM IST** — a 5.5-hour delay that misses the 9:30 AM leadership review entirely.
* **Why `catchup=False`:** Daily quick commerce reports require today's fresh operational snapshot. Setting `catchup=True` on a local embedded warehouse like DuckDB would cause Airflow to trigger dozens of historical backfill runs simultaneously, causing fatal database file-lock conflicts (`duckdb.duckdb.IOException: Could not set lock on file`).

---

## 5. Data Governance & DPDP Act 2023 Compliance

* **Regulatory Framework:** Compliance with India's **Digital Personal Data Protection (DPDP) Act 2023**.
* **PII Protection:** Customer names, emails, and phone numbers are strictly classified as Digital Personal Data.
* **Masking Standards:**
  * Email: `pr***@example.com` (first 2 characters + `***` + domain).
  * Phone: `98******21` (first 2 and last 2 digits preserved, middle 6 masked).
* **Why Mask Rather than Delete:** Masking preserves referential integrity, loyalty tier attribution, and longitudinal cohort analysis while rendering personal identities unrecoverable without privileged access.
* **Role-Based Access Control (RBAC):**
  * *Raw Tables:* Restricted to Data Engineering Admins & DPO.
  * *Staging Views:* Available to Data Engineers & Compliance Auditors.
  * *Reporting Marts:* Available to Ops Managers, Finance Analysts, and Executive BI dashboards.
  * *Quarantine Tables:* Available to Operations Quality Leads & Data Engineering for supplier triage.
* **Retention Policy:** 7-year statutory retention for financial transactions and audit logs under Section 128 of the Companies Act 2013; rolling 90-day retention for operational raw staging files.

---

## 6. Observability & Chaos Testing

Both pipelines implement end-to-end observability across the five core data observability pillars:
1. **Freshness:** Evaluated via max timestamp boundaries against current run time.
2. **Volume:** Monitored against historical averages with warning alerts if daily volume drops > 90%.
3. **Schema:** Pre-load validation ensuring strict column naming and data type enforcement.
4. **Distribution:** Unit price and delivery time sanity bounds (catching price inflation and delivery anomalies).
5. **Lineage:** Metadata columns (`_loaded_at`, `_source_file`, `_run_id`) tracking every reporting metric back to source CSV rows.

### Audit & Scorecard Tables
* `dq_results`: Captures check name, dimension, severity, rows checked, rows failed, status, and execution timestamp for every rule run.
* `pipeline_runs`: Written by a terminal task with `trigger_rule="all_done"`, logging `run_id`, `dag_id`, execution duration, rows extracted, rows loaded, rows quarantined, and pipeline status.

### 10-Experiment Chaos Matrix (Summary)
All 10 failure experiments (Happy path, Re-run idempotency, Incremental load, Missing file, New bad data, Schema modification, Volume anomaly, Distribution drift, Stale data, and Task recovery) were tested. Full details and triage steps are documented in Section F of [SUBMISSION.md](SUBMISSION.md).

---

## 7. Business Questions Answered (Key Golden Metrics)

| # | Business Metric | SQL Target Table | Golden Result (September 2026) |
| :- | :--- | :--- | :--- |
| **Q1** | Daily Net Revenue by Zone & Store | `mart_daily_store_kpis` | **₹1,137,446.00** total delivered revenue (South Zone leads with ₹368.1k; Indiranagar top store at ₹105.7k) |
| **Q2** | Category Net Revenue & Gross Margin | `mart_category_margin` | **₹261,178.08 overall margin (22.96%)**; Staples leads revenue (₹266.8k, 21.99%), Dairy & Bakery leads margin % (26.33%) |
| **Q3** | Delivery Duration & 15-min SLA % | `mart_delivery_sla` | **14.18 min** network average duration; **69.59%** orders delivered $\le$ 15 min (Koramangala best at 81.33%) |
| **Q4** | Payment Reconciliation & Discrepancies | `mart_payment_reconciliation` | **₹1,113,934.60** reconciled; **₹23,511.40 variance** across 27 delivered orders with missing gateway records |
| **Q5** | Top 10 Customers by Spend (Masked PII)| `mart_top_customers` | Top customer **C00142** (Gold tier, 14 orders, **₹12,480.50** spend, phone `98******07`) |
| **Q6** | Quarantine Breakdown by Failure Reason | `dq_results` / Quarantine CSV | **98 rejected order rows** (31 negative quantities, 18 formula errors, 18 null customers, 9 invalid products, etc.) |

---

## 8. Quickstart & How to Run

### Prerequisites
* **Docker Desktop** installed and running.
* **Astro CLI** installed (`brew install astro` on macOS).
* **Python 3.13.9** (used for exploratory data profiling in `data-profiling/main.ipynb`).

### Step 1: Clone the Repository
```bash
git clone <repository_url>
cd IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model
```

### Step 2: Start Apache Airflow
Launch the local Astronomer development environment:
```bash
astro dev start
```
This initializes the 5 standard Airflow containers (Scheduler, Webserver/API Server, Triggerer, DAG Processor, and Postgres metadata database).

### Step 3: Verify DAG Parsing
Verify that both DAGs parse cleanly without syntax or import errors:
```bash
astro dev run dags list-import-errors
```

### Step 4: Access the Airflow UI
* Navigate to `http://localhost:8080` in your browser.
* Log in with default credentials:
  * **Username:** `admin`
  * **Password:** `admin`
* Locate both DAGs:
  * `nammamart_etl_sumit_kumar_surana`
  * `nammamart_elt_sumit_kumar_surana`

### Step 5: Execute Pipeline Runs
Trigger each DAG via the UI or directly from the terminal:
```bash
# Test ETL DAG
astro dev run dags test nammamart_etl_sumit_kumar_surana

# Test ELT DAG
astro dev run dags test nammamart_elt_sumit_kumar_surana
```

### Step 6: Query Warehouse Deliverables
Connect to the local DuckDB database using Python or the DuckDB CLI:
```bash
duckdb include/warehouse_sumit_kumar_surana.duckdb
```
Example query:
```sql
-- Verify revenue reconciliation between ETL and ELT
SELECT 
    'ETL' AS pipeline, 
    SUM(order_amount) AS total_revenue 
FROM etl_fact_orders 
WHERE order_status = 'DELIVERED'
UNION ALL
SELECT 
    'ELT' AS pipeline, 
    SUM(net_revenue) AS total_revenue 
FROM mart_daily_store_kpis;
```

### Step 7: Explore Data Profiling
To inspect the initial profiling and exploratory data analysis:
* See instructions in [data-profiling/README.md](data-profiling/README.md).
* Open and run `data-profiling/main.ipynb` using Python 3.13.9.

---

## 9. Full Technical Submission

For the complete technical report, including:
* Part 0: Complete Data Issues Log & Profiling Matrix
* Part C: Detailed Scheduling & Timezone Answers
* Part E: In-Depth Governance & Compliance Blueprint
* Part F: 10-Experiment Chaos Engineering Log & Observability Pillar Mapping
* Part G: One-Page CTO Recommendation Memorandum to Sameer
* Complete SQL code and result tables for Questions 1 through 6

Please refer to the comprehensive [SUBMISSION.md](SUBMISSION.md) document.
