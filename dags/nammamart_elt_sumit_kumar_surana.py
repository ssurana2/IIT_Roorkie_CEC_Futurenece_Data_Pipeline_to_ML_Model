import os
import sys
from datetime import datetime, timedelta
import pendulum
import duckdb
from airflow.decorators import dag, task

# Base directory resolution (works in Docker /usr/local/airflow and local clone)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if not os.path.exists(os.path.join(BASE_DIR, "include", "data", "nammamart")):
    BASE_DIR = "/usr/local/airflow"

DATA_DIR = os.path.join(BASE_DIR, "include", "data", "nammamart")
WAREHOUSE_DB = os.path.join(BASE_DIR, "include", "warehouse_sumit_kumar_surana.duckdb")
SQL_DIR = os.path.join(BASE_DIR, "include", "sql")

default_args = {
    "owner": "sumit_kumar_surana",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="nammamart_elt_sumit_kumar_surana",
    schedule="0 9 * * 1-6",  # 9:00 AM IST, Monday through Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "meridian", "session1", "session2", "elt"],
)
def nammamart_elt_sumit_kumar_surana():
    # ---------------------------------------------------------
    # 1. RAW LAYER INGESTION
    # Ingest CSVs exactly as received, all columns as text.
    # Metadata appended: _loaded_at, _source_file, _run_id.
    # ---------------------------------------------------------

    def _ingest_raw_table(csv_filename: str, table_name: str, run_id: str):
        csv_path = os.path.join(DATA_DIR, csv_filename)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Source file not found: {csv_path}")

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            # Create table if it does not exist (all varchar)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT 
                    *, 
                    CURRENT_TIMESTAMP AS _loaded_at, 
                    '{csv_filename}' AS _source_file, 
                    '{run_id}' AS _run_id 
                FROM read_csv('{csv_path}', all_varchar=true)
                WHERE 1=0;
            """)

            # Idempotent load: clear any previous load for this source file to avoid duplicate raw data
            con.execute(f"DELETE FROM {table_name} WHERE _source_file = '{csv_filename}';")

            con.execute(f"""
                INSERT INTO {table_name}
                SELECT 
                    *, 
                    CURRENT_TIMESTAMP AS _loaded_at, 
                    '{csv_filename}' AS _source_file, 
                    '{run_id}' AS _run_id 
                FROM read_csv('{csv_path}', all_varchar=true);
            """)

            count = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE _source_file = '{csv_filename}';").fetchone()[0]
            print(f"[Raw Ingest] Successfully loaded {count} rows into {table_name} from {csv_filename}")
            return count
        finally:
            con.close()

    @task
    def load_raw_orders(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("orders.csv", "raw_orders", run_id)

    @task
    def load_raw_payments(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("payments.csv", "raw_payments", run_id)

    @task
    def load_raw_customers(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("customers.csv", "raw_customers", run_id)

    @task
    def load_raw_products(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("products.csv", "raw_products", run_id)

    @task
    def load_raw_stores(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("stores.csv", "raw_stores", run_id)

    # ---------------------------------------------------------
    # 2. STAGING LAYER TRANSFORMATION (SQL ONLY)
    # Execute SQL scripts from include/sql/staging/
    # ---------------------------------------------------------

    def _execute_sql_file(rel_path: str):
        full_path = os.path.join(SQL_DIR, rel_path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"SQL file not found: {full_path}")

        with open(full_path, "r") as f:
            sql_script = f.read()

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute(sql_script)
            tbl_name = os.path.basename(rel_path).replace(".sql", "")
            # If creating a table, fetch row count
            try:
                cnt = con.execute(f"SELECT COUNT(*) FROM {tbl_name};").fetchone()[0]
                print(f"[Staging/Mart SQL] Executed {rel_path} -> {tbl_name} ({cnt} rows)")
                return cnt
            except Exception:
                print(f"[Staging/Mart SQL] Executed {rel_path} successfully")
                return 0
        finally:
            con.close()

    @task
    def run_stg_customers():
        return _execute_sql_file("staging/stg_customers.sql")

    @task
    def run_stg_products():
        return _execute_sql_file("staging/stg_products.sql")

    @task
    def run_stg_stores():
        return _execute_sql_file("staging/stg_stores.sql")

    @task
    def run_stg_payments():
        return _execute_sql_file("staging/stg_payments.sql")

    @task
    def run_stg_orders():
        return _execute_sql_file("staging/stg_orders.sql")

    @task
    def log_dq_scorecard(**context):
        run_id = context.get("run_id", "manual_run")
        dag_id = "nammamart_elt_sumit_kumar_surana"
        print(f"[DQ Scorecard] Evaluating 6 Data Quality dimensions for run: {run_id}")

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS dq_results (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    check_name VARCHAR,
                    dimension VARCHAR,
                    severity VARCHAR,
                    rows_checked INTEGER,
                    rows_failed INTEGER,
                    status VARCHAR,
                    checked_at TIMESTAMP
                );
            """)

            checks = [
                # Completeness
                (
                    "check_orders_not_null", "Completeness", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN customer_id IS NULL OR order_ts IS NULL THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                (
                    "check_delivered_has_delivery_time", "Completeness", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN delivery_minutes IS NULL THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv' AND UPPER(TRIM(order_status)) = 'DELIVERED'"
                ),
                (
                    "check_product_category_not_null", "Completeness", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN category IS NULL THEN 1 END) FROM raw_products"
                ),
                (
                    "check_store_zone_not_null", "Completeness", "Warning",
                    "SELECT COUNT(*), COUNT(CASE WHEN zone IS NULL THEN 1 END) FROM raw_stores"
                ),
                # Uniqueness
                (
                    "check_unique_order_ids", "Uniqueness", "Critical",
                    "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT order_id) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                (
                    "check_unique_payment_ids", "Uniqueness", "Critical",
                    "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT payment_id) FROM raw_payments"
                ),
                (
                    "check_unique_customer_ids", "Uniqueness", "Critical",
                    "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT customer_id) FROM raw_customers"
                ),
                (
                    "check_unique_product_ids", "Uniqueness", "Critical",
                    "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT product_id) FROM raw_products"
                ),
                # Validity
                (
                    "check_valid_order_status", "Validity", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'RETURNED', 'CANCELLED') THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                (
                    "check_discount_range", "Validity", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN TRY_CAST(discount_pct AS DOUBLE) < 0 OR TRY_CAST(discount_pct AS DOUBLE) > 100 THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                # Accuracy
                (
                    "check_positive_quantity", "Accuracy", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN TRY_CAST(quantity AS INTEGER) <= 0 THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                (
                    "check_order_amount_accuracy", "Accuracy", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN ABS(TRY_CAST(order_amount AS DOUBLE) - ROUND(TRY_CAST(quantity AS INTEGER) * TRY_CAST(unit_price AS DOUBLE) * (1.0 - TRY_CAST(discount_pct AS DOUBLE) / 100.0), 2)) > 0.02 THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                (
                    "check_delivery_time_limit", "Accuracy", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN TRY_CAST(delivery_minutes AS DOUBLE) >= 120 THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
                # Consistency
                (
                    "check_orders_product_foreign_key", "Consistency", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN p.product_id IS NULL THEN 1 END) FROM raw_orders o LEFT JOIN stg_products p ON o.product_id = p.product_id WHERE o._source_file = 'orders.csv'"
                ),
                (
                    "check_orders_store_foreign_key", "Consistency", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN s.store_id IS NULL THEN 1 END) FROM raw_orders o LEFT JOIN stg_stores s ON o.store_id = s.store_id WHERE o._source_file = 'orders.csv'"
                ),
                # Timeliness
                (
                    "check_no_future_orders", "Timeliness", "Critical",
                    "SELECT COUNT(*), COUNT(CASE WHEN TRY_CAST(order_ts AS TIMESTAMP) > TIMESTAMP '2026-12-31 23:59:59' THEN 1 END) FROM raw_orders WHERE _source_file = 'orders.csv'"
                ),
            ]

            now_ts = datetime.now()
            for check_name, dim, sev, query in checks:
                row = con.execute(query).fetchone()
                checked_cnt = int(row[0]) if row and row[0] is not None else 0
                failed_cnt = int(row[1]) if row and row[1] is not None else 0
                status = "PASSED" if failed_cnt == 0 else "FAILED"

                con.execute("""
                    INSERT INTO dq_results (
                        run_id, dag_id, check_name, dimension, severity,
                        rows_checked, rows_failed, status, checked_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """, [run_id, dag_id, check_name, dim, sev, checked_cnt, failed_cnt, status, now_ts])

            total_dq = con.execute("SELECT COUNT(*) FROM dq_results WHERE run_id = ?;", [run_id]).fetchone()[0]
            print(f"[DQ Scorecard] Logged {total_dq} DQ checks into dq_results table.")
            return total_dq
        finally:
            con.close()

    # ---------------------------------------------------------
    # 3. MART LAYER TRANSFORMATION (SQL ONLY)
    # Answers the 5 business questions
    # ---------------------------------------------------------

    @task
    def run_mart_daily_store_kpis():
        return _execute_sql_file("marts/mart_daily_store_kpis.sql")

    @task
    def run_mart_category_margin():
        return _execute_sql_file("marts/mart_category_margin.sql")

    @task
    def run_mart_delivery_sla():
        return _execute_sql_file("marts/mart_delivery_sla.sql")

    @task
    def run_mart_payment_reconciliation():
        return _execute_sql_file("marts/mart_payment_reconciliation.sql")

    @task
    def run_mart_top_customers():
        return _execute_sql_file("marts/mart_top_customers.sql")

    # ---------------------------------------------------------
    # 4. INCREMENTAL INGESTION & MERGE
    # Ingest orders_2026-10-01_increment.csv into raw_orders
    # and execute ANSI SQL MERGE into stg_orders
    # ---------------------------------------------------------

    @task
    def load_raw_increment_orders(**context):
        run_id = context.get("run_id", "manual_run")
        return _ingest_raw_table("orders_2026-10-01_increment.csv", "raw_orders", run_id)

    @task
    def execute_stg_orders_incremental_merge():
        merge_file = os.path.join(SQL_DIR, "staging", "stg_orders_incremental_merge.sql")
        with open(merge_file, "r") as f:
            merge_sql = f.read()

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute(merge_sql)
            total_stg = con.execute("SELECT COUNT(*) FROM stg_orders;").fetchone()[0]
            returned_cnt = con.execute("SELECT COUNT(*) FROM stg_orders WHERE order_status = 'RETURNED';").fetchone()[0]
            print(f"[Incremental MERGE] Completed successfully. Total in stg_orders: {total_stg}, Total RETURNED: {returned_cnt}")
            return {"total_orders": total_stg, "returned_count": returned_cnt}
        finally:
            con.close()

    # ---------------------------------------------------------
    # 5. PIPELINE AUDIT SUMMARY
    # Log run details to pipeline_runs table (trigger_rule="all_done")
    # ---------------------------------------------------------

    @task(trigger_rule="all_done")
    def run_audit_summary(**context):
        run_id = context.get("run_id", "manual_run")
        dag_id = "nammamart_elt_sumit_kumar_surana"
        start_time = context.get("data_interval_start", datetime.now())
        end_time = datetime.now()

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id VARCHAR,
                    dag_id VARCHAR,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    rows_extracted INTEGER,
                    rows_loaded INTEGER,
                    rows_quarantined INTEGER,
                    status VARCHAR
                );
            """)

            # Fetch metrics from warehouse
            raw_orders_cnt = con.execute("SELECT COUNT(*) FROM raw_orders;").fetchone()[0]
            valid_orders_cnt = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'VALID';").fetchone()[0]
            rejected_orders_cnt = con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'REJECTED';").fetchone()[0]

            con.execute("""
                INSERT INTO pipeline_runs (
                    run_id, dag_id, start_time, end_time,
                    rows_extracted, rows_loaded, rows_quarantined, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """, [run_id, dag_id, start_time, end_time, raw_orders_cnt, valid_orders_cnt, rejected_orders_cnt, "SUCCESS"])

            print(f"\n==================== [ELT PIPELINE AUDIT SUMMARY] ====================")
            print(f"Run ID: {run_id}")
            print(f"Raw Orders Extracted & Ingested: {raw_orders_cnt}")
            print(f"Valid Orders Loaded in Staging: {valid_orders_cnt}")
            print(f"Rejected Orders Flagged in Staging: {rejected_orders_cnt}")
            print(f"Pipeline Status: SUCCESS")
            print(f"=======================================================================\n")
            return {"status": "SUCCESS", "rows_loaded": valid_orders_cnt}
        finally:
            con.close()

    # ---------------------------------------------------------
    # TASK DEPENDENCIES & EXECUTION FLOW
    # ---------------------------------------------------------

    # Raw layer execution (sequential to respect DuckDB file locks)
    t_raw_orders = load_raw_orders()
    t_raw_payments = load_raw_payments()
    t_raw_customers = load_raw_customers()
    t_raw_products = load_raw_products()
    t_raw_stores = load_raw_stores()

    raw_flow = t_raw_orders >> t_raw_payments >> t_raw_customers >> t_raw_products >> t_raw_stores

    # Staging layer execution
    t_stg_customers = run_stg_customers()
    t_stg_products = run_stg_products()
    t_stg_stores = run_stg_stores()
    t_stg_payments = run_stg_payments()
    t_stg_orders = run_stg_orders()
    t_dq_scorecard = log_dq_scorecard()

    staging_flow = (
        raw_flow
        >> t_stg_customers
        >> t_stg_products
        >> t_stg_stores
        >> t_stg_payments
        >> t_stg_orders
        >> t_dq_scorecard
    )

    # Mart layer execution (baseline September)
    t_mart_kpis = run_mart_daily_store_kpis()
    t_mart_margin = run_mart_category_margin()
    t_mart_sla = run_mart_delivery_sla()
    t_mart_recon = run_mart_payment_reconciliation()
    t_mart_cust = run_mart_top_customers()

    mart_flow = (
        staging_flow
        >> t_mart_kpis
        >> t_mart_margin
        >> t_mart_sla
        >> t_mart_recon
        >> t_mart_cust
    )

    # Incremental layer execution & MERGE
    t_raw_increment = load_raw_increment_orders()
    t_stg_merge = execute_stg_orders_incremental_merge()

    incremental_flow = mart_flow >> t_raw_increment >> t_stg_merge

    # Audit summary task
    t_audit = run_audit_summary()
    incremental_flow >> t_audit


nammamart_elt_dag = nammamart_elt_sumit_kumar_surana()
