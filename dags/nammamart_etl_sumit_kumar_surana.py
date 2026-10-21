import sys
import os
from io import StringIO
from datetime import datetime, timedelta
import json
import pendulum
import pandas as pd
import duckdb
from airflow.decorators import dag, task

# Include helper modules
sys.path.insert(0, "/usr/local/airflow/include")
from include.data_quality import save_to_quarantine, log_dq_batch
from include.masking import mask_email, mask_phone

DATA_DIR       = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB   = "/usr/local/airflow/include/warehouse_sumit_kumar_surana.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/sumit_kumar_surana"

default_args = {
    "owner": "sumit_kumar_surana",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

@dag(
    dag_id="nammamart_etl_sumit_kumar_surana",
    schedule="0 9 * * 1-6",  # 9:00 AM IST, Monday through Saturday
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "meridian", "session1", "session2"],
)
def nammamart_etl_sumit_kumar_surana():
    
    @task
    def extract_orders():
        df = pd.read_csv(f"{DATA_DIR}/orders.csv")
        print(f"[Extract] {len(df)} orders loaded")
        return df.to_json()
    
    @task
    def extract_payments():
        df = pd.read_csv(f"{DATA_DIR}/payments.csv")
        print(f"[Extract] {len(df)} payments loaded")
        return df.to_json()
    
    @task
    def extract_customers():
        df = pd.read_csv(f"{DATA_DIR}/customers.csv")
        print(f"[Extract] {len(df)} customers loaded")
        return df.to_json()

    @task
    def extract_products():
        df = pd.read_csv(f"{DATA_DIR}/products.csv")
        print(f"[Extract] {len(df)} products loaded")
        return df.to_json()
    
    @task
    def extract_stores():
        df = pd.read_csv(f"{DATA_DIR}/stores.csv")
        print(f"[Extract] {len(df)} stores loaded")
        return df.to_json()

    @task
    def validate_and_quarantine(orders_json, payments_json, customers_json, products_json, stores_json, **context):
        run_id = context.get("run_id", "manual_run")
        dag_id = "nammamart_etl_sumit_kumar_surana"
        print(f"[Validate & Quarantine] Starting validation for run: {run_id}")

        # 1. Rehydrate DataFrames
        orders_df = pd.read_json(StringIO(orders_json))
        payments_df = pd.read_json(StringIO(payments_json))
        customers_df = pd.read_json(StringIO(customers_json))
        products_df = pd.read_json(StringIO(products_json))
        stores_df = pd.read_json(StringIO(stores_json))

        dq_records = []
        failure_reasons = [[] for _ in range(len(orders_df))]

        # --- Rule 1: Completeness - customer_id and order_ts not null ---
        null_cust_mask = orders_df["customer_id"].isna()
        null_ts_mask = orders_df["order_ts"].isna()
        for idx in orders_df[null_cust_mask].index:
            failure_reasons[idx].append("Missing customer_id")
        for idx in orders_df[null_ts_mask].index:
            failure_reasons[idx].append("Missing order_ts")
        
        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_orders_not_null",
            "dimension": "Completeness", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int((null_cust_mask | null_ts_mask).sum()),
            "status": "PASSED" if int((null_cust_mask | null_ts_mask).sum()) == 0 else "FAILED"
        })

        # --- Rule 2: Completeness - delivered orders must have delivery_minutes ---
        is_delivered = orders_df["order_status"].astype(str).str.strip().str.upper() == "DELIVERED"
        null_delivery_mask = is_delivered & orders_df["delivery_minutes"].isna()
        for idx in orders_df[null_delivery_mask].index:
            failure_reasons[idx].append("Missing delivery_minutes for DELIVERED order")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_delivered_has_delivery_time",
            "dimension": "Completeness", "severity": "Critical",
            "rows_checked": int(is_delivered.sum()), "rows_failed": int(null_delivery_mask.sum()),
            "status": "PASSED" if int(null_delivery_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 3: Uniqueness - order_id must be unique ---
        dup_order_mask = orders_df["order_id"].duplicated(keep="first")
        for idx in orders_df[dup_order_mask].index:
            failure_reasons[idx].append("Duplicate order_id")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_unique_order_ids",
            "dimension": "Uniqueness", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(dup_order_mask.sum()),
            "status": "PASSED" if int(dup_order_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 4: Validity - order_status in allowed list ---
        allowed_statuses = ["DELIVERED", "RETURNED", "CANCELLED"]
        clean_status = orders_df["order_status"].astype(str).str.strip().str.upper()
        invalid_status_mask = ~clean_status.isin(allowed_statuses)
        for idx in orders_df[invalid_status_mask].index:
            failure_reasons[idx].append(f"Invalid order_status: {orders_df.loc[idx, 'order_status']}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_valid_order_status",
            "dimension": "Validity", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(invalid_status_mask.sum()),
            "status": "PASSED" if int(invalid_status_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 5: Validity - discount_pct between 0 and 100 ---
        invalid_discount_mask = (orders_df["discount_pct"] < 0) | (orders_df["discount_pct"] > 100)
        for idx in orders_df[invalid_discount_mask].index:
            failure_reasons[idx].append(f"Discount out of range: {orders_df.loc[idx, 'discount_pct']}%")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_discount_range",
            "dimension": "Validity", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(invalid_discount_mask.sum()),
            "status": "PASSED" if int(invalid_discount_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 6: Accuracy - quantity > 0 ---
        invalid_qty_mask = orders_df["quantity"] <= 0
        for idx in orders_df[invalid_qty_mask].index:
            failure_reasons[idx].append(f"Non-positive quantity: {orders_df.loc[idx, 'quantity']}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_positive_quantity",
            "dimension": "Accuracy", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(invalid_qty_mask.sum()),
            "status": "PASSED" if int(invalid_qty_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 7: Accuracy - order_amount formula verification ---
        expected_amount = (orders_df["quantity"] * orders_df["unit_price"] * (1 - orders_df["discount_pct"] / 100)).round(2)
        amount_mismatch_mask = (orders_df["order_amount"] - expected_amount).abs() > 0.02
        for idx in orders_df[amount_mismatch_mask].index:
            failure_reasons[idx].append(f"Amount mismatch: stored {orders_df.loc[idx, 'order_amount']} vs computed {expected_amount[idx]}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_order_amount_accuracy",
            "dimension": "Accuracy", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(amount_mismatch_mask.sum()),
            "status": "PASSED" if int(amount_mismatch_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 8: Accuracy - delivery_minutes < 120 ---
        extreme_delivery_mask = orders_df["delivery_minutes"] >= 120
        for idx in orders_df[extreme_delivery_mask].index:
            failure_reasons[idx].append(f"Delivery time >= 120 mins: {orders_df.loc[idx, 'delivery_minutes']}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_delivery_time_limit",
            "dimension": "Accuracy", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(extreme_delivery_mask.sum()),
            "status": "PASSED" if int(extreme_delivery_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 9: Consistency - Foreign Keys for products and stores ---
        valid_products = set(products_df["product_id"].dropna().unique())
        invalid_prod_mask = ~orders_df["product_id"].isin(valid_products)
        for idx in orders_df[invalid_prod_mask].index:
            failure_reasons[idx].append(f"Invalid product_id: {orders_df.loc[idx, 'product_id']}")

        valid_stores = set(stores_df["store_id"].dropna().unique())
        invalid_store_mask = ~orders_df["store_id"].isin(valid_stores)
        for idx in orders_df[invalid_store_mask].index:
            failure_reasons[idx].append(f"Invalid store_id: {orders_df.loc[idx, 'store_id']}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_orders_product_foreign_key",
            "dimension": "Consistency", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(invalid_prod_mask.sum()),
            "status": "PASSED" if int(invalid_prod_mask.sum()) == 0 else "FAILED"
        })
        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_orders_store_foreign_key",
            "dimension": "Consistency", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(invalid_store_mask.sum()),
            "status": "PASSED" if int(invalid_store_mask.sum()) == 0 else "FAILED"
        })

        # --- Rule 10: Timeliness & Validity - Timestamp parsing, unparseable and future dates ---
        parsed_order_ts = pd.to_datetime(orders_df["order_ts"], format="mixed", errors="coerce")
        future_order_mask = parsed_order_ts.dt.year > 2026
        corrupt_ts_mask = parsed_order_ts.isna() & orders_df["order_ts"].notna()

        for idx in orders_df[future_order_mask].index:
            failure_reasons[idx].append(f"Future order timestamp: {orders_df.loc[idx, 'order_ts']}")
        for idx in orders_df[corrupt_ts_mask].index:
            failure_reasons[idx].append(f"Invalid/unparseable timestamp: {orders_df.loc[idx, 'order_ts']}")

        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_no_future_orders",
            "dimension": "Timeliness", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(future_order_mask.sum()),
            "status": "PASSED" if int(future_order_mask.sum()) == 0 else "FAILED"
        })
        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_valid_timestamp_format",
            "dimension": "Validity", "severity": "Critical",
            "rows_checked": len(orders_df), "rows_failed": int(corrupt_ts_mask.sum()),
            "status": "PASSED" if int(corrupt_ts_mask.sum()) == 0 else "FAILED"
        })

        # --- Quarantine Partitioning ---
        orders_df["failure_reason"] = ["; ".join(reasons) for reasons in failure_reasons]
        failed_orders = orders_df[orders_df["failure_reason"] != ""].copy()
        clean_orders = orders_df[orders_df["failure_reason"] == ""].copy()
        clean_orders.drop(columns=["failure_reason"], inplace=True)

        print(f"[Validate & Quarantine] Orders checked: {len(orders_df)} | Clean: {len(clean_orders)} | Quarantined: {len(failed_orders)}")
        if not failed_orders.empty:
            quarantine_path = save_to_quarantine(failed_orders, "orders", run_id, "sumit_kumar_surana")
            print(f"[Validate & Quarantine] Quarantined orders saved to: {quarantine_path}")

        # --- Dimension Deduplication ---
        cust_dup_count = int(customers_df["customer_id"].duplicated().sum())
        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_unique_customer_id",
            "dimension": "Uniqueness", "severity": "Critical",
            "rows_checked": len(customers_df), "rows_failed": cust_dup_count,
            "status": "PASSED" if cust_dup_count == 0 else "FAILED"
        })
        clean_customers = customers_df.drop_duplicates(subset=["customer_id"], keep="last").copy()

        prod_dup_count = int(products_df["product_id"].duplicated().sum())
        dq_records.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": "check_unique_product_id",
            "dimension": "Uniqueness", "severity": "Critical",
            "rows_checked": len(products_df), "rows_failed": prod_dup_count,
            "status": "PASSED" if prod_dup_count == 0 else "FAILED"
        })
        clean_products = products_df.drop_duplicates(subset=["product_id"], keep="first").copy()

        # Log DQ results to DuckDB scorecard
        log_dq_batch(WAREHOUSE_DB, dq_records)
        print(f"[Validate & Quarantine] Logged {len(dq_records)} check records to {WAREHOUSE_DB}")

        return {
            "orders": clean_orders.to_json(),
            "payments": payments_df.to_json(),
            "customers": clean_customers.to_json(),
            "products": clean_products.to_json(),
            "stores": stores_df.to_json(),
        }

    @task
    def transform_and_enrich(validated_dict, **context):
        print("[Transform & Enrich] Starting data transformation and enrichment...")
        
        # 1. Rehydrate DataFrames
        orders_df = pd.read_json(StringIO(validated_dict["orders"]))
        payments_df = pd.read_json(StringIO(validated_dict["payments"]))
        customers_df = pd.read_json(StringIO(validated_dict["customers"]))
        products_df = pd.read_json(StringIO(validated_dict["products"]))
        stores_df = pd.read_json(StringIO(validated_dict["stores"]))

        # 2. Standardize payment_mode
        mode_map = {
            "upi": "UPI",
            "upi ": "UPI",
            "card": "Card",
            "cash on delivery": "Cash on Delivery",
            "cod": "Cash on Delivery",
            "cashh": "Cash on Delivery",
            "wallet": "Wallet",
        }
        def clean_mode(val):
            if pd.isna(val):
                return "Unknown"
            v = str(val).strip().lower()
            return mode_map.get(v, str(val).strip())

        orders_df["payment_mode"] = orders_df["payment_mode"].apply(clean_mode)
        payments_df["payment_mode"] = payments_df["payment_mode"].apply(clean_mode)

        # 3. Standardize customer loyalty tier
        tier_map = {
            "silver": "Silver",
            "bronze": "Bronze",
            "gold": "Gold",
            "platinum": "Platinum",
        }
        customers_df["loyalty_tier"] = (
            customers_df["loyalty_tier"]
            .astype(str)
            .str.strip()
            .str.lower()
            .map(lambda x: tier_map.get(x, x.capitalize()))
        )

        # 4. Impute missing store zone (S07 BTM Layout -> South)
        stores_df.loc[stores_df["store_id"] == "S07", "zone"] = "South"

        # 5. Mask Customer PII (DPDP Act Compliance)
        customers_df["email"] = customers_df["email"].apply(mask_email)
        customers_df["phone"] = customers_df["phone"].apply(mask_phone)

        # 6. Joins & Derived Columns for orders
        orders_enriched = orders_df.merge(
            products_df[["product_id", "category", "cost_price"]],
            on="product_id",
            how="left",
        )
        orders_enriched = orders_enriched.merge(
            stores_df[["store_id", "store_name", "zone"]],
            on="store_id",
            how="left",
        )

        # Derive order_date safely with mixed format & error coercion
        orders_enriched["order_date"] = pd.to_datetime(
            orders_enriched["order_ts"], format="mixed", errors="coerce"
        ).dt.date.astype(str)
        orders_enriched["order_status"] = orders_enriched["order_status"].astype(str).str.strip().str.upper()

        # Derive net_revenue (only DELIVERED orders count)
        orders_enriched["net_revenue"] = orders_enriched.apply(
            lambda r: round(float(r["order_amount"]), 2) if r["order_status"] == "DELIVERED" else 0.0,
            axis=1,
        )

        # Derive gross_margin (net_revenue - quantity * cost_price)
        orders_enriched["gross_margin"] = orders_enriched.apply(
            lambda r: round(float(r["net_revenue"]) - (float(r["quantity"]) * float(r["cost_price"])), 2)
            if r["order_status"] == "DELIVERED"
            else 0.0,
            axis=1,
        )

        # Derive within_15_min
        orders_enriched["within_15_min"] = (
            (orders_enriched["order_status"] == "DELIVERED") & (orders_enriched["delivery_minutes"] <= 15)
        )

        print(f"[Transform & Enrich] Completed: {len(orders_enriched)} orders enriched, PII masked, derived metrics calculated.")

        return {
            "orders": orders_enriched.to_json(),
            "payments": payments_df.to_json(),
            "customers": customers_df.to_json(),
            "products": products_df.to_json(),
            "stores": stores_df.to_json(),
        }

    @task
    def load_to_duckdb(transformed_dict, **context):
        print(f"[Load] Loading star schema into DuckDB at {WAREHOUSE_DB}...")

        orders_df = pd.read_json(StringIO(transformed_dict["orders"]))
        customers_df = pd.read_json(StringIO(transformed_dict["customers"]))
        products_df = pd.read_json(StringIO(transformed_dict["products"]))
        stores_df = pd.read_json(StringIO(transformed_dict["stores"]))

        os.makedirs(os.path.dirname(WAREHOUSE_DB), exist_ok=True)
        con = duckdb.connect(WAREHOUSE_DB)
        try:
            # Fact table: etl_fact_orders
            con.register("df_fact_orders", orders_df)
            con.execute("CREATE OR REPLACE TABLE etl_fact_orders AS SELECT * FROM df_fact_orders")

            # Dimension tables
            con.register("df_dim_customer", customers_df)
            con.execute("CREATE OR REPLACE TABLE etl_dim_customer AS SELECT * FROM df_dim_customer")

            con.register("df_dim_product", products_df)
            con.execute("CREATE OR REPLACE TABLE etl_dim_product AS SELECT * FROM df_dim_product")

            con.register("df_dim_store", stores_df)
            con.execute("CREATE OR REPLACE TABLE etl_dim_store AS SELECT * FROM df_dim_store")

            fact_count = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
            cust_count = con.execute("SELECT COUNT(*) FROM etl_dim_customer").fetchone()[0]
            prod_count = con.execute("SELECT COUNT(*) FROM etl_dim_product").fetchone()[0]
            store_count = con.execute("SELECT COUNT(*) FROM etl_dim_store").fetchone()[0]

            print(f"[Load] Idempotent load complete:")
            print(f"  - etl_fact_orders: {fact_count} rows")
            print(f"  - etl_dim_customer: {cust_count} rows")
            print(f"  - etl_dim_product: {prod_count} rows")
            print(f"  - etl_dim_store: {store_count} rows")
        finally:
            con.close()

        return {
            "fact_orders_count": fact_count,
            "dim_customer_count": cust_count,
            "dim_product_count": prod_count,
            "dim_store_count": store_count,
        }

    @task
    def reconcile_payments(transformed_dict, **context):
        print(f"[Reconcile] Starting payment reconciliation against orders...")

        orders_df = pd.read_json(StringIO(transformed_dict["orders"]))
        payments_df = pd.read_json(StringIO(transformed_dict["payments"]))

        # Outer join between orders and payments on order_id
        merged = pd.merge(
            orders_df[["order_id", "order_amount", "order_status", "payment_mode"]],
            payments_df[["payment_id", "order_id", "amount_paid", "payment_status", "payment_mode", "gateway_ref"]],
            on="order_id",
            how="outer",
            suffixes=("_order", "_payment"),
        )

        reconciliation_records = []
        now_ts = datetime.now()

        for _, row in merged.iterrows():
            order_id = row.get("order_id")
            payment_id = row.get("payment_id")
            order_status = row.get("order_status")
            payment_status = row.get("payment_status")
            order_amount = row.get("order_amount")
            amount_paid = row.get("amount_paid")

            reasons = []

            # 1. Delivered order with missing payment
            if order_status == "DELIVERED" and pd.isna(payment_id):
                reasons.append("DELIVERED order with no payment record")

            # 2. Cancelled order with captured payment
            elif order_status == "CANCELLED" and payment_status == "SUCCESS":
                reasons.append("Payment captured for CANCELLED order")

            # 3. Delivered order with non-success payment
            elif order_status == "DELIVERED" and payment_status in ["FAILED", "REFUNDED"]:
                reasons.append(f"Payment status is {payment_status} for DELIVERED order")

            # 4. Successful payment with amount discrepancy
            if payment_status == "SUCCESS" and pd.notna(order_amount) and pd.notna(amount_paid):
                if abs(float(order_amount) - float(amount_paid)) > 0.01:
                    reasons.append(f"Amount mismatch: order_amount ₹{order_amount} vs amount_paid ₹{amount_paid}")

            # 5. Orphaned payment (order does not exist)
            if pd.isna(order_status) and pd.notna(payment_id):
                reasons.append("Orphaned payment (order_id does not exist in OMS)")

            if reasons:
                reconciliation_records.append({
                    "order_id": order_id if pd.notna(order_id) else None,
                    "payment_id": payment_id if pd.notna(payment_id) else None,
                    "order_amount": float(order_amount) if pd.notna(order_amount) else None,
                    "amount_paid": float(amount_paid) if pd.notna(amount_paid) else None,
                    "order_status": order_status if pd.notna(order_status) else None,
                    "payment_status": payment_status if pd.notna(payment_status) else None,
                    "mismatch_reason": "; ".join(reasons),
                    "reconciled_at": now_ts,
                })

        reconcile_df = pd.DataFrame(reconciliation_records)
        print(f"[Reconcile] Found {len(reconcile_df)} reconciliation discrepancies")

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            con.register("df_reconcile", reconcile_df)
            con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM df_reconcile")
            recon_count = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
            print(f"[Reconcile] Saved {recon_count} mismatch rows to etl_payment_reconciliation table in DuckDB")
        finally:
            con.close()

        return {"discrepancies_count": len(reconcile_df)}

    @task
    def report_summary(recon_summary, **context):
        print(f"[Report] Generating summary reports and logging KPI tables...")

        con = duckdb.connect(WAREHOUSE_DB)
        try:
            # Table 1: etl_daily_zone_revenue
            con.execute("""
                CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
                SELECT 
                    order_date,
                    zone,
                    store_name,
                    ROUND(SUM(net_revenue), 2) AS net_revenue,
                    COUNT(*) AS delivered_order_count
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY order_date, zone, store_name
                ORDER BY order_date DESC, zone, store_name;
            """)

            # Table 2: etl_category_margin
            con.execute("""
                CREATE OR REPLACE TABLE etl_category_margin AS
                SELECT 
                    COALESCE(category, 'Unknown') AS category,
                    ROUND(SUM(net_revenue), 2) AS total_revenue,
                    ROUND(SUM(gross_margin), 2) AS total_gross_margin,
                    ROUND(100.0 * SUM(gross_margin) / NULLIF(SUM(net_revenue), 0), 2) AS gross_margin_pct
                FROM etl_fact_orders
                WHERE order_status = 'DELIVERED'
                GROUP BY category
                ORDER BY total_revenue DESC;
            """)

            # Fetch samples for task logs
            zone_rev = con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 5").fetchdf()
            cat_margin = con.execute("SELECT * FROM etl_category_margin").fetchdf()

            print("\n==================== [REPORT] DAILY ZONE REVENUE (TOP 5) ====================")
            print(zone_rev.to_string(index=False))

            print("\n==================== [REPORT] CATEGORY GROSS MARGIN ====================")
            print(cat_margin.to_string(index=False))

            # Total September Delivered Revenue
            total_rev = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders WHERE order_status = 'DELIVERED'").fetchone()[0]
            print(f"\n==================== TOTAL SEPTEMBER REVENUE: ₹{total_rev:,.2f} ====================\n")

        finally:
            con.close()

        return {"status": "SUCCESS"}

    # Pipeline Flow
    orders_raw = extract_orders()
    payments_raw = extract_payments()
    customers_raw = extract_customers()
    products_raw = extract_products()
    stores_raw = extract_stores()

    validated_data = validate_and_quarantine(
        orders_raw, payments_raw, customers_raw, products_raw, stores_raw
    )

    transformed_data = transform_and_enrich(validated_data)
    loaded_summary = load_to_duckdb(transformed_data)
    recon_summary = reconcile_payments(transformed_data)

    # Explicit bitshift dependency: load finishes before reconciliation runs
    loaded_summary >> recon_summary

    report_summary(recon_summary)

nammamart_etl_dag = nammamart_etl_sumit_kumar_surana()
