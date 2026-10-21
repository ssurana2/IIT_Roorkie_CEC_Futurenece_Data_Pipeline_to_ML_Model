"""Data quality validation, quarantining, and scorecard logging utilities for NammaMart pipelines.
"""

import os
from datetime import datetime
import duckdb
import pandas as pd


def get_quarantine_dir(engineer_name: str = "sumit_kumar_surana") -> str:
    """Ensure and return the quarantine directory path."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    quarantine_dir = os.path.join(base_dir, "quarantine", engineer_name)
    os.makedirs(quarantine_dir, exist_ok=True)
    return quarantine_dir


def save_to_quarantine(df_failed: pd.DataFrame, filename_prefix: str, run_id: str, engineer_name: str = "sumit_kumar_surana") -> str:
    """Save failed rows with their failure_reason column to the quarantine directory.
    
    File pattern: include/quarantine/<engineer_name>/<filename_prefix>_failed_<run_id>.csv
    """
    if df_failed.empty:
        return ""
    
    quarantine_dir = get_quarantine_dir(engineer_name)
    # Sanitize run_id for filesystem safety (replace colons and plus signs)
    safe_run_id = str(run_id).replace(":", "_").replace("+", "_")
    output_filename = f"{filename_prefix}_failed_{safe_run_id}.csv"
    output_path = os.path.join(quarantine_dir, output_filename)
    
    df_failed.to_csv(output_path, index=False)
    print(f"Quarantined {len(df_failed)} rows to {output_path}")
    return output_path


def init_dq_table(db_path: str):
    """Initialize the dq_results scorecard table in DuckDB if not present."""
    create_table_sql = """
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
    """
    con = duckdb.connect(db_path)
    try:
        con.execute(create_table_sql)
    finally:
        con.close()


def log_dq_result(
    db_path: str,
    run_id: str,
    dag_id: str,
    check_name: str,
    dimension: str,
    severity: str,
    rows_checked: int,
    rows_failed: int,
    status: str,
):
    """Log an individual data quality check result into the dq_results DuckDB table."""
    init_dq_table(db_path)
    con = duckdb.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO dq_results (
                run_id, dag_id, check_name, dimension, severity,
                rows_checked, rows_failed, status, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                dag_id,
                check_name,
                dimension,
                severity,
                rows_checked,
                rows_failed,
                status,
                datetime.now(),
            ],
        )
    finally:
        con.close()


def log_dq_batch(db_path: str, records: list):
    """Log a batch of data quality check results into dq_results.
    
    records: list of dicts with keys:
      run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status
    """
    if not records:
        return
    init_dq_table(db_path)
    con = duckdb.connect(db_path)
    try:
        now = datetime.now()
        for r in records:
            con.execute(
                """
                INSERT INTO dq_results (
                    run_id, dag_id, check_name, dimension, severity,
                    rows_checked, rows_failed, status, checked_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    r["run_id"],
                    r["dag_id"],
                    r["check_name"],
                    r["dimension"],
                    r["severity"],
                    r["rows_checked"],
                    r["rows_failed"],
                    r["status"],
                    now,
                ],
            )
    finally:
        con.close()
