# Data Profiling & Exploratory Analysis — NammaMart Quick Commerce

**IIT Roorkee CEC — Data Pipeline to ML Model**  
**Author:** Sumit Kumar Surana  
**Environment:** Python 3.13.9

---

## Overview

This directory contains the initial discovery, data sanity checks, and data profiling work for the NammaMart Quick Commerce dataset. Before developing the production ETL (`dags/nammamart_etl_sumit_kumar_surana.py`) and ELT (`dags/nammamart_elt_sumit_kumar_surana.py`) Airflow pipelines, exploratory profiling was conducted in `main.ipynb` to inspect all 6 raw source files, detect data quality defects, check referential integrity, and formulate quarantine rules.

---

## Source Datasets Profiled

The notebook loads and inspects the following raw CSV exports located in `../include/data/nammamart/`:

| Dataset | Row Count | Core Focus & Checks |
| :--- | :--- | :--- |
| `orders.csv` | 2,412 | Null IDs, negative quantities, formula recalculation, status/payment mode validity, delivery time KPIs |
| `payments.csv` | 2,228 | Duplicate payment IDs, zero/negative amounts, missing gateway references, timestamp boundaries |
| `customers.csv` | 405 | Duplicate customer keys, null/invalid emails, non-10-digit phone numbers, loyalty tier casing, PII detection |
| `products.csv` | 152 | Duplicate product IDs, null categories, negative MRP, `cost_price > mrp` margin inversions |
| `stores.csv` | 15 | Store ID uniqueness, zone nulls, zero order capacity anomalies |
| `orders_2026-10-01_increment.csv` | 160 | Incremental feed analysis (10 September return status updates + 150 new October orders) |

---

## Python Version & Environment Setup

This exploratory notebook was developed and tested using **Python 3.13.9**.

### 1. Create a Virtual Environment

It is recommended to use a dedicated virtual environment with Python 3.13.9:

```bash
# Verify Python version (requires Python 3.13.9)
python3.13 --version

# Create virtual environment
python3.13 -m venv .venv

# Activate virtual environment
# On macOS / Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate
```

### 2. Install Required Dependencies

Install `pandas` and Jupyter notebook support:

```bash
pip install --upgrade pip
pip install pandas jupyter ipykernel
```

Register the kernel (optional but recommended):

```bash
python -m ipykernel install --user --name nammamart-py313 --display-name "Python 3.13.9 (NammaMart)"
```

---

## How to Run `main.ipynb`

### Option A: Using VS Code (Recommended)
1. Open the repository root in Visual Studio Code:
   ```bash
   code /Users/sumitsurana/studies/iitr/assignment/IIT_Roorkie_CEC_Futurenece_Data_Pipeline_to_ML_Model
   ```
2. Open `data-profiling/main.ipynb`.
3. Select the Python kernel matching **Python 3.13.9** (or the `.venv` created above) from the top-right kernel picker.
4. Click **Run All** to execute all profiling cells sequentially.

### Option B: Using Jupyter Notebook / JupyterLab
1. Navigate to the `data-profiling` directory from your terminal:
   ```bash
   cd data-profiling
   ```
2. Launch Jupyter Notebook or JupyterLab:
   ```bash
   jupyter notebook main.ipynb
   # OR
   jupyter lab
   ```
3. Run all cells via **Cell > Run All** (or **Run > Run All Cells** in JupyterLab).

> **Important Working Directory Note:**  
> The notebook loads raw data using relative paths (`../include/data/nammamart/*.csv`). Ensure that the working directory of the kernel is set to the `data-profiling` folder so that all relative file paths resolve accurately.

---

## Summary of Key Profiling Discoveries

The profiling executed in `main.ipynb` revealed critical data quality findings that directly shaped our pipeline validation logic:

1. **Orders Data Quality:**
   * 18 null `customer_id` records.
   * 10 zero or negative `quantity` values.
   * 18 arithmetic discrepancies where `order_amount != quantity * unit_price * (1 - discount_pct / 100)`.
   * 3 orders with `discount_pct > 100%`.
   * 2 future-dated order timestamps (e.g., year 2027) and 2 null timestamps.
   * 8 non-cancelled orders missing `delivery_minutes`, and 6 unrealistic delivery outliers (> 60 minutes).
2. **Referential Integrity Mismatches:**
   * 9 orders referencing non-existent product IDs.
   * 5 orders referencing non-existent store IDs.
   * 8 payments referencing non-existent order IDs.
   * 27 delivered orders with missing payment gateway records (totaling ₹23,511.40 in reconciliation discrepancy).
3. **Customer PII & Data Hygiene:**
   * 4 null emails, 12 malformed email formats, and 7 non-10-digit phone numbers.
   * Unmasked personal identifiers requiring masking under the DPDP Act 2023.
   * Inconsistent casing in loyalty tiers (`Silver`, `SILVER`, `Gold`, `bronze`, `Platinum`).

All identified issues are codified in the **Data Issues Log** in `SUBMISSION.md` and enforced in both ETL and ELT data quality validation stages.
