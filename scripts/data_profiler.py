import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from quickmeds_utils import log_summary

FILES = [
    {"file": "data/QuickMeds_customers.csv",   "key_column": "customer_id"},
    {"file": "data/QuickMeds_products.csv",    "key_column": "product_id"},
    {"file": "data/QuickMeds_orders.csv",      "key_column": "order_id"},
    {"file": "data/QuickMeds_order_items.csv", "key_column": "item_id"},
    {"file": "data/QuickMeds_deliveries.csv",  "key_column": "delivery_id"},
]

print("=" * 60)
print("  QuickMeds — Data Profiler Report")
print("=" * 60)

for entry in FILES:
    filepath   = entry["file"]
    key_column = entry["key_column"]
    filename   = os.path.basename(filepath)

    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"\n[QuickMeds] ERROR: File not found → {filepath}")
        continue

    row_count  = len(df)
    col_count  = len(df.columns)
    null_counts = {col: int(df[col].isnull().sum()) for col in df.columns}
    null_summary = ", ".join(
        [f"{col}: {cnt}" for col, cnt in null_counts.items() if cnt > 0]
    ) or "None"
    dup_count  = int(df[key_column].duplicated().sum())
    dtypes     = ", ".join([f"{col}: {str(dtype)}" for col, dtype in df.dtypes.items()])

    print(f"\n  File     : {filename}")
    print(f"  Rows     : {row_count}  |  Columns: {col_count}")
    print(f"  Nulls    : {null_summary}")
    print(f"  Dup key  : ({key_column}): {dup_count} duplicates found")
    print(f"  Dtypes   : {dtypes}")

    null_report = {
        "column"    : key_column,
        "null_count": null_counts.get(key_column, 0),
        "valid"     : null_counts.get(key_column, 0) == 0
    }
    log_summary(filename.replace(".csv", ""), row_count, null_report, dup_count)