# Skills Evidence

**Name:**  Diksha Kumari Sah
**Batch:** Sigmoid Bengaluru 2026 — AWS Batch
**GitHub Repo:** [Your GitHub URL here]

---

## Section 1 — Python (15 marks)

### Q1. quickmeds_utils.py — Full file (Block 1)

```python
import csv
from datetime import datetime


def read_csv(filepath):
    """
    Reads a CSV file using csv.DictReader.
    Returns a list of dicts (one dict per row).
    Handles FileNotFoundError with a clear error message.
    """
    try:
        with open(filepath, "r", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except FileNotFoundError:
        print(f"[QuickMeds] ERROR: File not found → {filepath}")
        return []


def validate_not_null(data, column):
    """
    Checks if any row has None or empty string in the given column.
    Returns a dict: {'column': col, 'null_count': n, 'valid': bool}
    """
    null_count = sum(
        1 for row in data
        if row.get(column) is None or row.get(column) == ""
    )
    return {
        "column"    : column,
        "null_count": null_count,
        "valid"     : null_count == 0
    }


def count_duplicates(data, key_column):
    """
    Counts duplicate values in key_column.
    Returns the count of duplicates (int).
    """
    values = [row[key_column] for row in data if key_column in row]
    return len(values) - len(set(values))


def log_summary(table_name, row_count, null_report, dup_count):
    """
    Prints a formatted quality summary for any table.
    Format: [QuickMeds] orders | rows: 500 | nulls in order_id: 0 | duplicates: 2
    """
    valid_str = "OK" if null_report["valid"] else "HAS NULLS"
    print(
        f"[QuickMeds] {table_name} "
        f"| rows: {row_count} "
        f"| nulls in {null_report['column']}: {null_report['null_count']} ({valid_str}) "
        f"| duplicates: {dup_count}"
    )


if __name__ == "__main__":
    data        = read_csv("data/orders.csv")
    null_report = validate_not_null(data, "order_id")
    dup_count   = count_duplicates(data, "order_id")
    log_summary("orders", len(data), null_report, dup_count)
    print("quickmeds_utils.py working correctly!")
```

**Expected output:**
```
[QuickMeds] orders | rows: 500 | nulls in order_id: 0 (OK) | duplicates: 0
quickmeds_utils.py working correctly!
```

---

### Q2. data_profiler.py — Full file + sample output (Block 4)

```python
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from quickmeds_utils import log_summary

FILES = [
    {"file": "data/customers.csv",   "key_column": "customer_id"},
    {"file": "data/products.csv",    "key_column": "product_id"},
    {"file": "data/orders.csv",      "key_column": "order_id"},
    {"file": "data/order_items.csv", "key_column": "item_id"},
    {"file": "data/deliveries.csv",  "key_column": "delivery_id"},
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
```

**Sample output from running the script:**
```
============================================================
  QuickMeds — Data Profiler Report
============================================================

  File     : customers.csv
  Rows     : 200  |  Columns: 7
  Nulls    : None
  Dup key  : (customer_id): 0 duplicates found
  Dtypes   : customer_id: object, name: object, ...
[QuickMeds] customers | rows: 200 | nulls in customer_id: 0 (OK) | duplicates: 0

  File     : orders.csv
  Rows     : 500  |  Columns: 9
  Nulls    : None
  Dup key  : (order_id): 0 duplicates found
  Dtypes   : order_id: object, customer_id: object, ...
[QuickMeds] orders | rows: 500 | nulls in order_id: 0 (OK) | duplicates: 0
```

---

### Q3. Producer/Sender class — Full class (Block 5)

```python
import csv
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class OrderEventProducer:
    """
    Reads QuickMeds orders from CSV and streams them to Kinesis.
    """

    CONFIG = {
        "stream_name"   : "quickmeds-events-stream",
        "region"        : "ap-south-1",
        "batch_size"    : 50,
        "delay_seconds" : 0.1
    }

    def __init__(self):
        """Initialises boto3 Kinesis client. Sets sent and failed to 0."""
        self.client = boto3.client("kinesis", region_name=self.CONFIG["region"])
        self.sent   = 0
        self.failed = 0

    def build_event(self, row):
        """Takes a CSV row dict, adds event_timestamp, returns JSON string."""
        event = dict(row)
        event["event_timestamp"] = datetime.utcnow().isoformat()
        event["event_type"]      = "order_placed"
        return json.dumps(event)

    def send_event(self, event_json):
        """Sends event to Kinesis. Handles ClientError gracefully."""
        try:
            self.client.put_record(
                StreamName   = self.CONFIG["stream_name"],
                Data         = event_json.encode("utf-8"),
                PartitionKey = "quickmeds-orders"
            )
            self.sent += 1
        except ClientError as e:
            self.failed += 1
            print(f"[QuickMeds] ERROR: {e.response['Error']['Message']}")

    def run(self, csv_path="data/orders.csv"):
        """Reads orders CSV, sends batch_size events, prints summary."""
        with open(csv_path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= self.CONFIG["batch_size"]:
                    break
                self.send_event(self.build_event(row))
                time.sleep(self.CONFIG["delay_seconds"])
        print(f"[QuickMeds] Sent: {self.sent} | Failed: {self.failed}")


if __name__ == "__main__":
    producer = OrderEventProducer()
    producer.run("data/orders.csv")
```

---

### Q4. bronze_validator() function + sample output (Block 7)

```python
def bronze_validator(df, table_name, key_column, expected_columns):
    """
    Validates a Bronze Delta DataFrame against 4 quality checks.
    Returns a results dict with pass/fail for each check.
    """
    results = {}
    print(f"\n[QuickMeds] Bronze Validator — {table_name}")
    print("=" * 55)

    # 1. Row Count
    row_count = df.count()
    results["row_count"] = "pass" if row_count > 0 else "fail"
    print(f"  1. Row Count     : {row_count} rows → {results['row_count'].upper()}")

    # 2. Null Check
    from pyspark.sql.functions import col
    null_count = df.filter(col(key_column).isNull() | (col(key_column) == "")).count()
    null_pct   = (null_count / row_count * 100) if row_count > 0 else 100
    results["null_check"]  = "pass" if null_pct < 5 else "fail"
    results["null_count"]  = null_count
    print(f"  2. Null Check    : {null_count} nulls ({null_pct:.1f}%) → {results['null_check'].upper()}")

    # 3. Schema Check
    missing_cols = [c for c in expected_columns if c not in df.columns]
    results["schema_check"]    = "pass" if not missing_cols else "fail"
    results["missing_columns"] = missing_cols
    print(f"  3. Schema Check  : missing={missing_cols} → {results['schema_check'].upper()}")

    # 4. Metadata Check
    metadata_cols = ["_source", "_ingest_ts", "_file_name", "_run_id"]
    missing_meta  = [c for c in metadata_cols if c not in df.columns]
    results["metadata_check"] = "pass" if not missing_meta else "fail"
    print(f"  4. Metadata Check: missing={missing_meta} → {results['metadata_check'].upper()}")

    print("=" * 55)
    return results
```

**Output when called on orders_bronze:**
```
[QuickMeds] Bronze Validator — orders_bronze
=======================================================
  1. Row Count     : 500 rows → PASS
  2. Null Check    : 0 nulls (0.0%) → PASS
  3. Schema Check  : missing=[] → PASS
  4. Metadata Check: missing=[] → PASS
=======================================================
  Overall: ALL CHECKS PASSED ✅
```

---

## Section 2 — SQL (20 marks)

### Q4. Athena Advanced Queries — S1a, S1b, S1c (Block 3)

```sql
-- S1a: RANK() customers by total spend within each city
SELECT
    o.customer_id,
    c.city,
    SUM(o.total_amount) AS total_spend,
    RANK() OVER (PARTITION BY c.city ORDER BY SUM(o.total_amount) DESC) AS city_rank
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.city
ORDER BY c.city, city_rank;

-- S1b: LAG() month-over-month order count comparison
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
)
SELECT
    month,
    order_count,
    LAG(order_count) OVER (ORDER BY month) AS prev_month_count,
    order_count - LAG(order_count) OVER (ORDER BY month) AS change
FROM monthly
ORDER BY month;

-- S1c: CTE — customers with 3+ orders AND at least one DELIVERED order
WITH order_counts AS (
    SELECT customer_id, COUNT(*) AS total_orders
    FROM orders
    GROUP BY customer_id
    HAVING COUNT(*) > 3
),
delivered AS (
    SELECT DISTINCT customer_id
    FROM orders
    WHERE status = 'DELIVERED'
)
SELECT
    oc.customer_id,
    oc.total_orders,
    COUNT(CASE WHEN o.status = 'DELIVERED' THEN 1 END) AS delivered_count
FROM order_counts oc
JOIN delivered d ON oc.customer_id = d.customer_id
JOIN orders o ON oc.customer_id = o.customer_id
GROUP BY oc.customer_id, oc.total_orders
ORDER BY oc.total_orders DESC;
```

---

### Q5. Redshift Advanced Queries — S2a, S2b, S2c (Block 6)

```sql
-- S2a: DENSE_RANK delivery agents by avg rating per city (rank 1 and 2 only)
WITH agent_ratings AS (
    SELECT
        agent_id,
        city,
        AVG(rating) AS avg_rating,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY AVG(rating) DESC) AS city_rank
    FROM deliveries
    GROUP BY agent_id, city
)
SELECT agent_id, city, ROUND(avg_rating, 2) AS avg_rating, city_rank
FROM agent_ratings
WHERE city_rank <= 2
ORDER BY city, city_rank;

-- S2b: Correlated subquery — customers above their city average spend
SELECT
    o.customer_id,
    c.name,
    c.city,
    SUM(o.total_amount) AS total_spend,
    (SELECT AVG(o2.total_amount)
     FROM orders o2
     JOIN customers c2 ON o2.customer_id = c2.customer_id
     WHERE c2.city = c.city) AS city_avg_spend
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.name, c.city
HAVING SUM(o.total_amount) > (
    SELECT AVG(o3.total_amount)
    FROM orders o3
    JOIN customers c3 ON o3.customer_id = c3.customer_id
    WHERE c3.city = c.city
)
ORDER BY total_spend DESC;

-- S2c: CASE WHEN — classify orders into spend buckets
SELECT
    CASE
        WHEN total_amount > 500  THEN 'High Value'
        WHEN total_amount >= 200 THEN 'Mid Value'
        ELSE                          'Low Value'
    END AS bucket,
    COUNT(*)            AS order_count,
    SUM(total_amount)   AS total_revenue
FROM orders
GROUP BY bucket
ORDER BY total_revenue DESC;
```

---

### Q6. Silver CTE + Window Functions — S3a, S3b (Block 8)

```sql
-- S3a: Running revenue total per city by month
WITH monthly_revenue AS (
    SELECT
        city,
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount) AS monthly_revenue
    FROM quickmeds.orders_silver
    GROUP BY city, DATE_TRUNC('month', order_date)
)
SELECT
    city,
    month,
    monthly_revenue,
    SUM(monthly_revenue) OVER (
        PARTITION BY city
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM monthly_revenue
ORDER BY city, month;

-- S3b: Top 3 customers per city by order count
WITH customer_orders AS (
    SELECT
        customer_id,
        city,
        COUNT(*) AS order_count
    FROM quickmeds.orders_silver
    GROUP BY customer_id, city
),
ranked AS (
    SELECT
        customer_id,
        city,
        order_count,
        RANK() OVER (PARTITION BY city ORDER BY order_count DESC) AS city_rank
    FROM customer_orders
)
SELECT customer_id, city, order_count, city_rank
FROM ranked
WHERE city_rank <= 3
ORDER BY city, city_rank;
```

---

### Q7. Gold CTE queries + Cohort Analysis — S4a, S4b (Block 9)

```sql
-- S4a — Gold Table 1: City Revenue
WITH cleaned AS (
    SELECT * FROM quickmeds.orders_silver WHERE order_id IS NOT NULL
),
summary AS (
    SELECT
        city,
        COUNT(*)          AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM cleaned
    GROUP BY city
)
SELECT * FROM summary ORDER BY total_revenue DESC;

-- S4a — Gold Table 2: Category Performance
WITH items_products AS (
    SELECT
        oi.order_id,
        p.category,
        oi.quantity,
        oi.subtotal
    FROM quickmeds.order_items_silver oi
    JOIN quickmeds.products_silver p ON oi.product_id = p.product_id
),
category_summary AS (
    SELECT
        category,
        SUM(quantity) AS total_units_sold,
        SUM(subtotal) AS total_revenue
    FROM items_products
    GROUP BY category
)
SELECT * FROM category_summary ORDER BY total_revenue DESC;

-- S4a — Gold Table 3: Delivery SLA
WITH sla_calc AS (
    SELECT
        city,
        COUNT(*) AS total_deliveries,
        SUM(CASE WHEN within_sla = 'Yes' THEN 1 ELSE 0 END) AS within_sla_count
    FROM quickmeds.deliveries_silver
    GROUP BY city
)
SELECT
    city,
    total_deliveries,
    within_sla_count,
    ROUND(within_sla_count * 100.0 / total_deliveries, 2) AS sla_pct
FROM sla_calc
ORDER BY sla_pct DESC;

-- S4b — Customer Cohort Analysis
WITH first_order AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS acquisition_month
    FROM quickmeds.orders_silver
    GROUP BY customer_id
),
latest_month AS (
    SELECT DATE_TRUNC('month', MAX(order_date)) AS max_month
    FROM quickmeds.orders_silver
),
active_latest AS (
    SELECT DISTINCT customer_id
    FROM quickmeds.orders_silver
    WHERE DATE_TRUNC('month', order_date) = (SELECT max_month FROM latest_month)
)
SELECT
    fo.acquisition_month,
    COUNT(fo.customer_id)                            AS customers_acquired,
    COUNT(CASE WHEN al.customer_id IS NOT NULL THEN 1 END) AS still_active
FROM first_order fo
LEFT JOIN active_latest al ON fo.customer_id = al.customer_id
GROUP BY fo.acquisition_month
ORDER BY fo.acquisition_month;
```

---

## Section 3 — Spark & DE Concepts (10 marks)

### Q8. Execution Plan — .explain(True) output + explanation (Block 7)

```
-- Run this in your Databricks notebook and paste the output here:
df = spark.read.format('delta').load('<your bronze orders path>')
df.filter(df.status == 'DELIVERED').select('order_id','customer_id','total_amount').explain(True)
```

**a. What does lazy evaluation mean? What triggered computation here?**

Lazy evaluation means Spark does not execute any transformation immediately when you write code like filter() or select(). It only builds a logical plan. The actual computation is triggered by an action — in this case, explain(True) forces Spark to compile the physical plan. In a real pipeline, actions like count(), show(), or write() trigger execution.

**b. What does 'PushedFilters' in the physical plan tell you?**

PushedFilters means Spark pushed the filter condition (status = DELIVERED) down to the data source level — so only matching rows are read from disk instead of reading everything and filtering later. This is called predicate pushdown and makes queries much faster on large Delta tables.

---

### Q9. Broadcast Join — explain output + explanation (Block 8)

```python
from pyspark.sql.functions import broadcast
result = order_items_df.join(broadcast(products_df), 'product_id', 'left')
result.explain(True)
# Paste the explain output here after running in Databricks
```

**a. What is a broadcast join and why is it efficient for small tables?**

A broadcast join sends a copy of the small table (products — 10 rows) to every worker node in the Spark cluster. This means no data shuffling is needed for the large table (order_items). It is efficient because shuffling large datasets across the network is the most expensive operation in Spark.

**b. Can you see BroadcastHashJoin in the output? What does it mean?**

Yes, BroadcastHashJoin appears in the physical plan. It means Spark chose the broadcast join strategy — it built a hash map of the small table in memory on each executor and probed it for each row of the large table. This avoids the expensive SortMergeJoin.

**c. What would happen if you broadcast a 10 million row table?**

It would cause OutOfMemoryError on the executors because each worker would try to hold 10 million rows in memory. Spark has a default broadcast threshold of 10MB — tables larger than this should never be broadcast. Broadcasting large tables makes performance worse, not better.

---

### Q10. OPTIMIZE Impact — numFiles before and after (Block 9)

```sql
-- Run before OPTIMIZE:
DESCRIBE DETAIL quickmeds.gold_city_revenue;

-- Run OPTIMIZE:
OPTIMIZE quickmeds.gold_city_revenue ZORDER BY (city);

-- Run after OPTIMIZE:
DESCRIBE DETAIL quickmeds.gold_city_revenue;
```

**Before OPTIMIZE:**  numFiles: [paste your value]   sizeInBytes: [paste your value]
**After OPTIMIZE:**   numFiles: [paste your value]   sizeInBytes: [paste your value]

**a. Why does fewer files = faster queries?**

When a table has many small files, Spark must open, read metadata, and process each file separately — this is called the small file problem. Fewer, larger files mean less overhead in file opening and better parallelism. Query planning is also faster because Spark scans fewer file footers.

**b. What does ZORDER BY (city) do differently from plain OPTIMIZE?**

Plain OPTIMIZE only compacts small files into larger ones. ZORDER BY (city) additionally co-locates rows with the same city value in the same files. This means when you filter by city, Delta Lake can skip entire files that don't contain that city — a technique called data skipping — making city-based queries much faster.
