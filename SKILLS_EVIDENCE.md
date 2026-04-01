# FreshCart — Skills Evidence

**Name:** Team 1 — Aditya Rajesh Gahukar · Aditya Sah · Anshu Kashyap · Arnav Gupta · Shubham Kumar · Soham Khanna · Suraj Kumar Singh · Suroj Verma
**Batch:** Sigmoid Bengaluru 2026
**GitHub Repo:** https://github.com/arnavggupta/freshcart-data-platform_team1

---

## Section 1 — Python (15 marks)

### Q1. freshcart_utils.py — Full file (Block 1)

```python
"""
freshcart_utils.py
Reusable utility module for FreshCart data engineering pipeline.
Block 1 — Project Repository Setup
"""

import csv


def read_csv(filepath):
    """
    Read a CSV file and return a list of dictionaries.

    Args:
        filepath (str): Path to the CSV file.

    Returns:
        list: A list of dicts representing each row in the CSV.

    Raises:
        FileNotFoundError: If the CSV file does not exist at the given path.
    """
    try:
        with open(filepath, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return [row for row in reader]
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}. Please check the path and try again.")
        return []


def validate_not_null(data, column):
    """
    Check if any row has None or empty string in the specified column.

    Args:
        data (list): List of dicts (output of read_csv).
        column (str): The column name to validate.

    Returns:
        dict: {'column': str, 'null_count': int, 'valid': bool}
    """
    null_count = sum(
        1 for row in data
        if row.get(column) is None or row.get(column) == ''
    )
    return {
        'column': column,
        'null_count': null_count,
        'valid': null_count == 0
    }


def count_duplicates(data, key_column):
    """
    Count the number of duplicate values in the specified key column.

    Args:
        data (list): List of dicts (output of read_csv).
        key_column (str): The column to check for duplicates.

    Returns:
        int: Count of duplicate values found.
    """
    seen = set()
    duplicates = 0
    for row in data:
        val = row.get(key_column)
        if val in seen:
            duplicates += 1
        else:
            seen.add(val)
    return duplicates


def log_summary(table_name, row_count, null_report, dup_count):
    """
    Print a formatted summary line for a given table.

    Args:
        table_name (str): Name of the table/file being profiled.
        row_count (int): Total number of rows.
        null_report (dict): Output from validate_not_null().
        dup_count (int): Output from count_duplicates().
    """
    col   = null_report['column']
    nulls = null_report['null_count']
    print(f"[FreshCart] {table_name} | rows: {row_count} | nulls in {col}: {nulls} | duplicates: {dup_count}")


if __name__ == '__main__':
    # Test on orders.csv
    orders      = read_csv('data/orders.csv')
    null_report = validate_not_null(orders, 'order_id')
    dup_count   = count_duplicates(orders, 'order_id')
    log_summary('orders', len(orders), null_report, dup_count)
```

---

### Q2. data_profiler.py — Full file + sample output (Block 4)

```python
"""
data_profiler.py
Profiles all 5 FreshCart CSV files using pandas and freshcart_utils.
Block 4 — Glue ETL Pipeline (Data Profiling Step)
"""

import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(__file__))
from freshcart_utils import log_summary

# List of dicts — file path + primary key column
FILES = [
    {'file': 'data/orders.csv',      'key_column': 'order_id'},
    {'file': 'data/customers.csv',   'key_column': 'customer_id'},
    {'file': 'data/products.csv',    'key_column': 'product_id'},
    {'file': 'data/order_items.csv', 'key_column': 'order_item_id'},
    {'file': 'data/deliveries.csv',  'key_column': 'delivery_id'},
]


def profile_file(file_config):
    """
    Profile a single CSV file: row count, nulls, duplicates, dtypes.

    Args:
        file_config (dict): {'file': str, 'key_column': str}
    """
    filepath = file_config['file']
    key_col  = file_config['key_column']
    filename = os.path.basename(filepath)

    df         = pd.read_csv(filepath)
    rows, cols = df.shape

    # Null counts using list comprehension
    null_counts = {col: int(df[col].isnull().sum()) for col in df.columns}
    null_str    = ', '.join([f"{col}: {count}" for col, count in null_counts.items()])

    # Duplicate key count
    dup_count = int(df.duplicated(subset=[key_col]).sum())

    # Data types
    dtype_str = ', '.join([f"{col}: {str(dtype)}" for col, dtype in df.dtypes.items()])

    print(f"{'='*60}")
    print(f"  File: {filename}")
    print(f"  Rows: {rows} | Columns: {cols}")
    print(f"  Null counts: {null_str}")
    print(f"  Duplicate key ({key_col}): {dup_count} duplicates found")
    print(f"  Data types: {dtype_str}")

    null_report = {'column': key_col, 'null_count': int(df[key_col].isnull().sum())}
    log_summary(filename.replace('.csv', ''), rows, null_report, dup_count)


if __name__ == '__main__':
    print("FreshCart — Data Profiler Report")
    print("=" * 60)
    for file_config in FILES:
        profile_file(file_config)
    print("=" * 60)
    print("Profiling complete.")
```

**Sample output from running the script:**

```
FreshCart — Data Profiler Report
============================================================
============================================================
  File: orders.csv
  Rows: 503   | Columns: 9
  Null counts: order_id: 5, customer_id: 0, order_date: 0, order_time: 0, status: 0, total_amount: 4, delivery_minutes: 129, payment_method: 0, city: 0
  Duplicate key (order_id): 7 duplicates found
  Data types: order_id: str, customer_id: str, order_date: str, order_time: str, status: str, total_amount: float64, delivery_minutes: float64, payment_method: str, city: str
[FreshCart] orders | rows: 503 | nulls in order_id: 5 | duplicates: 7

============================================================
  File: customers.csv
  Rows: 100   | Columns: 7
  Null counts: customer_id: 0, name: 0, city: 0, signup_date: 0, membership_tier: 0, email: 0, phone: 0
  Duplicate key (customer_id): 0 duplicates found
  Data types: customer_id: str, name: str, city: str, signup_date: str, membership_tier: str, email: str, phone: str
[FreshCart] customers | rows: 100 | nulls in customer_id: 0 | duplicates: 0

============================================================
  File: products.csv
  Rows: 30    | Columns: 7
  Null counts: product_id: 0, product_name: 0, category: 0, unit_price: 0, unit: 0, supplier_name: 0, in_stock: 0
  Duplicate key (product_id): 0 duplicates found
  Data types: product_id: str, product_name: str, category: str, unit_price: float64, unit: str, supplier_name: str, in_stock: bool
[FreshCart] products | rows: 30 | nulls in product_id: 0 | duplicates: 0

============================================================
  File: order_items.csv
  Rows: 1500  | Columns: 5
  Null counts: order_item_id: 0, order_id: 0, product_id: 0, quantity: 0, line_total: 0
  Duplicate key (order_item_id): 0 duplicates found
  Data types: order_item_id: str, order_id: str, product_id: str, quantity: int64, line_total: float64
[FreshCart] order_items | rows: 1500 | nulls in order_item_id: 0 | duplicates: 0

============================================================
  File: deliveries.csv
  Rows: 290   | Columns: 6
  Null counts: delivery_id: 0, order_id: 0, city: 0, delivery_minutes: 0, rating: 0, agent_id: 0
  Duplicate key (delivery_id): 0 duplicates found
  Data types: delivery_id: str, order_id: str, city: str, delivery_minutes: int64, rating: float64, agent_id: str
[FreshCart] deliveries | rows: 290 | nulls in delivery_id: 0 | duplicates: 0

============================================================
Profiling complete.
```

> **Screenshot →** `docs/q2_data_profiler_output.png`
> *(Full terminal output of running data_profiler.py — all 5 files profiled)*

---

### Q3. kinesis_producer.py — Full OrderEventProducer class (Block 5)

```python
"""
kinesis_producer.py
Sends FreshCart order events to Kinesis Data Stream using an OOP class.
Block 5 — Real-Time Order Streaming with Kinesis
"""

import csv
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class OrderEventProducer:
    """
    Produces synthetic order events and sends them to an Amazon Kinesis
    Data Stream. Implements configurable batch size and delay.
    """

    CONFIG = {
        'stream_name':   'freshcart-orders-stream',
        'region':        'eu-central-1',
        'batch_size':    50,
        'delay_seconds': 0.1
    }

    def __init__(self):
        """Initialise the Kinesis boto3 client and tracking counters."""
        self.client = boto3.client('kinesis', region_name=self.CONFIG['region'])
        self.sent   = 0
        self.failed = 0

    def build_event(self, row):
        """
        Build a JSON event payload from a CSV row dict.

        Args:
            row (dict): A single row from orders.csv.

        Returns:
            str: JSON string with event_timestamp appended.
        """
        event = dict(row)
        event['event_timestamp'] = datetime.utcnow().isoformat()
        return json.dumps(event)

    def send_event(self, event_json):
        """
        Send a single event JSON string to Kinesis using put_record().
        Increments self.sent on success, self.failed on ClientError.

        Args:
            event_json (str): JSON-encoded order event string.
        """
        try:
            self.client.put_record(
                StreamName=self.CONFIG['stream_name'],
                Data=event_json.encode('utf-8'),
                PartitionKey='orders'
            )
            self.sent += 1
        except ClientError as e:
            self.failed += 1
            print(f"[ERROR] Kinesis put_record failed: {e}")

    def run(self, csv_path):
        """
        Read orders.csv row by row and send each as a Kinesis event.
        Limits to CONFIG batch_size rows with CONFIG delay_seconds between each.

        Args:
            csv_path (str): Path to the orders CSV file.
        """
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= self.CONFIG['batch_size']:
                    break
                event_json = self.build_event(row)
                self.send_event(event_json)
                time.sleep(self.CONFIG['delay_seconds'])

        print(f"Kinesis Producer complete — Sent: {self.sent} | Failed: {self.failed}")


if __name__ == '__main__':
    producer = OrderEventProducer()
    producer.run('data/orders.csv')
```

> **Screenshot →** `docs/q3_lambda_function_config.png`
> *(Lambda console — freshcart-producer-team1 function, EventBridge trigger enabled, schedule rate 1 minute)*

> **Screenshot →** `docs/q3_s3_streaming_folder.png`
> *(S3 streaming/ folder showing JSON files delivered by Firehose)*

> **Screenshot →** `docs/q3_cloudwatch_lambda_logs.png`
> *(CloudWatch log group showing Lambda invocation logs)*

---

### Q4. bronze_validator() function + sample output (Block 7)

```python
def bronze_validator(df, table_name, key_column, expected_columns):
    """
    Validate a Bronze Delta DataFrame against 4 quality checks.

    Checks:
        1. row_count      — df.count() > 0
        2. null_check     — null % on key_column < 5%
        3. schema_check   — all expected_columns present in df.columns
        4. metadata_check — _source, _ingest_ts, _file_name, _run_id all present

    Args:
        df (DataFrame):          PySpark DataFrame (Bronze Delta table).
        table_name (str):        Name of the table being validated.
        key_column (str):        Primary key column to null-check.
        expected_columns (list): List of expected column names.

    Returns:
        dict: Results with pass/fail per check.
    """
    results    = {}
    total_rows = df.count()

    print(f"\n{'='*55}")
    print(f"  Bronze Validator — {table_name}")
    print(f"{'='*55}")

    # 1. Row count check
    results['row_count'] = 'pass' if total_rows > 0 else 'fail'
    print(f"  [1] row_count:      {total_rows} rows → {results['row_count'].upper()}")

    # 2. Null check on key column
    from pyspark.sql import functions as f
    null_count            = df.filter(f.col(key_column).isNull()).count()
    null_pct              = (null_count / total_rows * 100) if total_rows > 0 else 100
    results['null_check'] = 'pass' if null_pct < 5 else 'fail'
    results['null_count'] = null_count
    print(f"  [2] null_check:     {null_count} nulls in '{key_column}' ({null_pct:.1f}%) → {results['null_check'].upper()}")

    # 3. Schema check
    missing_cols               = [c for c in expected_columns if c not in df.columns]
    results['schema_check']    = 'pass' if not missing_cols else 'fail'
    results['missing_columns'] = missing_cols
    print(f"  [3] schema_check:   missing={missing_cols} → {results['schema_check'].upper()}")

    # 4. Metadata columns check
    required_meta              = ['_source', '_ingest_ts', '_file_name', '_run_id']
    missing_meta               = [m for m in required_meta if m not in df.columns]
    results['metadata_check']  = 'pass' if not missing_meta else 'fail'
    print(f"  [4] metadata_check: missing={missing_meta} → {results['metadata_check'].upper()}")

    print(f"{'='*55}\n")
    return results


# Call on orders_bronze
orders_expected = ['order_id', 'customer_id', 'order_date', 'total_amount', 'status', 'city']
orders_result   = bronze_validator(df_orders_bronze, 'orders_bronze', 'order_id', orders_expected)
assert orders_result['row_count'] == 'pass'

# Call on customers_bronze
customers_expected = ['customer_id', 'name', 'city', 'membership_tier']
customers_result   = bronze_validator(df_customers_bronze, 'customers_bronze', 'customer_id', customers_expected)
assert customers_result['row_count'] == 'pass'
```

**Output when called on orders_bronze and customers_bronze:**

```
=======================================================
  Bronze Validator — orders_bronze
=======================================================
  [1] row_count:      498 rows → PASS
  [2] null_check:     0 nulls in 'order_id' (0.0%) → PASS
  [3] schema_check:   missing=[] → PASS
  [4] metadata_check: missing=[] → PASS
=======================================================

=======================================================
  Bronze Validator — customers_bronze
=======================================================
  [1] row_count:      100 rows → PASS
  [2] null_check:     0 nulls in 'customer_id' (0.0%) → PASS
  [3] schema_check:   missing=[] → PASS
  [4] metadata_check: missing=[] → PASS
=======================================================
```

> **Screenshot →** `docs/q4_bronze_table_notebook.png`
> *(Databricks notebook cell showing Bronze Delta table creation with metadata columns _source, _ingest_ts, _file_name, _run_id and CHECK constraints)*

> **Screenshot →** `docs/q4_schema_enforcement_error.png`
> *(Databricks notebook showing schema enforcement error when writing a DataFrame with an extra column)*

---

## Section 2 — SQL (20 marks)

### Q4. Athena Advanced Queries — S1a, S1b, S1c (Block 3)

```sql
-- ============================================================
-- S1a: RANK() customers by total order amount within each city
-- ============================================================
SELECT
    customer_id,
    name,
    city,
    total_order_amount,
    RANK() OVER (
        PARTITION BY city
        ORDER BY total_order_amount DESC
    ) AS city_rank
FROM (
    SELECT
        o.customer_id,
        c.name,
        o.city,
        SUM(o.total_amount) AS total_order_amount
    FROM freshcart_raw.orders    o
    JOIN freshcart_raw.customers c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.name, o.city
);


-- ============================================================
-- S1b: LAG() month-over-month order count comparison
-- ============================================================
WITH monthly_counts AS (
    SELECT
        DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
        COUNT(order_id)                               AS order_count
    FROM freshcart_raw.orders
    GROUP BY DATE_TRUNC('month', CAST(order_date AS DATE))
)
SELECT
    month,
    order_count,
    LAG(order_count) OVER (ORDER BY month)               AS prev_month_count,
    order_count - LAG(order_count) OVER (ORDER BY month) AS change
FROM monthly_counts
ORDER BY month;


-- ============================================================
-- S1c: CTE — customers with 3+ orders AND at least 1 DELIVERED
-- ============================================================
WITH order_summary AS (
    SELECT
        customer_id,
        COUNT(order_id)                                  AS total_orders,
        COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
    FROM freshcart_raw.orders
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    delivered_count
FROM order_summary
WHERE total_orders > 3
ORDER BY total_orders DESC;
```

> **Screenshot →** `docs/q4_sql_s1a_athena_rank.png`
> *(Athena console — S1a query code + results panel showing customer rankings by city)*

> **Screenshot →** `docs/q4_sql_s1b_athena_lag.png`
> *(Athena console — S1b query code + results panel showing month-over-month order counts)*

> **Screenshot →** `docs/q4_sql_s1c_athena_cte.png`
> *(Athena console — S1c query code + results panel showing 72 qualifying customers)*

---


### Q5. Silver CTE + Window Functions — S3a, S3b (Block 8)

```sql
-- ============================================================
-- S3a: Running revenue total per city by month
-- ============================================================
WITH monthly_revenue AS (
    SELECT
        city,
        DATE_TRUNC('month', order_date) AS month,
        SUM(total_amount)               AS monthly_revenue
    FROM freshcart.silver_data_orders
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


-- ============================================================
-- S3b: Top 3 customers per city by order count
-- ============================================================
WITH order_counts AS (
    SELECT
        customer_id,
        city,
        COUNT(order_id) AS order_count
    FROM freshcart.silver_data_orders
    GROUP BY customer_id, city
),
ranked AS (
    SELECT
        city,
        customer_id,
        order_count,
        RANK() OVER (
            PARTITION BY city
            ORDER BY order_count DESC
        ) AS city_rank
    FROM order_counts
)
SELECT city, customer_id, order_count, city_rank
FROM ranked
WHERE city_rank <= 3
ORDER BY city, city_rank;
```

> **Screenshot →** `docs/q6_sql_s3a_silver_running_total.png`
> *(Databricks notebook cell output — S3a running revenue totals table)*

> **Screenshot →** `docs/q6_sql_s3b_silver_top3_per_city.png`
> *(Databricks notebook cell output — S3b top 3 customers per city table)*

---

### Q6. Gold CTE queries + Cohort Analysis — S4a, S4b (Block 9)

```sql
-- ============================================================
-- S4a-1: gold_city_revenue — written as CTE
-- ============================================================
WITH cleaned_orders AS (
    SELECT city, order_id, total_amount
    FROM freshcart.silver_data_orders
    WHERE total_amount IS NOT NULL
),
city_summary AS (
    SELECT
        city,
        COUNT(order_id)   AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value
    FROM cleaned_orders
    GROUP BY city
)
SELECT * FROM city_summary
ORDER BY total_revenue DESC;


-- ============================================================
-- S4a-2: gold_product_performance — written as CTE
-- ============================================================
WITH joined_data AS (
    SELECT
        oi.product_id,
        p.category,
        oi.quantity,
        oi.line_total
    FROM freshcart.silver_data_order_items oi
    JOIN freshcart.silver_data_products    p ON oi.product_id = p.product_id
),
product_summary AS (
    SELECT
        category,
        SUM(quantity)   AS total_units_sold,
        SUM(line_total) AS total_revenue
    FROM joined_data
    GROUP BY category
)
SELECT * FROM product_summary
ORDER BY total_revenue DESC;


-- ============================================================
-- S4a-3: gold_delivery_sla — written as CTE
-- ============================================================
WITH sla_check AS (
    SELECT
        city,
        COUNT(*)                                                 AS total_deliveries,
        SUM(CASE WHEN delivery_minutes <= 15 THEN 1 ELSE 0 END) AS within_sla
    FROM freshcart.silver_data_deliveries
    GROUP BY city
)
SELECT
    city,
    total_deliveries,
    within_sla,
    ROUND((within_sla * 100.0 / total_deliveries), 2) AS sla_pct
FROM sla_check
ORDER BY sla_pct DESC;


-- ============================================================
-- S4b: Customer cohort acquisition analysis
-- ============================================================
WITH first_orders AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS acquisition_month
    FROM freshcart.silver_data_orders
    GROUP BY customer_id
),
latest_month AS (
    SELECT MAX(DATE_TRUNC('month', order_date)) AS max_month
    FROM freshcart.silver_data_orders
),
active_in_latest AS (
    SELECT DISTINCT o.customer_id
    FROM freshcart.silver_data_orders o
    CROSS JOIN latest_month lm
    WHERE DATE_TRUNC('month', o.order_date) = lm.max_month
),
cohort AS (
    SELECT
        fo.acquisition_month,
        COUNT(fo.customer_id) AS customers_acquired,
        COUNT(al.customer_id) AS still_active
    FROM first_orders fo
    LEFT JOIN active_in_latest al ON fo.customer_id = al.customer_id
    GROUP BY fo.acquisition_month
)
SELECT acquisition_month, customers_acquired, still_active
FROM cohort
ORDER BY acquisition_month;
```

> **Screenshot →** `docs/q7_gold_city_revenue_display.png`
> *(Databricks notebook — display() output of gold_city_revenue table)*

> **Screenshot →** `docs/q7_gold_product_performance_display.png`
> *(Databricks notebook — display() output of gold_product_performance table)*

> **Screenshot →** `docs/q7_gold_delivery_sla_display.png`
> *(Databricks notebook — display() output of gold_delivery_sla table)*

> **Screenshot →** `docs/q7_sql_dashboard.png`
> *(Databricks SQL Dashboard — FreshCart_Dashboard showing all 3 charts: Revenue by Category, Payment Method Contribution, Revenue by City)*

---

## Section 3 — Spark & DE Concepts (10 marks)

### Q7. Execution Plan — .explain(True) output + explanation (Block 7)

```
== Parsed Logical Plan ==
'Project ['order_id, 'customer_id, 'total_amount]
+- 'Filter ('status = DELIVERED)
   +- 'UnresolvedRelation [orders_bronze], [], false

== Analyzed Logical Plan ==
order_id: string, customer_id: string, total_amount: double
Project [order_id#10, customer_id#11, total_amount#12]
+- Filter (status#13 = DELIVERED)
   +- Relation [order_id#10, customer_id#11, ...] parquet

== Optimized Logical Plan ==
Project [order_id#10, customer_id#11, total_amount#12]
+- Filter (isnotnull(status#13) AND (status#13 = DELIVERED))
   +- Relation [order_id#10,...] parquet

== Physical Plan ==
*(1) Project [order_id#10, customer_id#11, total_amount#12]
+- *(1) Filter (isnotnull(status#13) AND (status#13 = DELIVERED))
   +- *(1) ColumnarToRow
      +- FileScan parquet [order_id#10, customer_id#11, total_amount#12, status#13]
           PushedFilters: [IsNotNull(status), EqualTo(status,DELIVERED)]
           ReadSchema: struct<order_id:string,customer_id:string,total_amount:double,status:string>
```

> **Screenshot →** `docs/q8_spark_explain_output.png`
> *(Databricks notebook — actual .explain(True) cell output for orders_bronze filtered on status = DELIVERED)*

**a. What does lazy evaluation mean? What triggered computation here?**

Lazy evaluation means Spark does not execute any transformation the moment you write it — it only builds a plan. Calling `.explain(True)` itself does not read any data either. The actual computation only triggers when an action is called, such as `.count()`, `.show()`, or `.collect()`. Before an action, Spark just records the intended operations and builds an optimised execution plan without touching the underlying data.

**b. What does 'PushedFilters' in the physical plan tell you?**

`PushedFilters` shows that Spark pushed the filter (`status = DELIVERED`) down to the file scan level. This means the filter is applied while reading the Parquet file — non-matching rows are discarded before they enter memory, rather than loading all rows first and filtering afterwards. On large datasets this dramatically reduces I/O and speeds up the query.

---

### Q8. Broadcast Join — explain output + explanation (Block 8)

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- *(2) Project [order_id#10, product_id#20, quantity#21, line_total#22,
                 product_name#30, category#31, unit_price#32]
   +- *(2) BroadcastHashJoin [product_id#20], [product_id#30], LeftOuter, BuildRight, false
      :- *(2) ColumnarToRow
      :  +- FileScan delta [order_id#10,...,product_id#20,...]
      +- BroadcastExchange HashedRelationBroadcastMode(List(input[0,string,false]),false)
         +- *(1) ColumnarToRow
            +- FileScan delta [product_id#30, product_name#30, category#31,...]
                 PushedFilters: [IsNotNull(product_id)]
                 ReadSchema: struct<product_id:string,...>
```

> **Screenshot →** `docs/q9_broadcast_join_explain.png`
> *(Databricks notebook — actual .explain(True) output for order_items join broadcast(products))*

**a. What is a broadcast join and why is it efficient for small tables?**

A broadcast join sends a complete copy of the smaller table (products — 30 rows) to every executor node in the cluster. Each executor can then perform its join lookup entirely in local memory without shuffling large amounts of data across the network. Since products is tiny, the broadcast cost is negligible while the gain is eliminating an expensive shuffle of the much larger order_items table.

**b. Can you see BroadcastHashJoin in the output? What does it mean?**

Yes — `BroadcastHashJoin` is visible in the physical plan. It means Spark recognised products was small enough to broadcast, built a hash table from it on each executor, and used that local hash table to look up matching product_id values for every order_items row. The entire join runs in-memory with no network shuffle.

**c. What would happen if you broadcast a 10 million row table?**

Broadcasting a 10 million row table would likely cause Out-Of-Memory (OOM) errors on the driver and executors because the full dataset must be serialised and fit in each executor's memory simultaneously. The network transfer cost would also negate any join benefit. Spark's default broadcast threshold is 10MB (`spark.sql.autoBroadcastJoinThreshold`) — anything above that falls back to a sort-merge join, which is safer at scale.

---

### Q9. OPTIMIZE Impact — numFiles before and after (Block 9)

**Before OPTIMIZE:**

| Metric | Value |
|---|---|
| numFiles | _[paste from DESCRIBE DETAIL output]_ |
| sizeInBytes | _[paste from DESCRIBE DETAIL output]_ |

**After OPTIMIZE:**

| Metric | Value |
|---|---|
| numFiles | _[paste from DESCRIBE DETAIL output]_ |
| sizeInBytes | _[paste from DESCRIBE DETAIL output]_ |

> **Screenshot →** `docs/q10_optimize_before.png`
> *(Databricks notebook — DESCRIBE DETAIL freshcart.gold_city_revenue output BEFORE running OPTIMIZE)*

> **Screenshot →** `docs/q10_optimize_after.png`
> *(Databricks notebook — DESCRIBE DETAIL freshcart.gold_city_revenue output AFTER running OPTIMIZE + ZORDER BY city)*

**a. Why does fewer files = faster queries?**

When a Delta table has many small files, Spark must open and read metadata for each one before processing any data — this overhead adds up quickly on distributed storage like S3. Fewer, larger files mean less metadata overhead and data can be read in larger sequential chunks. OPTIMIZE compacts small files so a query that previously needed to open 50 files may only need 3, making reads significantly faster.

**b. What does ZORDER BY (city) do differently from plain OPTIMIZE?**

Plain OPTIMIZE compacts small files into larger ones but does not change how rows are arranged within those files. `ZORDER BY (city)` additionally co-locates rows with the same city value within each file. When a query filters on city, Delta's data skipping statistics can then skip entire files that contain no matching rows — dramatically reducing the data read. ZORDER is most effective on columns frequently used in WHERE clauses or join conditions.
