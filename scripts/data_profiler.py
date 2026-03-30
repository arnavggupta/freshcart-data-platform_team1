import pandas as pd
import sys
import os

# Add parent directory to path to import freshcart_utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scripts.freshcart_utils import log_summary

# ── Config: all 5 files with their key columns ──────────────────────────────
FILES = [
    {'file': 'orders.csv',      'key_column': 'order_id'},
    {'file': 'customers.csv',   'key_column': 'customer_id'},
    {'file': 'products.csv',    'key_column': 'product_id'},
    {'file': 'order_items.csv', 'key_column': 'order_item_id'},
    {'file': 'deliveries.csv',  'key_column': 'delivery_id'},
]

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')


def profile_file(file: str, key_column: str) -> None:
    """Profile a single CSV file and print a data quality report."""
    filepath = os.path.join(DATA_DIR, file)

    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}")
        return

    rows, cols = df.shape

    # Null counts using list comprehension
    null_report = {col: int(df[col].isnull().sum()) for col in df.columns}
    null_str = ', '.join([f"{col}: {count}" for col, count in null_report.items()])

    # Duplicate count on key column
    dup_count = int(df[key_column].duplicated().sum()) if key_column in df.columns else 0

    # Data types
    dtype_str = ', '.join([f"{col}: {str(dtype)}" for col, dtype in df.dtypes.items()])

    # Print report
    print(f"\n{'='*60}")
    print(f"  File: {file}")
    print(f"  Rows: {rows}  |  Columns: {cols}")
    print(f"  Null counts:  {null_str}")
    print(f"  Duplicate key ({key_column}): {dup_count} duplicates found")
    print(f"  Data types:  {dtype_str}")

    # Call log_summary from freshcart_utils
    key_null_count = null_report.get(key_column, 0)
    key_null_report = {'column': key_column, 'null_count': key_null_count, 'valid': key_null_count == 0}
    log_summary(file.replace('.csv', ''), rows, key_null_report, dup_count)


if __name__ == '__main__':
    print("FreshCart — Data Profiler Report")
    print("=" * 60)
    for entry in FILES:
        profile_file(entry['file'], entry['key_column'])
    print(f"\n{'='*60}")
    print("Profiling complete.")