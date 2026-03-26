import pandas as pd
from freshcart_utils import log_summary

files = [
    {'file': 'orders.csv', 'key': 'order_id'},
    {'file': 'customers.csv', 'key': 'customer_id'},
    {'file': 'products.csv', 'key': 'product_id'},
    {'file': 'order_items.csv', 'key': 'order_item_id'},
    {'file': 'deliveries.csv', 'key': 'delivery_id'}
]

for f in files:
    df = pd.read_csv(f'../data/{f["file"]}')

    print(f"\nFile: {f['file']}")
    print(f"Rows: {df.shape[0]} | Columns: {df.shape[1]}")

    nulls = df.isnull().sum()
    print("Null counts:", nulls.to_dict())

    dup = df.duplicated(subset=[f['key']]).sum()

    print("Data types:", df.dtypes.to_dict())

    log_summary(f['file'], df.shape[0],
                {'column': f['key'], 'null_count': nulls[f['key']]},
                dup)