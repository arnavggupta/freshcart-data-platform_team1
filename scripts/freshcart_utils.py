import csv

def read_csv(filepath):
    """
    Reads CSV file and returns list of dictionaries
    """
    data = []
    try:
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
    except FileNotFoundError:
        print(f"Error: File {filepath} not found")
    return data


def validate_not_null(data, column):
    """
    Checks null values in a column
    """
    null_count = sum(1 for row in data if not row[column])
    return {
        'column': column,
        'null_count': null_count,
        'valid': null_count == 0
    }


def count_duplicates(data, key_column):
    """
    Counts duplicate values
    """
    seen = set()
    duplicates = 0

    for row in data:
        val = row[key_column]
        if val in seen:
            duplicates += 1
        else:
            seen.add(val)

    return duplicates


def log_summary(table_name, row_count, null_report, dup_count):
    """
    Prints summary
    """
    print(f"[FreshCart] {table_name} | rows: {row_count} | "
          f"nulls in {null_report['column']}: {null_report['null_count']} | "
          f"duplicates: {dup_count}")


if __name__ == "__main__":
    data = read_csv('../data/orders.csv')

    null_report = validate_not_null(data, 'order_id')
    dup_count = count_duplicates(data, 'order_id')

    log_summary('orders', len(data), null_report, dup_count)