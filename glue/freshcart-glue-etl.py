import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, current_timestamp
from pyspark.sql.types import DoubleType, IntegerType

# ── Init ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET = "s3-de-q1-26/DE-Training/freshcart-datalake-team1"

FILES_CONFIG = [
    {
        'name'        : 'orders',
        'input'       : f's3://{BUCKET}/raw/orders/',
        'output'      : f's3://{BUCKET}/processed/orders/',
        'drop_null_on': 'order_id',
        'transforms'  : ['cast_total_amount', 'cast_order_date']
    },
    {
        'name'        : 'customers',
        'input'       : f's3://{BUCKET}/raw/customers/',
        'output'      : f's3://{BUCKET}/processed/customers/',
        'drop_null_on': 'customer_id',
        'transforms'  : ['cast_signup_date']
    },
    {
        'name'        : 'products',
        'input'       : f's3://{BUCKET}/raw/products/',
        'output'      : f's3://{BUCKET}/processed/products/',
        'drop_null_on': 'product_id',
        'transforms'  : ['cast_unit_price_products', 'cast_in_stock']
    },
    {
        'name'        : 'order_items',
        'input'       : f's3://{BUCKET}/raw/order_items/',
        'output'      : f's3://{BUCKET}/processed/order_items/',
        'drop_null_on': 'item_id',
        'transforms'  : ['cast_quantity', 'cast_unit_price_items', 'cast_line_total']
    },
    {
        'name'        : 'deliveries',
        'input'       : f's3://{BUCKET}/raw/deliveries/',
        'output'      : f's3://{BUCKET}/processed/deliveries/',
        'drop_null_on': 'delivery_id',
        'transforms'  : ['cast_delivery_minutes', 'cast_distance_km', 'cast_rating']
    },
]


# ── Transformation functions ──────────────────────────────────────────────────

# orders
def cast_total_amount(df):
    return df.withColumn("total_amount", col("total_amount").cast(DoubleType()))

def cast_order_date(df):
    return df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# customers
def cast_signup_date(df):
    return df.withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

# products
def cast_unit_price_products(df):
    return df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

def cast_in_stock(df):
    return df.withColumn("in_stock", col("in_stock").cast(IntegerType()))

# order_items
def cast_quantity(df):
    return df.withColumn("quantity", col("quantity").cast(IntegerType()))

def cast_unit_price_items(df):
    return df.withColumn("unit_price", col("unit_price").cast(DoubleType()))

def cast_line_total(df):
    return df.withColumn("line_total", col("line_total").cast(DoubleType()))

# deliveries
def cast_delivery_minutes(df):
    return df.withColumn("delivery_minutes", col("delivery_minutes").cast(IntegerType()))

def cast_distance_km(df):
    return df.withColumn("distance_km", col("distance_km").cast(DoubleType()))

def cast_rating(df):
    return df.withColumn("rating", col("rating").cast(DoubleType()))


# Map transform names to functions
TRANSFORM_MAP = {
    'cast_total_amount'       : cast_total_amount,
    'cast_order_date'         : cast_order_date,
    'cast_signup_date'        : cast_signup_date,
    'cast_unit_price_products': cast_unit_price_products,
    'cast_in_stock'           : cast_in_stock,
    'cast_quantity'           : cast_quantity,
    'cast_unit_price_items'   : cast_unit_price_items,
    'cast_line_total'         : cast_line_total,
    'cast_delivery_minutes'   : cast_delivery_minutes,
    'cast_distance_km'        : cast_distance_km,
    'cast_rating'             : cast_rating,
}


# ── Main processing loop ──────────────────────────────────────────────────────

def process_file(config: dict) -> None:
    name        = config['name']
    input_path  = config['input']
    output_path = config['output']
    null_col    = config['drop_null_on']
    transforms  = config['transforms']

    print(f"\n{'='*60}")
    print(f"[INFO] Processing: {name}")

    # 1. Read CSV
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(input_path)

    print(f"[INFO] Raw row count        : {df.count()}")
    df.printSchema()

    # 2. Drop nulls on key column
    df = df.filter(col(null_col).isNotNull())
    print(f"[INFO] After dropping nulls : {df.count()}")

    # 3. Apply transforms
    for transform_name in transforms:
        if transform_name in TRANSFORM_MAP:
            df = TRANSFORM_MAP[transform_name](df)
            print(f"[INFO] Applied transform    : {transform_name}")
        else:
            print(f"[WARN] Unknown transform skipped: {transform_name}")

    # 4. Add ingestion_timestamp to every file
    df = df.withColumn("ingestion_timestamp", current_timestamp())

    # 5. Write as Parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"[INFO] Written to           : {output_path}")


# ── Run for all 5 files ───────────────────────────────────────────────────────
for config in FILES_CONFIG:
    try:
        process_file(config)
    except Exception as e:
        print(f"[ERROR] Failed processing {config['name']}: {str(e)}")
        raise e

print(f"\n{'='*60}")
print("[INFO] All 5 files processed successfully.")

job.commit()