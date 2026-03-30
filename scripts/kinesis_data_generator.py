import boto3, json, random
from datetime import datetime


kinesis = boto3.client('kinesis', region_name='eu-central-1')

STREAM   = 'freshcart-orders-stream-team1'
CITIES   = ["Bangalore", "Hyderabad", "Chennai", "Mumbai", "Pune"]
STATUSES = ["DELIVERED", "DELIVERED", "DELIVERED", "PENDING", "CANCELLED"]
PAYMENTS = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Wallet"]

CUSTOMER_IDS = [f"C{i:03d}" for i in range(1, 101)]

def lambda_handler(event, context):
    sent = 0
    failed = 0

    for i in range(10):
        try:
            status = random.choice(STATUSES)

            order = {
                "order_id"        : f"ORD{random.randint(1000, 9999)}",
                "customer_id"     : random.choice(CUSTOMER_IDS),  
                "order_date"      : datetime.utcnow().strftime("%Y-%m-%d"),
                "order_time"      : datetime.utcnow().strftime("%H:%M:%S"),
                "status"          : status,
                "total_amount"    : round(random.uniform(100, 1500), 2),
                "delivery_minutes": random.randint(8, 45) if status == "DELIVERED" else None,
                "payment_method"  : random.choice(PAYMENTS),
                "city"            : random.choice(CITIES),
                "event_time"      : datetime.utcnow().isoformat(),
                "ingest_date"     : datetime.utcnow().strftime("%Y-%m-%d")
            }

            kinesis.put_record(
                StreamName=STREAM,
                Data=json.dumps(order),
                PartitionKey=order["order_id"]
            )
            sent += 1
            print(f"Sent: {order['order_id']} | {order['customer_id']} | {order['city']} | Rs{order['total_amount']} | {order['status']}")

        except Exception as e:
            failed += 1
            print(f"Failed: {str(e)}")

    print(f"Summary: Sent={sent} | Failed={failed}")
    return {"statusCode": 200, "sent": sent, "failed": failed}