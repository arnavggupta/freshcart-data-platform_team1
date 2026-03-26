import boto3
import csv
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError


class OrderEventProducer:

    CONFIG = {
        'stream_name': 'freshcart-orders-stream',
        'region': 'ap-south-1',
        'batch_size': 50,
        'delay_seconds': 0.1
    }

    def __init__(self):
        self.client = boto3.client('kinesis', region_name=self.CONFIG['region'])
        self.sent = 0
        self.failed = 0

    def build_event(self, row):
        row['event_timestamp'] = datetime.utcnow().isoformat()
        return json.dumps(row)

    def send_event(self, event_json):
        try:
            self.client.put_record(
                StreamName=self.CONFIG['stream_name'],
                Data=event_json,
                PartitionKey='1'
            )
            self.sent += 1
        except ClientError as e:
            self.failed += 1
            print(f"Error: {e}")

    def run(self, csv_path):
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if i >= self.CONFIG['batch_size']:
                    break

                event = self.build_event(row)
                self.send_event(event)

                time.sleep(self.CONFIG['delay_seconds'])

        print(f"Sent: {self.sent} | Failed: {self.failed}")


if __name__ == "__main__":
    producer = OrderEventProducer()
    producer.run('../data/orders.csv')