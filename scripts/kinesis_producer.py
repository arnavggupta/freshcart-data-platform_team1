import boto3
import csv
import json
import time
from datetime import datetime
from botocore.exceptions import ClientError


class OrderEventProducer:
    """
    Produces real-time order events from a CSV file and sends them
    to an AWS Kinesis Data Stream. Tracks sent/failed counts.
    """

    CONFIG = {
        'stream_name': 'freshcart-orders-stream',
        'region': 'ap-south-1',
        'batch_size': 450,
        'delay_seconds': 0.1
    }

    def __init__(self):
        self.client = boto3.client('kinesis', region_name=self.CONFIG['region'])
        self.sent = 0
        self.failed = 0

    def build_event(self, row):
        """
        Takes a CSV row dict, attaches a UTC timestamp, and
        returns the event as a JSON string.

        Args:
            row (dict): A single row from the orders CSV.

        Returns:
            str: JSON-encoded event string.
        """
        event = dict(row)  # copy first so that original row remains untouched
        event['event_timestamp'] = datetime.utcnow().isoformat()
        return json.dumps(event)

    def send_event(self, event_json):
        """
        Sends one event JSON string to Kinesis using put_record().
        Uses order_id as the partition key if available.
        Increments self.sent on success, self.failed on ClientError.

        Args:
            event_json (str): The JSON string to send.
        """
        try:
            self.client.put_record(
                StreamName=self.CONFIG['stream_name'],
                Data=event_json,
                PartitionKey=str(json.loads(event_json).get('order_id', '1'))
            )
            self.sent += 1
        except ClientError as e:
            self.failed += 1
            print(f"Error: {e}")

def run(self, csv_path): # Added a try except block for error handling
    """
        Reads orders from a CSV file row by row. Sends up to
        CONFIG['batch_size'] events with a delay between each.
        Prints a final summary of sent vs failed counts.

        Args:
            csv_path (str): Path to the orders CSV file.
        """
    try:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)

            for i, row in enumerate(reader):
                if i >= self.CONFIG['batch_size']:
                    break

                event = self.build_event(row)
                self.send_event(event)

                time.sleep(self.CONFIG['delay_seconds'])

    except FileNotFoundError:
        print(f"[ERROR] File not found: {csv_path}")
        return

    print(f"Sent: {self.sent} | Failed: {self.failed}")


if __name__ == "__main__":
    producer = OrderEventProducer()
    producer.run('../data/orders.csv')