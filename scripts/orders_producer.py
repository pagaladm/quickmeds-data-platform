import csv
import json
import time
import boto3
from datetime import datetime
from botocore.exceptions import ClientError


class OrderEventProducer:

    CONFIG = {
        "stream_name"   : "quickmeds-orders-stream-team-4",
        "region"        : "eu-central-1",
        "batch_size"    : 50,
        "delay_seconds" : 0.1
    }

    def __init__(self):
        self.client = boto3.client("kinesis", region_name=self.CONFIG["region"])
        self.sent   = 0
        self.failed = 0

    def build_event(self, row):
        event = dict(row)
        event["event_timestamp"] = datetime.utcnow().isoformat()
        event["event_type"]      = "order_placed"
        return json.dumps(event)

    def send_event(self, event_json):
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

    def run(self, csv_path="data/QuickMeds_orders.csv"):
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
    producer.run("data/QuickMeds_orders.csv")