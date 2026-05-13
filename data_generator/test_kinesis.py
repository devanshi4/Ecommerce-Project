import boto3
import json
import time
import random
from faker import Faker

kinesis_client = boto3.client("kinesis", region_name="us-east-1")
fake = Faker()
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "purchase"]
USER_IDS = ["USR-001", "USR-002", "USR-003", "USR-004", "USR-005"]

total_sent = 0   # let's count how many we've sent

# "while True" means run forever — the only way to stop is Ctrl+C
while True:

    user_id = random.choice(USER_IDS)
    event_type = random.choice(EVENT_TYPES)

    event = {
        "user_id": user_id,
        "event_type": event_type,
        "country": fake.country_code(),
    }

    kinesis_client.put_record(
        StreamName="ecommerce-clickstream",
        Data=json.dumps(event),
        PartitionKey=user_id,
    )

    total_sent += 1
    print(f"[{total_sent} sent] {event_type} from {user_id}")

    time.sleep(1)