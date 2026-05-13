import json
import base64
import boto3
import os
from datetime import datetime

# boto3 creates our connection to S3
# Just like in the order generator, boto3 is the bridge to AWS
s3_client = boto3.client("s3")
S3_BUCKET = os.environ["S3_BUCKET"]


def lambda_handler(event, context):
    print(f"Lambda triggered — received {len(event['Records'])} records")

    # We'll collect all the events first, then write them together
    # Writing one file per event would create millions of tiny files
    # — terrible for Spark and Athena performance
    all_events = []

    # ── Step 1: Decode every record from Kinesis ──────────────────────────────
    for record in event["Records"]:

        # Get the raw base64 encoded data
        raw_data = record["kinesis"]["data"]

        # Decode from base64 → readable text
        decoded_data = base64.b64decode(raw_data)

        # Convert text → Python dictionary
        event_data = json.loads(decoded_data)

        # Add to our collection
        all_events.append(event_data)

    print(f"Successfully decoded {len(all_events)} events")

    # ── Step 2: Build the S3 file path ────────────────────────────────────────
    # We use the current time to create a unique path for this batch of events
    # This matches the partitioned structure we set up in Phase 1
    now = datetime.utcnow()

    s3_key = (
        f"bronze/clickstream/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"hour={now.strftime('%H')}/"
        f"events_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
    )
    # Example result:
    # bronze/clickstream/date=2025-05-09/hour=14/events_20250509_143022.jsonl

    # ── Step 3: Convert events to JSONL format ────────────────────────────────
    # JSONL means JSON Lines — one JSON event per line
    # This is better than one big JSON array because:
    # Spark can read it line by line without loading the whole file into memory
    jsonl_content = "\n".join(json.dumps(evt) for evt in all_events)

    # ── Step 4: Write to S3 ───────────────────────────────────────────────────
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=jsonl_content.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(f"Written to S3: s3://{S3_BUCKET}/{s3_key}")

    return {
        "statusCode": 200,
        "records_processed": len(all_events),
    }