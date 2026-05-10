# generate_clickstream.py
# Simulates real-time user behaviour events on an e-commerce website.
# Unlike the order generator which uploads files to S3,
# this script sends individual events to Kinesis one at a time, continuously.

import boto3      # AWS library — we use it to talk to Kinesis this time, not S3
import json       # we send events as JSON, not CSV
import random
import time       # we need this to control HOW FAST we send events
import uuid       # generates universally unique IDs for each event
from datetime import datetime
from faker import Faker

import config

# We import the product and user pools from the order generator
# so both datasets reference the same users and products.
# This is important — when we join orders and clickstream data later in
# Databricks, the user IDs need to match between both datasets.
from generate_orders import PRODUCTS, USER_IDS

fake = Faker()

# ── Event Definitions ─────────────────────────────────────────────────────────

# These are the types of events a user can trigger on an e-commerce site
EVENT_TYPES = [
    "page_view",        # user loads any page
    "product_view",     # user views a product detail page
    "add_to_cart",      # user adds something to cart
    "remove_from_cart", # user removes something from cart
    "checkout_start",   # user begins checkout
    "purchase",         # user completes a purchase
]

# Weights control how often each event type is chosen.
# page_view is most common, purchase is rarest.
# This reflects real e-commerce funnel behaviour —
# thousands of views, fewer adds to cart, even fewer purchases.
# This is called a "conversion funnel" and it's something
# data engineers build reports on constantly.
EVENT_WEIGHTS = [30, 25, 20, 5, 12, 8]

DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
REFERRERS = [None, "google.com", "facebook.com", "instagram.com", "email", "direct"]

# ── Connect to Kinesis ────────────────────────────────────────────────────────

# This creates a Kinesis client — the object we use to send data to Kinesis.
# Just like boto3.client("s3") talked to S3, boto3.client("kinesis") talks to Kinesis.
# It automatically uses the credentials from your `aws configure` setup.
kinesis_client = boto3.client("kinesis", region_name=config.AWS_REGION)


# ── Generate One Event ────────────────────────────────────────────────────────

def generate_single_event(session_id: str, user_id: str) -> dict:
    """
    Creates one clickstream event for a given user session.
    
    session_id: groups multiple events from the same user visit together.
                Think of it like a shopping trip — one visit = one session,
                but many events happen within that visit.
    user_id: which user is doing the action.
    """
    # Pick a random event type, using weights so purchases are rarer than views
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Some events are product-related, others are not
    # A page_view might be the homepage — no specific product involved
    is_product_event = event_type in ["product_view", "add_to_cart", "remove_from_cart", "purchase"]
    product = random.choice(PRODUCTS) if is_product_event else None

    return {
        # uuid4() generates a random unique ID for this specific event
        # No two events will ever have the same event_id
        "event_id": str(uuid.uuid4()),

        # session_id groups events from the same user visit
        "session_id": session_id,

        "user_id": user_id,
        "event_type": event_type,

        # Product info — only included if this is a product-related event
        # None becomes null in JSON
        "product_id": product["product_id"] if product else None,
        "product_name": product["product_name"] if product else None,
        "category": product["category"] if product else None,

        # Device and browser info — useful for "do mobile users convert less?" analysis
        "device_type": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),

        # Where the user came from — useful for marketing attribution analysis
        # "did users from Instagram buy more than users from Google?"
        "referrer": random.choice(REFERRERS),

        "country_code": fake.country_code(),
        "ip_address": fake.ipv4_public(),

        # "Z" at the end means UTC timezone — always use UTC in data pipelines
        # Mixing timezones is one of the most common sources of bugs in real pipelines
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ── Send One Event to Kinesis ─────────────────────────────────────────────────

def send_to_kinesis(event: dict):
    """
    Sends a single event to the Kinesis stream.
    
    Two important parameters:
    - Data: the actual event, converted to a JSON string
    - PartitionKey: determines which shard this record goes to
    
    We use session_id as the partition key.
    This means all events from the same user session go to the same shard,
    preserving the order of events within a session.
    Why does order matter? If "add_to_cart" arrives before "product_view"
    in your analysis, your session analysis will be wrong.
    """
    kinesis_client.put_record(
        StreamName=config.KINESIS_STREAM,
        
        # Kinesis expects bytes or a string — json.dumps converts our
        # dictionary into a JSON string
        Data=json.dumps(event),
        
        # Partition key routes events to shards and preserves ordering
        # within a session
        PartitionKey=event["session_id"],
    )


# ── Main Streaming Loop ───────────────────────────────────────────────────────

def stream_events(duration_seconds: int = 120):
    """
    Continuously generates and sends clickstream events to Kinesis.
    Runs for duration_seconds then stops.
    
    Default is 2 minutes — enough to generate meaningful data
    without running your AWS bill up.
    """
    print(f"\n🚀 Starting clickstream stream...")
    print(f"   Sending to: {config.KINESIS_STREAM}")
    print(f"   Rate: {config.CLICKSTREAM_EVENTS_PER_SECOND} events/second")
    print(f"   Duration: {duration_seconds} seconds")
    print(f"   Press Ctrl+C at any time to stop early\n")

    start_time = time.time()
    total_sent = 0

    # active_sessions simulates multiple users browsing at the same time
    # Key = session_id, Value = user_id
    # Think of it as: right now, these N users have open browser tabs on the site
    active_sessions = {}

    while time.time() - start_time < duration_seconds:

        # ── Manage active sessions ────────────────────────────────────────────
        # Occasionally start a new session (new user arrives on the site)
        # We cap at 15 active sessions to keep things manageable
        if len(active_sessions) < 5 or (len(active_sessions) < 15 and random.random() > 0.85):
            new_session_id = str(uuid.uuid4())
            new_user_id = random.choice(USER_IDS)
            active_sessions[new_session_id] = new_user_id

        # ── Generate and send an event ────────────────────────────────────────
        # Pick a random active session to generate an event for
        session_id = random.choice(list(active_sessions.keys()))
        user_id = active_sessions[session_id]

        event = generate_single_event(session_id, user_id)

        try:
            send_to_kinesis(event)
            total_sent += 1

            # Log progress every 20 events so you can see it's working
            if total_sent % 20 == 0:
                elapsed = time.time() - start_time
                remaining = duration_seconds - elapsed
                print(f"   ✅ {total_sent} events sent | "
                      f"{elapsed:.0f}s elapsed | "
                      f"{remaining:.0f}s remaining | "
                      f"Last: {event['event_type']} from {user_id}")

        except Exception as e:
            # We catch errors per-event so one failure doesn't crash everything
            # In production, failed events would go to a dead letter queue
            print(f"   ❌ Failed to send event: {e}")

        # Occasionally close a session (user leaves the site)
        if len(active_sessions) > 10 and random.random() > 0.95:
            session_to_close = list(active_sessions.keys())[0]
            del active_sessions[session_to_close]

        # This is what controls the speed — sleep between events
        # 1 / events_per_second = seconds to wait between each event
        # e.g. 5 events/sec = sleep 0.2 seconds between each
        time.sleep(1 / config.CLICKSTREAM_EVENTS_PER_SECOND)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_time = time.time() - start_time
    print(f"\n🎉 Stream complete.")
    print(f"   Total events sent: {total_sent}")
    print(f"   Total time: {total_time:.0f} seconds")
    print(f"   Actual rate: {total_sent/total_time:.1f} events/second")
    print(f"\n   Now check Lambda logs to see events being written to S3.")
    print(f"   (We set up Lambda in Phase 2)")


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Run for 2 minutes — generates ~600 events at 5/second
    # This is enough to see the pipeline working without running up costs
    stream_events(duration_seconds=120)