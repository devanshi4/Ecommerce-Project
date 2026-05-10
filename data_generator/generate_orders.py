# generate_orders.py
# This script simulates a nightly database export from an e-commerce store.
# It generates realistic fake orders and uploads them to S3 Bronze layer.

import boto3       # AWS library — lets Python talk to S3, Kinesis, etc.
import pandas      # data manipulation — we use it to create a table of orders
import random      # for generating random choices (products, users, etc.)
import io          # lets us write files to memory instead of disk
from datetime import datetime, timedelta   # for working with dates and times
from faker import Faker    # generates realistic fake data (names, countries, etc.)

# Import our central config — this is why we built config.py first
import config

# ── Setup ─────────────────────────────────────────────────────────────────────

# Faker generates realistic random data
fake = Faker()

# random.seed(42) means: every time you run this script, the "random" choices
# will be the same sequence. This makes your data reproducible — important for
# testing. If you run it twice, you get the same users, same products, same
# patterns. Remove the seed if you want truly random data each run.
random.seed(42)


# ── Build a Product Catalog ───────────────────────────────────────────────────
# Real e-commerce stores have a product database. We simulate one here.
# We build it once at the top so every order can reference the same products.

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty"]

def build_product_catalog(num_products=config.NUM_PRODUCTS):
    """
    Creates a list of fake products with IDs, names, categories, and prices.
    This simulates what would normally come from a products database table.
    """
    products = []
    for i in range(1, num_products + 1):
        products.append({
            "product_id": f"PROD-{i:04d}",      # e.g. PROD-0001, PROD-0042
            "product_name": fake.catch_phrase(), # generates phrases like "Synergized content-based interface"
            "category": random.choice(CATEGORIES),
            "base_price": round(random.uniform(5.0, 500.0), 2),   # price between $5 and $500
        })
    return products

# Build the catalog once — all functions below will reference this same list
PRODUCTS = build_product_catalog()

# Create a pool of user IDs — simulates users registered in the platform
USER_IDS = [f"USR-{i:05d}" for i in range(1, config.NUM_USERS + 1)]

# Order statuses — weighted toward "completed" because most orders succeed
# The list has "completed" 3 times, so it gets picked ~50% of the time
ORDER_STATUSES = [
    "completed", "completed", "completed",
    "pending",
    "cancelled",
    "refunded"
]


# ── Generate One Order ────────────────────────────────────────────────────────

def generate_single_order(order_id: int, order_timestamp: datetime) -> dict:
    """
    Creates one realistic order record.
    Returns a dictionary — think of it as one row in a spreadsheet.
    """
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)

    # Prices vary slightly from base — simulates sales, regional pricing
    unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)

    # 30% of orders have a discount
    has_discount = random.random() > 0.7
    discount_pct = round(random.uniform(0.05, 0.25), 2) if has_discount else 0.0

    total_price = round(unit_price * quantity * (1 - discount_pct), 2)

    return {
        # Order identifiers
        "order_id": f"ORD-{order_id:08d}",   # e.g. ORD-00000001
        "user_id": random.choice(USER_IDS),
        
        # Product details — in a real pipeline, you'd join with a products table
        # but for Bronze layer we include them directly for simplicity
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        
        # Financial details
        "quantity": quantity,
        "unit_price": unit_price,
        "discount_pct": discount_pct,
        "total_price": total_price,
        "currency": "USD",
        
        # Order metadata
        "status": random.choice(ORDER_STATUSES),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
        "shipping_country": fake.country_code(),  # e.g. "US", "GB", "DE"
        
        # Timestamps
        # order_timestamp = when the customer placed the order
        "order_timestamp": order_timestamp.isoformat(),
        
        # ingestion_timestamp = when this file was generated and uploaded
        # In real pipelines, tracking this separately helps you debug
        # "why did this order arrive late?" type questions
        "ingestion_timestamp": datetime.utcnow().isoformat(),
    }


# ── Generate a Full Day of Orders and Upload to S3 ───────────────────────────

def generate_and_upload_orders(run_date: datetime = None, num_orders: int = config.ORDERS_PER_RUN):
    """
    Generates a full batch of orders for a given date and uploads to S3.
    
    run_date: which date to generate orders for (defaults to today)
    num_orders: how many orders to generate
    """
    if run_date is None:
        run_date = datetime.utcnow()

    print(f"\n📦 Generating {num_orders} orders for {run_date.strftime('%Y-%m-%d')}...")

    # Generate all orders for this day
    orders = []
    
    # Order IDs are unique across days — we use the date as a base number
    # e.g. May 1 2025 → base ID 20250501_0000, 20250501_0001, etc.
    base_order_id = int(run_date.strftime("%Y%m%d")) * 10000

    for i in range(num_orders):
        # Spread orders randomly throughout the day (0 to 86399 seconds = 24 hours)
        seconds_offset = random.randint(0, 86399)
        order_time = run_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=seconds_offset)
        
        orders.append(generate_single_order(base_order_id + i, order_time))

    # Convert list of dictionaries → Pandas DataFrame (like a spreadsheet in Python)
    df = pandas.DataFrame(orders)

    # ── Build the S3 key (file path) ──────────────────────────────────────────
    # This is the Hive partitioning we discussed — date=YYYY-MM-DD format
    date_str = run_date.strftime("%Y-%m-%d")
    s3_key = f"{config.BRONZE_ORDERS_PREFIX}/date={date_str}/orders_{date_str}.csv"
    # Result: "bronze/orders/date=2025-05-01/orders_2025-05-01.csv"

    # ── Write to S3 ───────────────────────────────────────────────────────────
    # We use io.StringIO() to write the CSV to memory (RAM) instead of saving
    # a file on disk first. This is faster and cleaner — no temp files to clean up.
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)  # index=False means don't add a row number column

    # boto3 is the AWS Python library — this creates an S3 client
    # It automatically uses the credentials from your `aws configure` setup
    s3_client = boto3.client("s3", region_name=config.AWS_REGION)
    
    s3_client.put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue(),    # the actual CSV content
        ContentType="text/csv",        # tells S3 what kind of file this is
    )

    print(f"✅ Uploaded → s3://{config.S3_BUCKET}/{s3_key}")
    print(f"   Orders generated: {num_orders}")
    print(f"   Sample order ID: {orders[0]['order_id']}")
    return df


# ── Generate Multiple Days of Historical Data ─────────────────────────────────

def backfill_historical_orders(days_back: int = 7):
    """
    Generates orders for the past N days.
    
    Why do we need this? When we connect Databricks and Athena later,
    we want enough historical data to run meaningful queries like
    "revenue by day over the past week." One day of data isn't interesting.
    
    In real companies this is called a 'backfill' — loading historical data
    into a new pipeline that didn't exist when that data was created.
    Backfills are extremely common whenever a new pipeline is built.
    """
    print(f"🕐 Starting backfill for the past {days_back} days...")
    
    for i in range(days_back, 0, -1):
        past_date = datetime.utcnow() - timedelta(days=i)
        generate_and_upload_orders(run_date=past_date)
    
    print(f"\n🎉 Backfill complete — {days_back} days of orders now in S3.")


# ── Entry Point ───────────────────────────────────────────────────────────────
# This block only runs when you execute this file directly (python generate_orders.py)
# It does NOT run when another file imports this one — that's what `if __name__` means

if __name__ == "__main__":
    # Step 1: Generate 7 days of historical data
    backfill_historical_orders(days_back=7)
    
    # Step 2: Generate today's orders
    generate_and_upload_orders()
    
    print("\n✅ All done. Check your S3 bucket in the AWS console.")