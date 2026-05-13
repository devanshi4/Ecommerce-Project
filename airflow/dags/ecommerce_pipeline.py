# ecommerce_pipeline.py
# Complete pipeline DAG — generates data, transforms it, builds Gold tables
# This runs automatically every day at 6am UTC

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os
import json
import random
import io
import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)

# ── Default Arguments ─────────────────────────────────────────────────────────
# These apply to every task in the DAG
# This handles temporary issues like network blips

default_args = {
    "owner": "devanshi",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,    # would send email alerts in production
}

# ── Helper: Get S3 Client ─────────────────────────────────────────────────────
# We use this in multiple tasks so we define it once here
# Reads credentials from environment variables we set in .env

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )

S3_BUCKET = os.environ.get("S3_BUCKET", "ecommerce-data-platform-dp")

# ── Task Functions ────────────────────────────────────────────────────────────
# Each function below becomes one task in the pipeline

def log_pipeline_start(**context):
    """Task 1 — Log that pipeline has started"""
    print("=" * 50)
    print("🚀 Ecommerce Daily Pipeline Started")
    print(f"   Date: {datetime.utcnow().strftime('%Y-%m-%d')}")
    print("=" * 50)


def generate_daily_orders(**context):
    
    # Build product catalog and user pool
    categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty"]
    products = [
        {
            "product_id": f"PROD-{i:04d}",
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "base_price": round(random.uniform(5.0, 500.0), 2),
        }
        for i in range(1, 101)
    ]
    user_ids = [f"USR-{i:05d}" for i in range(1, 501)]
    statuses = ["completed", "completed", "completed", "pending", "cancelled"]

    today = datetime.utcnow()
    orders = []
    base_id = int(today.strftime("%Y%m%d")) * 10000

    for i in range(200):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        unit_price = round(product["base_price"] * random.uniform(0.85, 1.15), 2)
        discount = round(random.uniform(0.05, 0.25), 2) if random.random() > 0.7 else 0.0
        total_price = round(unit_price * quantity * (1 - discount), 2)

        orders.append({
            "order_id": f"ORD-{base_id + i:08d}",
            "user_id": random.choice(user_ids),
            "product_id": product["product_id"],
            "product_name": product["product_name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_pct": discount,
            "total_price": total_price,
            "currency": "USD",
            "status": random.choice(statuses),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"]),
            "shipping_country": fake.country_code(),
            "order_timestamp": today.isoformat(),
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        })

    # Write to CSV in memory and upload to S3
    df = pd.DataFrame(orders)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    date_str = today.strftime("%Y-%m-%d")
    s3_key = f"bronze/orders/date={date_str}/orders_{date_str}.csv"

    get_s3_client().put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"✅ Generated 200 orders → s3://{S3_BUCKET}/{s3_key}")


def check_s3_data(**context):
    
    today = datetime.utcnow().strftime("%Y-%m-%d")
    prefix = f"bronze/orders/date={today}/"

    response = get_s3_client().list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=prefix
    )

    if response.get("KeyCount", 0) == 0:
        raise FileNotFoundError(
            f"No order data found for {today}. Pipeline stopped."
        )

    print(f"✅ Data verified for {today}")


def transform_bronze_to_silver(**context):

    s3 = get_s3_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Download today's Bronze file
    bronze_key = f"bronze/orders/date={today}/orders_{today}.csv"
    response = s3.get_object(Bucket=S3_BUCKET, Key=bronze_key)
    df = pd.read_csv(io.BytesIO(response["Body"].read()))

    print(f"Bronze rows loaded: {len(df)}")

    # Apply Silver transformations — same logic as Phase 3 PySpark
    # just using pandas here since we don't have a Spark cluster locally

    # Remove bad rows
    df = df[df["order_id"].notna()]
    df = df[df["total_price"] > 0]
    df = df[df["quantity"] > 0]

    # Fix types
    df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
    df["ingestion_timestamp"] = pd.to_datetime(df["ingestion_timestamp"])

    # Add derived columns
    df["order_date"] = df["order_timestamp"].dt.date.astype(str)
    df["order_hour"] = df["order_timestamp"].dt.hour
    df["order_month"] = df["order_timestamp"].dt.month

    print(f"Silver rows after cleaning: {len(df)}")

    # Upload Silver CSV to S3
    silver_buffer = io.StringIO()
    df.to_csv(silver_buffer, index=False)

    silver_key = f"silver/orders/date={today}/orders_{today}.csv"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=silver_key,
        Body=silver_buffer.getvalue(),
        ContentType="text/csv",
    )

    print(f"✅ Silver written → s3://{S3_BUCKET}/{silver_key}")


def build_gold_tables(**context):
    
    s3 = get_s3_client()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Load Silver data
    silver_key = f"silver/orders/date={today}/orders_{today}.csv"
    response = s3.get_object(Bucket=S3_BUCKET, Key=silver_key)
    df = pd.read_csv(io.BytesIO(response["Body"].read()))

    # Only aggregate completed orders for revenue metrics
    df_completed = df[df["status"] == "completed"]

    # Gold Table 1: Daily revenue summary
    daily_revenue = pd.DataFrame([{
        "date": today,
        "total_revenue": round(df_completed["total_price"].sum(), 2),
        "total_orders": len(df_completed),
        "avg_order_value": round(df_completed["total_price"].mean(), 2),
        "total_all_orders": len(df),
    }])

    # Gold Table 2: Revenue by category
    category_revenue = df_completed.groupby("category").agg(
        total_revenue=("total_price", "sum"),
        total_orders=("order_id", "count"),
        avg_order_value=("total_price", "mean")
    ).round(2).reset_index()

    # Gold Table 3: Orders by status
    status_summary = df.groupby("status").agg(
        total_orders=("order_id", "count")
    ).reset_index()

    # Upload all three Gold tables to S3
    def upload_gold(df, name):
        buffer = io.StringIO()
        df.to_csv(buffer, index=False)
        key = f"gold/{name}/date={today}/{name}_{today}.csv"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="text/csv",
        )
        print(f"✅ Gold/{name} → s3://{S3_BUCKET}/{key}")

    upload_gold(daily_revenue, "daily_revenue")
    upload_gold(category_revenue, "revenue_by_category")
    upload_gold(status_summary, "orders_by_status")

    # Print summary so we can see it in Airflow logs
    print("\n📊 Today's Pipeline Summary:")
    print(f"   Total orders processed: {len(df)}")
    print(f"   Completed orders: {len(df_completed)}")
    print(f"   Total revenue: ${daily_revenue['total_revenue'].values[0]:,.2f}")
    print(f"   Top category: {category_revenue.sort_values('total_revenue', ascending=False).iloc[0]['category']}")


def log_pipeline_complete(**context):
    """Task 6 — Log successful completion"""
    print("=" * 50)
    print("✅ Ecommerce Daily Pipeline Complete")
    print(f"   Finished: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    print("   All layers updated: Bronze → Silver → Gold")
    print("=" * 50)


# ── Define The DAG ────────────────────────────────────────────────────────────
# This section creates the actual Airflow DAG and connects the tasks.

with DAG(
    dag_id="ecommerce_daily_pipeline",
    default_args=default_args,
    description="Daily pipeline: generate orders, clean to Silver, build Gold",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch"],
) as dag:

    # Wrap each function in a PythonOperator
    # PythonOperator = "run this Python function as an Airflow task"

    t1 = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    t2 = PythonOperator(
        task_id="generate_daily_orders",
        python_callable=generate_daily_orders,
    )

    t3 = PythonOperator(
        task_id="check_s3_data",
        python_callable=check_s3_data,
    )

    t4 = PythonOperator(
        task_id="transform_bronze_to_silver",
        python_callable=transform_bronze_to_silver,
    )

    t5 = PythonOperator(
        task_id="build_gold_tables",
        python_callable=build_gold_tables,
    )

    t6 = PythonOperator(
        task_id="log_pipeline_complete",
        python_callable=log_pipeline_complete,
    )

   
    # If any task fails, everything after it stops
    t1 >> t2 >> t3 >> t4 >> t5 >> t6