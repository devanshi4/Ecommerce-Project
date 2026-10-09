"""
Bronze → Silver → Gold with PySpark.

The same transformations as notebooks/phase3_transformations.ipynb, packaged
as a script so they can run with spark-submit (locally, on EMR, or as a
Glue job) instead of only inside a notebook.

Usage (local folders):
    spark-submit jobs/medallion_job.py --base-path ./data

Usage (S3, needs the hadoop-aws package and AWS credentials in the environment):
    spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
        jobs/medallion_job.py --base-path s3a://<your-bucket>

Expected layout under --base-path:
    bronze/orders/        CSV files with a header row
    bronze/clickstream/   JSON lines, one event per line
Outputs (Parquet, overwritten each run):
    silver/orders/, silver/clickstream/
    gold/revenue_by_category/, gold/orders_by_status/, gold/daily_revenue/,
    gold/user_journey/, gold/device_conversion/, gold/category_funnel/
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ── Orders: Bronze → Silver ───────────────────────────────────────────────────
def orders_bronze_to_silver(df_orders: DataFrame) -> DataFrame:
    # Fix column types
    df = (
        df_orders
        .withColumn("order_timestamp", F.to_timestamp("order_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    )
    # Derived columns for analytics
    df = (
        df
        .withColumn("order_date", F.to_date("order_timestamp"))
        .withColumn("order_hour", F.hour("order_timestamp"))
        .withColumn("order_month", F.month("order_timestamp"))
    )
    # Data quality: drop rows missing keys or with impossible values
    df = (
        df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("total_price") > 0)
        .filter(F.col("quantity") > 0)
    )
    # "date" is added by Spark from the date=YYYY-MM-DD folder name; order_date replaces it
    return df.drop("date")


# ── Orders: Silver → Gold ─────────────────────────────────────────────────────
def build_order_gold(df_silver: DataFrame) -> dict:
    completed = df_silver.filter(F.col("status") == "completed")

    revenue_by_category = (
        completed.groupBy("category")
        .agg(
            F.round(F.sum("total_price"), 2).alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.round(F.avg("total_price"), 2).alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    orders_by_status = (
        df_silver.groupBy("status")
        .agg(F.count("order_id").alias("total_orders"))
        .orderBy(F.desc("total_orders"))
    )
    daily_revenue = (
        completed.groupBy("order_date")
        .agg(
            F.round(F.sum("total_price"), 2).alias("daily_revenue"),
            F.count("order_id").alias("total_orders"),
        )
        .orderBy("order_date")
    )
    return {
        "revenue_by_category": revenue_by_category,
        "orders_by_status": orders_by_status,
        "daily_revenue": daily_revenue,
    }


# ── Clickstream: Bronze → Silver ──────────────────────────────────────────────
def clickstream_bronze_to_silver(df_clickstream: DataFrame) -> DataFrame:
    df = (
        df_clickstream
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("event_hour", F.hour("event_timestamp"))
    )
    # Data quality: drop events missing their identifiers
    return (
        df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_type").isNotNull())
    )


# ── Clickstream + Orders: Gold ────────────────────────────────────────────────
def build_clickstream_gold(df_click_silver: DataFrame, df_orders_silver: DataFrame) -> dict:
    # LEFT JOIN on user_id: keep every event, attach order info where it exists
    df_joined = df_click_silver.join(
        df_orders_silver.select(
            "user_id",
            "order_id",
            "total_price",
            F.col("category").alias("order_category"),
            "status",
            "order_date",
        ),
        on="user_id",
        how="left",
    )

    user_journey = (
        df_joined.groupBy("user_id")
        .agg(
            F.count("event_id").alias("total_events"),
            F.countDistinct("session_id").alias("total_sessions"),
            F.round(F.sum("total_price"), 2).alias("total_spent"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .orderBy(F.desc("total_spent"))
    )
    device_conversion = (
        df_click_silver.groupBy("device_type")
        .agg(
            F.count("event_id").alias("total_events"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        )
        .withColumn("conversion_rate", F.round(F.col("total_purchases") / F.col("total_events") * 100, 2))
        .orderBy(F.desc("conversion_rate"))
    )
    category_funnel = (
        df_joined.groupBy("order_category")
        .agg(
            F.count("event_id").alias("total_interactions"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_orders"),
        )
        .filter(F.col("order_category").isNotNull())
        .orderBy(F.desc("total_interactions"))
    )
    return {
        "user_journey": user_journey,
        "device_conversion": device_conversion,
        "category_funnel": category_funnel,
    }


# ── Quality gate ──────────────────────────────────────────────────────────────
def check_quality(name: str, before: int, after: int, max_drop_pct: float) -> None:
    """Fail the run if a layer came out empty or too many rows were dropped."""
    dropped = before - after
    drop_pct = (dropped / before * 100) if before else 0.0
    print(f"{name}: {before} rows in, {after} rows out, {dropped} dropped ({drop_pct:.1f}%)")
    if after == 0:
        raise ValueError(f"{name}: no rows left after quality filters")
    if drop_pct > max_drop_pct:
        raise ValueError(f"{name}: dropped {drop_pct:.1f}% of rows (limit {max_drop_pct}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base-path", required=True, help="Folder or s3a://bucket holding bronze/")
    parser.add_argument("--max-drop-pct", type=float, default=5.0, help="Fail if more rows than this are dropped")
    args = parser.parse_args()
    base = args.base_path.rstrip("/")

    spark = SparkSession.builder.appName("ecommerce-medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df_orders = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{base}/bronze/orders/")
    df_clickstream = spark.read.option("multiline", "false").json(f"{base}/bronze/clickstream/")

    df_orders_silver = orders_bronze_to_silver(df_orders)
    check_quality("orders", df_orders.count(), df_orders_silver.count(), args.max_drop_pct)

    df_click_silver = clickstream_bronze_to_silver(df_clickstream)
    check_quality("clickstream", df_clickstream.count(), df_click_silver.count(), args.max_drop_pct)

    df_orders_silver.write.mode("overwrite").parquet(f"{base}/silver/orders/")
    df_click_silver.write.mode("overwrite").parquet(f"{base}/silver/clickstream/")

    gold = {**build_order_gold(df_orders_silver), **build_clickstream_gold(df_click_silver, df_orders_silver)}
    for name, df in gold.items():
        df.write.mode("overwrite").parquet(f"{base}/gold/{name}/")
        print(f"gold/{name}: {df.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
