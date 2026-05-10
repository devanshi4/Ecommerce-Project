# =============================================================================
# config.py — Central configuration for the entire project
# Every other file imports from here. Change a value once, it updates everywhere.
# =============================================================================

# ── AWS Settings ──────────────────────────────────────────────────────────────

# Your S3 bucket name — replace yourname with what you actually named it
S3_BUCKET = "ecommerce-data-platform-dp"

# The Kinesis stream we created in Step 3
KINESIS_STREAM = "ecommerce-clickstream"

# AWS region — must match the region you used when creating S3 and Kinesis
AWS_REGION = "us-east-1"


# ── S3 Folder Prefixes ────────────────────────────────────────────────────────
# These match the folder structure we created in Step 2
# We define them here so if we ever reorganize folders, we change it once

BRONZE_ORDERS_PREFIX = "bronze/orders"
BRONZE_CLICKSTREAM_PREFIX = "bronze/clickstream"

SILVER_ORDERS_PREFIX = "silver/orders"
SILVER_CLICKSTREAM_PREFIX = "silver/clickstream"

GOLD_DAILY_REVENUE_PREFIX = "gold/daily_revenue"
GOLD_FUNNEL_PREFIX = "gold/funnel_conversion"


# ── Data Generation Settings ──────────────────────────────────────────────────
# Controls how much fake data we generate

NUM_USERS = 500           # size of our simulated user pool
NUM_PRODUCTS = 100        # number of products in our fake catalog
ORDERS_PER_RUN = 200      # how many orders to generate per daily batch
CLICKSTREAM_EVENTS_PER_SECOND = 5   # how fast we send events to Kinesis