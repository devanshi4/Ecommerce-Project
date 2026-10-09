# End-to-End Data Engineering Pipeline — AWS + PySpark + Airflow
A production-style data engineering project built on AWS, processing both 
real-time streaming and batch data through a complete Bronze → Silver → Gold 
lakehouse architecture.

## What This Project Does
Simulates a real e-commerce data platform that:
- Ingests **live clickstream events** (user clicks, add-to-cart, purchases) via streaming
- Ingests **daily order data** via batch processing
- Automatically cleans, transforms, and aggregates both datasets
- Runs the entire pipeline on a daily schedule without manual intervention

## Architecture
```
Data Sources
├── Clickstream Events → Kinesis Data Stream (real-time)
└── Order Data        → S3 Bronze Layer (batch)
Ingestion Layer
├── Kinesis → Lambda → S3 Bronze (streaming path)
└── Python Generator → S3 Bronze (batch path)
Processing Layer
├── PySpark transformations (Bronze → Silver)
└── PySpark aggregations (Silver → Gold)
Catalog + Query Layer
├── AWS Glue Crawler → Glue Data Catalog
└── Amazon Athena (SQL queries on S3)
Orchestration
└── Apache Airflow DAG (scheduled daily at 6am)
```

## Tech Stack
```
| Layer | Technology |
|---|---|
| Cloud Platform | AWS |
| Storage | Amazon S3 (data lake) |
| Streaming | Amazon Kinesis |
| Serverless | AWS Lambda |
| Processing | PySpark |
| Catalog | AWS Glue |
| Querying | Amazon Athena |
| Orchestration | Apache Airflow |
| Permissions | AWS IAM |
| Language | Python |
```

## Data Architecture — Medallion Pattern
- **Bronze** — Raw data exactly as ingested. Never modified. Safety net for reprocessing.
- **Silver** — Cleaned, typed, deduplicated data. Quality checks enforced.
- **Gold** — Business-ready aggregations. Daily revenue, category analysis, conversion funnels.

## Pipeline Walkthrough

### Streaming Path
1. Python generator sends clickstream events to Kinesis (5 events/second)
2. AWS Lambda triggers automatically on new Kinesis records
3. Lambda decodes events and writes JSONL files to S3 Bronze layer
4. Glue Crawler scans Bronze and registers schema in Glue Data Catalog
5. Data immediately queryable via Athena SQL

### Batch Path
1. Airflow DAG triggers daily at 6am UTC
2. Generates 200 realistic orders and uploads to S3 (partitioned by date)
3. Validates data exists before processing
4. Cleans the day's Bronze orders → writes Silver (pandas, CSV)
5. Aggregates Silver → writes 3 daily Gold tables (CSV)
6. Pipeline completes with full audit log

### Spark Path (full history, orders + clickstream)
`jobs/medallion_job.py` runs the full Bronze → Silver → Gold build with PySpark over all order and clickstream data and writes Parquet:
1. Casts types and adds date/hour columns
2. Applies data-quality filters (missing keys, non-positive prices or quantities) and fails the run if a layer comes out empty or more than 5% of rows are dropped
3. Joins clickstream to orders on `user_id` and builds 6 Gold tables

It runs with `spark-submit` locally, on EMR, or as a Glue job. `notebooks/phase3_transformations.ipynb` is the exploratory walkthrough of the same logic.

## Gold Tables Built
- `daily_revenue` — total revenue, order count, average order value per day
- `revenue_by_category` — which product categories drive most revenue
- `orders_by_status` — completed vs cancelled vs pending breakdown
- `device_conversion` — mobile vs desktop purchase conversion rates
- `user_journey` — sessions, events, and spend per user
- `category_funnel` — interactions vs completed orders per category

## Key Engineering Concepts Demonstrated
- **Medallion Architecture** — Bronze/Silver/Gold data lake pattern
- **Event-driven architecture** — Kinesis triggers Lambda automatically
- **Hive-style partitioning** — `date=YYYY-MM-DD` for query performance
- **Least privilege IAM** — separate roles for Lambda and Glue
- **Separation of storage and compute** — S3 + Glue + Athena pattern
- **Pipeline orchestration** — Airflow DAGs with task dependencies and retries
- **Batch vs streaming** — both patterns implemented in one project

## Project Structure
```
ecommerce-data-platform/
├── data_generator/
│   ├── config.py
│   ├── generate_orders.py      # batch order generator
│   └── generate_clickstream.py # streaming event generator
├── lambda/
│   ├── kinesis_to_s3/
│   │   └── handler.py          # Lambda function
│   ├── trust-policy.json
│   └── lambda-permissions.json
├── jobs/
│   └── medallion_job.py        # PySpark Bronze→Silver→Gold (spark-submit)
├── notebooks/
│   └── phase3_transformations.ipynb  # Exploratory walkthrough of the Spark logic
├── airflow/
│   ├── dags/
│   │   └── ecommerce_pipeline.py     # Airflow DAG
│   └── docker-compose.yml
└── README.md
```

## How to Run
1. Configure AWS credentials: `aws configure`
2. Create S3 bucket and Kinesis stream (see setup guide)
3. Deploy Lambda function
4. Run order generator: `python data_generator/generate_orders.py`
5. Run clickstream generator: `python data_generator/generate_clickstream.py`
6. Start Airflow: `docker-compose up -d` from `/airflow`
7. Trigger DAG from Airflow UI at `localhost:8080`
8. Run the Spark build:
   ```bash
   # local copy of the bucket layout (bronze/orders/, bronze/clickstream/)
   spark-submit jobs/medallion_job.py --base-path ./data
   # or straight from S3
   spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 jobs/medallion_job.py --base-path s3a://<your-bucket>
   ```