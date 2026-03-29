# Manual GCP Setup Guide

This guide walks through the same RetailPulse project setup using the GCP Console instead of Terraform. Use it if you want to understand every resource visually or build the project without Infrastructure as Code.

## Before you start

- Create or choose a GCP project.
- Make sure billing is enabled.
- Install the Google Cloud SDK if you also want to use the `gcloud` and `bq` commands from the repo.
- Decide your primary region, for example `us-central1`.

## Step 1: Enable required APIs

In the GCP Console, go to `APIs & Services` -> `Library` and enable:

- BigQuery API
- Cloud Composer API
- Cloud Dataflow API
- Cloud Pub/Sub API
- Cloud Storage API

## Step 2: Create Cloud Storage buckets

Go to `Cloud Storage` -> `Buckets` and create:

- `your-gcp-project-id-retailpulse-raw`
- `your-gcp-project-id-retailpulse-temp`

Recommended settings:

- region: `us-central1` or your chosen region
- access control: uniform
- public access prevention: enforced

Use cases:

- `retailpulse-raw`: dead-letter and archive files
- `retailpulse-temp`: Dataflow temp and staging files

## Step 3: Create Pub/Sub resources

Go to `Pub/Sub` -> `Topics` and create:

- `retail-events`
- `retail-events-dlq`

Then go to `Pub/Sub` -> `Subscriptions` and create:

- subscription name: `retail-events-sub`
- attached topic: `retail-events`
- acknowledgment deadline: `60` seconds
- dead-letter topic: `retail-events-dlq`
- maximum delivery attempts: `10`

If the Console asks for permissions on the dead-letter topic, allow the Pub/Sub service agent to publish to it.

## Step 4: Create the Dataflow service account

Go to `IAM & Admin` -> `Service Accounts` and create:

- name: `retailpulse-dataflow`
- email: `retailpulse-dataflow@your-gcp-project-id.iam.gserviceaccount.com`

Grant these roles to the service account:

- `BigQuery Data Editor`
- `Dataflow Worker`
- `Pub/Sub Subscriber`
- `Storage Object Admin`
- `Viewer`

## Step 5: Create BigQuery datasets

Go to `BigQuery` and create the following datasets:

- `bronze`
- `silver`
- `gold`
- `ops`

Recommended settings:

- location: `US`
- default table expiration: none

## Step 6: Create base tables in BigQuery

Open the BigQuery SQL editor and run [01_create_objects.sql](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/sql/01_create_objects.sql). Replace `your-gcp-project-id` with your actual project ID first.

This creates:

- `bronze.retail_events`
- `ops.real_time_order_metrics`
- `ops.data_quality_audit`

The `silver` and `gold` tables are created later by the transformation SQL and Composer workflow.

## Step 7: Optional Cloud Composer setup

Cloud Composer is useful for interview demos because it shows orchestration and scheduling, but it is also one of the more expensive services in the project. If you want the orchestration component:

1. Go to `Cloud Composer` -> `Create environment`.
2. Choose the latest supported Composer image in your region.
3. Use the same region as the rest of the project when possible.
4. After the environment is ready, upload [retailpulse_realtime_dag.py](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/orchestration/composer/retailpulse_realtime_dag.py) and the SQL files from [sql](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/sql).
5. Add Airflow variables:
   - `retailpulse_project_id`
   - `retailpulse_dataset_location`

If you do not want to pay for Composer during practice, you can still run the SQL files manually in BigQuery and explain Composer as the production orchestration choice.

## Step 8: Install Python dependencies locally

From the repo root:

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Step 9: Start the streaming pipeline

Run the pipeline from [pipeline.py](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/src/streaming/pipeline.py):

```powershell
python -m src.streaming.pipeline `
  --project_id=your-gcp-project-id `
  --region=us-central1 `
  --input_subscription=projects/your-gcp-project-id/subscriptions/retail-events-sub `
  --raw_table=your-gcp-project-id:bronze.retail_events `
  --metrics_table=your-gcp-project-id:ops.real_time_order_metrics `
  --dead_letter_path=gs://your-gcp-project-id-retailpulse-raw/dead-letter/retail-events `
  --temp_location=gs://your-gcp-project-id-retailpulse-temp/dataflow/temp `
  --staging_location=gs://your-gcp-project-id-retailpulse-temp/dataflow/staging `
  --runner=DataflowRunner
```

## Step 10: Publish sample events

Run the producer from [order_events_producer.py](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/src/producer/order_events_producer.py):

```powershell
python -m src.producer.order_events_producer `
  --project_id=your-gcp-project-id `
  --topic_id=retail-events `
  --event_count=500 `
  --sleep_seconds=0.5
```

## Step 11: Build curated and KPI tables

After data starts landing in BigQuery:

1. Run [02_silver_orders_curated.sql](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/sql/02_silver_orders_curated.sql)
2. Run [03_gold_business_kpis.sql](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/sql/03_gold_business_kpis.sql)
3. Run [04_data_quality_checks.sql](/C:/Users/Srinivas%20Porandla/OneDrive/Documents/New%20project/sql/04_data_quality_checks.sql)

If Composer is configured, the DAG can run these steps automatically on schedule.

## What to say in an interview

If asked why you did not use Terraform, a strong answer is:

"I wanted the project to be easy to reproduce with native GCP tools, so I documented both a gcloud setup path and a manual console path. If this were moving toward team use or multi-environment deployment, I would convert the provisioning layer to Terraform or OpenTofu."
