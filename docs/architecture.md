# Architecture Deep Dive

## Project summary

RetailPulse is a streaming retail analytics platform built to demonstrate practical GCP data engineering patterns. It ingests order lifecycle events in near real time, validates and processes them through Dataflow, stores them in BigQuery using bronze/silver/gold layers, and operationalizes downstream transformations with Cloud Composer.

## Functional requirements

- ingest order lifecycle events in near real time,
- support analyst-friendly curated tables,
- power live operational dashboards,
- isolate malformed records for replay,
- capture delivery and payment related business KPIs,
- provide an explainable architecture suitable for interview walkthroughs.

## Architecture decisions

### Pub/Sub for ingestion

Pub/Sub is used because it is fully managed, integrates natively with Dataflow, and supports decoupled producers and consumers. It also gives a simple way to simulate a real production event bus without adding Kafka operational overhead.

### Dataflow for streaming ETL

Dataflow is responsible for:

- reading messages continuously from Pub/Sub,
- parsing JSON payloads,
- validating required fields and event types,
- normalizing timestamps and payload structure,
- routing invalid records to a dead-letter path,
- deduplicating events within a time window,
- writing validated events into BigQuery bronze,
- emitting one-minute operational metrics for dashboards.

### BigQuery for storage and analytics

The BigQuery design follows a medallion pattern:

- bronze: validated raw events from the stream
- silver: order-level curated table with latest payment and shipment states
- gold: analytics-ready KPIs for stores, regions, and fulfilment performance
- ops: streaming metrics and data quality audit tables

This separation makes the project easier to explain and mirrors how many production data platforms are organized.

## End-to-end flow

1. A Python simulator generates realistic retail events.
2. Events are published to the `retail-events` Pub/Sub topic.
3. The Dataflow job consumes the stream and validates schema and business rules.
4. Invalid records are written to Cloud Storage for triage and replay.
5. Deduplicated valid events are written to `bronze.retail_events`.
6. One-minute operational metrics are written to `ops.real_time_order_metrics`.
7. A Composer DAG runs hourly SQL transformations to build `silver.orders_curated` and gold KPI tables.
8. Data quality checks are written to `ops.data_quality_audit`.
9. Looker Studio reads the gold and ops tables for dashboards.

## Table design

### bronze.retail_events

Purpose:
Store validated event-level data with minimal transformation.

Partitioning:
`event_date`

Clustering:
`event_type`, `store_id`, `region`

Why:
This improves query pruning and keeps streaming ingest simple.

### silver.orders_curated

Purpose:
Expose one row per order with current payment and shipment status.

Why:
Analysts and downstream dashboards often need order-level state, not raw event grain.

### gold.store_hourly_kpis

Purpose:
Provide hourly performance by store and region.

Metrics:

- total orders
- gross revenue
- average order value
- payment captured orders
- dispatch rate
- delivery SLA percentage

### gold.region_fulfilment_sla

Purpose:
Track regional delivery performance and identify bottlenecks.

## Data quality strategy

Validation happens in two stages:

### Stage 1: stream-time validation in Dataflow

- malformed JSON is rejected,
- required fields are checked,
- event type is validated,
- timestamp formatting is normalized,
- negative order amounts are rejected,
- empty item arrays for order creation are flagged.

### Stage 2: warehouse quality checks in BigQuery

- duplicate order IDs in silver,
- null business keys,
- revenue anomalies,
- delayed dispatch patterns,
- fulfilment SLA breaches.

Results are stored in `ops.data_quality_audit` for traceability.

## Reliability patterns

- Dead-letter path in Cloud Storage for replayable bad events
- Window-based deduplication using `event_id`
- Allowed lateness to accommodate delayed events
- Separate operational metrics table for fast BI access
- Repeatable provisioning through `gcloud` automation
- Manual console setup guide for interview demos and quick validation

## Performance and cost optimizations

- BigQuery partitioning and clustering on high-selectivity columns
- Streaming pipeline writes only validated rows to reduce warehouse noise
- Gold tables aggregate data for BI to avoid repeatedly scanning raw events
- One-minute metrics table supports dashboards without expensive ad hoc queries
- Composer orchestrates transformations on a schedule rather than continuously materializing every downstream view

## How to talk about scale

You can describe the project as if it is designed for:

- 50,000 to 200,000 retail events per hour,
- multiple stores and regions,
- burst traffic during campaign or sale windows,
- late-arriving events due to upstream system retries.

## Interview talking points

- Why you separated streaming ingestion from warehouse modeling
- How you handle duplicates and late-arriving data
- How you would replay dead-letter records
- How partitioning and clustering reduce BigQuery cost
- What you would monitor in Dataflow and Pub/Sub
- What changes you would make for production scale

## Production-grade improvements you can mention

- schema registry or protobuf/Avro contracts
- CI/CD for `gcloud` deployment steps and DAG release automation
- dbt for transformation testing and documentation
- Cloud Monitoring alerts for backlog, late data, and DQ failures
- row-level lineage with Data Catalog or Dataplex
- Flex Templates for repeatable Dataflow deployments
- Terraform or OpenTofu if the project later needs stronger multi-environment IaC
