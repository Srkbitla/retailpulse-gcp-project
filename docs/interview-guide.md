# Interview Guide

## 60-second summary

I built a real-time retail analytics platform on GCP where order lifecycle events are streamed through Pub/Sub, processed by Dataflow, stored in BigQuery using bronze, silver, and gold layers, and orchestrated with Cloud Composer. The pipeline validates schema, handles duplicate and late-arriving events, writes live KPI tables for dashboards, and captures data quality audit results. I used `gcloud` automation and a documented manual setup path so the project is reproducible and easy to walk through in interviews.

## Common interview questions and strong answer angles

### Why did you choose Pub/Sub instead of Kafka?

For a GCP-native project, Pub/Sub reduces operational overhead and integrates directly with Dataflow. It was a practical choice for a cloud-first architecture focused on delivery speed and reliability rather than cluster management.

### What makes this project real time?

Events are ingested continuously through Pub/Sub, processed by a streaming Dataflow job, and written to operational BigQuery tables in one-minute windows. That gives near real-time visibility into orders and revenue without waiting for batch jobs.

### How do you handle duplicates?

Each event includes an `event_id`. In Dataflow, I key records by `event_id` and keep the first event inside the processing window before writing to BigQuery. In production, I would also align producer retry semantics and optionally add an idempotent merge layer downstream.

### How do you handle bad data?

Malformed or invalid records are routed to a Cloud Storage dead-letter path with the original payload and error details. That keeps the main pipeline healthy and makes replay and root-cause analysis possible.

### Why did you use gcloud and a manual setup guide instead of Terraform?

I wanted the project to be easy to reproduce with native GCP tooling and simple enough for interview demonstrations. The `gcloud` script gives automation, while the manual guide makes every resource and permission easy to explain. If the platform needed team-based multi-environment promotion, I would convert the provisioning layer to Terraform or OpenTofu.

### Why use bronze, silver, and gold layers?

The layers separate concerns. Bronze preserves validated event-level data, silver creates an order-level business entity, and gold provides dashboard-ready KPIs. It keeps transformations maintainable and easier for analysts to consume.

### How did you optimize BigQuery?

I partitioned large tables by event or run date, clustered by high-cardinality filter columns like `store_id`, `region`, and `event_type`, and created aggregated gold tables so dashboards do not repeatedly scan the bronze data.

### What would you monitor in production?

- Pub/Sub subscription backlog
- Dataflow job lag, errors, and throughput
- dead-letter volume trends
- BigQuery query cost and slot consumption
- DQ check failures
- delivery SLA drop by region

### What would you improve next?

- Move to Avro or protobuf contracts
- Package the pipeline as a Dataflow Flex Template
- Add CI/CD and integration tests
- Add dbt tests and docs for warehouse modeling
- Implement replay tooling for dead-letter files
- Convert provisioning to Terraform or OpenTofu for stronger multi-environment IaC

## Resume framing

Use verbs like:

- built
- designed
- deployed
- automated
- optimized
- orchestrated
- monitored

## Good metrics to mention after a demo run

- number of events processed
- median end-to-end latency
- BigQuery query cost reduction from partitioning
- dashboard freshness interval
- invalid event rate
- on-time delivery percentage

## Safe phrasing if you do not have production traffic

Say:

"Designed and implemented a production-style real-time data platform capable of processing simulated retail events with streaming ingestion, warehouse modeling, and automated data quality checks."

Avoid claiming:

- actual enterprise traffic volumes you never handled,
- cost savings you did not measure,
- production incident ownership if this was a personal project.
