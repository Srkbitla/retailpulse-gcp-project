locals {
  labels = {
    system      = "retailpulse"
    environment = var.environment
    owner       = var.owner
  }

  raw_bucket_name  = var.raw_bucket_name != "" ? var.raw_bucket_name : lower("${var.project_id}-retailpulse-raw")
  temp_bucket_name = var.dataflow_temp_bucket_name != "" ? var.dataflow_temp_bucket_name : lower("${var.project_id}-retailpulse-temp")
}

resource "google_project_service" "required_services" {
  for_each = toset([
    "bigquery.googleapis.com",
    "composer.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com"
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "raw_zone" {
  name                        = local.raw_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels

  depends_on = [google_project_service.required_services]
}

resource "google_storage_bucket" "dataflow_temp" {
  name                        = local.temp_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
  labels                      = local.labels

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = 14
    }
  }

  depends_on = [google_project_service.required_services]
}

resource "google_pubsub_topic" "retail_events" {
  name   = var.pubsub_topic_name
  labels = local.labels

  depends_on = [google_project_service.required_services]
}

resource "google_pubsub_topic" "retail_events_dead_letter" {
  name   = "${var.pubsub_topic_name}-dlq"
  labels = local.labels

  depends_on = [google_project_service.required_services]
}

resource "google_pubsub_subscription" "retail_events_sub" {
  name  = "${var.pubsub_topic_name}-sub"
  topic = google_pubsub_topic.retail_events.name

  ack_deadline_seconds = 60
  labels               = local.labels

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.retail_events_dead_letter.id
    max_delivery_attempts = 10
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "300s"
  }
}

resource "google_service_account" "dataflow_runner" {
  account_id   = "retailpulse-dataflow"
  display_name = "RetailPulse Dataflow Runner"
}

resource "google_project_iam_member" "dataflow_worker" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/dataflow.worker",
    "roles/pubsub.subscriber",
    "roles/storage.objectAdmin",
    "roles/viewer"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_runner.email}"
}

resource "google_bigquery_dataset" "bronze" {
  dataset_id                 = "bronze"
  friendly_name              = "RetailPulse Bronze Dataset"
  description                = "Streaming validated retail events."
  location                   = var.dataset_location
  delete_contents_on_destroy = true
  labels                     = local.labels
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                 = "silver"
  friendly_name              = "RetailPulse Silver Dataset"
  description                = "Curated order-level retail tables."
  location                   = var.dataset_location
  delete_contents_on_destroy = true
  labels                     = local.labels
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                 = "gold"
  friendly_name              = "RetailPulse Gold Dataset"
  description                = "Analytics-ready business KPI tables."
  location                   = var.dataset_location
  delete_contents_on_destroy = true
  labels                     = local.labels
}

resource "google_bigquery_dataset" "ops" {
  dataset_id                 = "ops"
  friendly_name              = "RetailPulse Ops Dataset"
  description                = "Operational metrics and DQ audit tables."
  location                   = var.dataset_location
  delete_contents_on_destroy = true
  labels                     = local.labels
}

resource "google_bigquery_table" "raw_events" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "retail_events"
  deletion_protection = false
  schema              = file("${path.module}/schemas/raw_events.json")
  clustering          = ["event_type", "store_id", "region"]

  time_partitioning {
    type  = "DAY"
    field = "event_date"
  }
}

resource "google_bigquery_table" "realtime_metrics" {
  dataset_id          = google_bigquery_dataset.ops.dataset_id
  table_id            = "real_time_order_metrics"
  deletion_protection = false
  schema              = file("${path.module}/schemas/realtime_metrics.json")
  clustering          = ["store_id", "region"]

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }
}

resource "google_bigquery_table" "data_quality_audit" {
  dataset_id          = google_bigquery_dataset.ops.dataset_id
  table_id            = "data_quality_audit"
  deletion_protection = false
  schema              = file("${path.module}/schemas/data_quality_audit.json")
  clustering          = ["check_name", "status"]

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }
}

