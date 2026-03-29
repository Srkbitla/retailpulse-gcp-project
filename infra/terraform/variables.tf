variable "project_id" {
  description = "GCP project ID."
  type        = string
}

variable "region" {
  description = "Primary GCP region."
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "Primary GCP zone."
  type        = string
  default     = "us-central1-a"
}

variable "dataset_location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment label."
  type        = string
  default     = "dev"
}

variable "pubsub_topic_name" {
  description = "Pub/Sub topic name for retail events."
  type        = string
  default     = "retail-events"
}

variable "raw_bucket_name" {
  description = "Cloud Storage bucket for dead-letter and archive files."
  type        = string
  default     = ""
}

variable "dataflow_temp_bucket_name" {
  description = "Cloud Storage bucket for Dataflow temp and staging files."
  type        = string
  default     = ""
}

variable "owner" {
  description = "Owner label for resources."
  type        = string
  default     = "candidate"
}

