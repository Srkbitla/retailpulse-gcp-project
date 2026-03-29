output "retail_events_topic" {
  description = "Pub/Sub topic for incoming retail events."
  value       = google_pubsub_topic.retail_events.id
}

output "retail_events_subscription" {
  description = "Pub/Sub subscription for the Dataflow consumer."
  value       = google_pubsub_subscription.retail_events_sub.id
}

output "raw_bucket" {
  description = "Cloud Storage bucket for dead-letter and archive files."
  value       = google_storage_bucket.raw_zone.name
}

output "dataflow_temp_bucket" {
  description = "Cloud Storage bucket for Dataflow temp and staging files."
  value       = google_storage_bucket.dataflow_temp.name
}

output "dataflow_service_account" {
  description = "Service account for the Dataflow worker pool."
  value       = google_service_account.dataflow_runner.email
}

