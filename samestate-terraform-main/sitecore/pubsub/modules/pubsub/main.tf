resource "google_pubsub_topic" "topic" {
    name = var.topic_name
    labels = var.topic_labels

    message_storage_policy {
        allowed_persistence_regions = var.allowed_persistence_regions
    }
}

resource "google_pubsub_subscription" "example" {
  name  = var.subscription_name
  topic = google_pubsub_topic.topic.name

  labels = {
    foo = "bar"
  }
    
  message_retention_duration = var.message_retention_duration
  
  retain_acked_messages = var.retain_acked_messages
    
  ack_deadline_seconds = var.ack_deadline_seconds

  expiration_policy {
    ttl = var.expiration_policy_ttl
  }
  retry_policy {
    minimum_backoff = var.retry_policy_minimum_backoff
    maximum_backoff = var.retry_policy_maximum_backoff
  }

  enable_message_ordering = var.enable_message_ordering
}
