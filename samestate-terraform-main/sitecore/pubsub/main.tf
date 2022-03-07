provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "pubsub" {
    source = "./modules/pubsub"

    topic_name = var.topic_name
    topic_labels = var.topic_labels
    allowed_persistence_regions = var.allowed_persistence_regions
    
    subscription_name = var.subscription_name
    message_retention_duration = var.message_retention_duration
    retain_acked_messages = var.retain_acked_messages
    expiration_policy_ttl = var.expiration_policy_ttl
    retry_policy_minimum_backoff = var.retry_policy_minimum_backoff
    retry_policy_maximum_backoff = var.retry_policy_maximum_backoff
    enable_message_ordering = var.enable_message_ordering
    ack_deadline_seconds = var.ack_deadline_seconds
}
