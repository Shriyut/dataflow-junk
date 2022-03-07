resource "google_pubsub_topic" "topic" {
    name = var.topic_name
    labels = var.topic_labels

    message_storage_policy {
        allowed_persistence_regions = var.allowed_persistence_regions
    }
}