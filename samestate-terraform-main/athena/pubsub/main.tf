provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "pubsub" {
    source = "./modules/pubsub"

    topic_name = var.topic_name
    topic_labels = var.topic_labels
    allowed_persistence_regions = var.allowed_persistence_regions
}
