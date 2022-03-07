variable "project_id" {
    description = "GCP Project ID"
    type = string
}

variable "provider_region" {
    description = "GCP Region"
    type = string
    default = "us-central1"
}

variable "topic_name" {
    description = "Name of Pub/Sub topic"
    default = "TerraformTopic"
    type = string
}

variable "topic_labels" {
    description = "Labels for Pub/Sub topic"
    default = {
        env = "dev"
    }
    type = map(string)
}

variable "allowed_persistence_regions" {
    description = "Regions where messages published to the Pub/Sub topic maybe stored"
    default = [ "us-central1"]
    type = list(string)
}