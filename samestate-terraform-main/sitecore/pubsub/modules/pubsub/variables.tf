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

variable "subscription_name" {
    description = "name of pubsub subscription"
    type = string
    default = "SITECORE_INGEST_PUSHDATAFLOW"
}

variable "message_retention_duration" {
    description = "Message retention duration for subscriber"
    default = "1200s"
    type = string
}

variable "retain_acked_messages" {
    description = "Retain acked messages for subscriber"
    default = true
}

variable "expiration_policy_ttl" {
    description = "Expiration policy ttl for subscriber"
    type = string
    default = "300000.5s"
}

variable "retry_policy_minimum_backoff" {
    description = "Minimum backoff for retry policy"
    type = string
    default = "10s"
}

variable "retry_policy_maximum_backoff" {
    description = "Maximum backoff for retry policy"
    type = string
    default = "600s"
}

variable "enable_message_ordering" {
    description = "Option to enable message ordering for subscriber"
    default = false
}

variable "ack_deadline_seconds" {}
