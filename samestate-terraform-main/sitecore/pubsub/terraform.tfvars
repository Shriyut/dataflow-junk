project_id = "testproject123456789-313307"
provider_region = "us-central1"

topic_name = "SITECORE_DELIVER"
topic_labels = {
    env = "dev"
    }

allowed_persistence_regions = ["us-central1"]

subscription_name = "SITECORE_DELIVER_PUSHDATAFLOW"
message_retention_duration = "604800s"
retain_acked_messages = false
expiration_policy_ttl = "2678400s"
retry_policy_minimum_backoff = "10s"
retry_policy_maximum_backoff = "600s"
enable_message_ordering = false

ack_deadline_seconds = 20
