project_id = "testproject123456789-313307"
provider_region = "us-central1"
bucketname = "34469-apdh-dev-sitecore-ingest-archive-us_1"

archive_folder_name = "archive/"
archive_folder_content = "Content"


log_folder_name = "logs/"
log_folder_content = "Content"

temp_folder_name = "temp/"
temp_folder_content = "Content"

staging_folder_name = "staging/"
staging_folder_content = "Content"

bucketlocation = "US"
force_destroy = true
versioning_enabled = true

lifecycle_age_one = 30
lifecycle_action_one = "SetStorageClass"
lifecycle_storage_class_one = "COLDLINE"

lifecycle_age_two = 90
lifecycle_action_two = "SetStorageClass"
lifecycle_storage_class_two = "ARCHIVE"

