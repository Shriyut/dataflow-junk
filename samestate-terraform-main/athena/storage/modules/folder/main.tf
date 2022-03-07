resource "google_storage_bucket_object" "archive_folder" {
  name = var.archive_folder_name
  content = var.archive_folder_content
  bucket = var.bucketname
}

resource "google_storage_bucket_object" "log_folder" {
    name = var.log_folder_name
    content = var.log_folder_content
    bucket = var.bucketname
}

resource "google_storage_bucket_object" "temp_folder" {
    name = var.temp_folder_name
    content = var.temp_folder_content
    bucket = var.bucketname
}

resource "google_storage_bucket_object" "staging_folder" {
    name = var.staging_folder_name
    content = var.staging_folder_content
    bucket = var.bucketname
}