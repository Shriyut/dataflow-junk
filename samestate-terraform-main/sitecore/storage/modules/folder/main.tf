resource "google_storage_bucket" "bucket" {
  name = var.bucketname
  #location      = "US"
  location = var.bucketlocation
  #force_destroy = true
  force_destroy = var.force_destroy

  lifecycle_rule {
    condition {
      age = var.lifecycle_age_one
    }
    action {
      type = var.lifecycle_action_one
      storage_class = var.lifecycle_storage_class_one
    }
  }
  
  lifecycle_rule {
    condition {
      age = var.lifecycle_age_two
    }
    action {
      type = var.lifecycle_action_two
      storage_class = var.lifecycle_storage_class_two
    }
  }
  
  versioning {
    enabled = var.versioning_enabled 
  }
}

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
