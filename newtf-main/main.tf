provider "google" {
    project = "testproject123456789-313307"
    region = "us-central1"
}

resource "google_cloudfunctions_function" "function" {
  name        = "testfunction1"
  description = "My function"
  runtime     = "python37"

  available_memory_mb   = 256
  source_archive_bucket = "demo_bucket123456"
  source_archive_object = "python.zip"
  trigger_http          = true
  entry_point           = "hello_world"
}

resource "google_cloudfunctions_function" "functionnew" {
  name        = "testfunction2"
  description = "My function"
  runtime     = "python37"

  available_memory_mb   = 256
  source_archive_bucket = "demo_bucket123456"
  source_archive_object = "python.zip"
  trigger_http          = true
  entry_point           = "hello_world"
}
