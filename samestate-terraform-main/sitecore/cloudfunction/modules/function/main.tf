resource "google_cloudfunctions_function" "function" {
  name        = var.function_name
  description = var.function_description
  runtime     = var.function_runtime

  available_memory_mb   = var.function_memory
  source_archive_bucket = var.archive_bucket
  source_archive_object = var.archive_object
  trigger_http          = true
  entry_point           = "hello_world"
}
