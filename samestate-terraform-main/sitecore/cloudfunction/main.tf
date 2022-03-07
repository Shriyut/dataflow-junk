provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "function" {
    source = "./modules/function"

    function_name = var.function_name
    function_description = var.function_description
    function_runtime = var.function_runtime
    function_memory = var.function_memory
    archive_bucket = var.archive_bucket
    archive_object = var.archive_object
}