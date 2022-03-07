provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "folder" {
    source = "./modules/folder"

    bucketname = var.bucketname
    archive_folder_name = var.archive_folder_name
    archive_folder_content = var.archive_folder_content
    

    log_folder_name = var.log_folder_name
    log_folder_content = var.log_folder_content

    temp_folder_name = var.temp_folder_name
    temp_folder_content = var.temp_folder_content

    staging_folder_name = var.staging_folder_name
    staging_folder_content = var.staging_folder_content
    
    bucketlocation = var.bucketlocation
    force_destroy = var.force_destroy
    versioning_enabled = var.versioning_enabled
    
    lifecycle_age_one = var.lifecycle_age_one
    lifecycle_action_one = var.lifecycle_action_one
    lifecycle_storage_class_one = var.lifecycle_storage_class_one
    lifecycle_age_two = var.lifecycle_age_two
    lifecycle_action_two = var.lifecycle_action_two
    lifecycle_storage_class_two = var.lifecycle_storage_class_two
}
