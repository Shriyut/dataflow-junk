provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "folder" {
    source = "./modules/folder"

    archive_folder_name = var.archive_folder_name
    archive_folder_content = var.archive_folder_content
    bucketname = var.bucketname

    log_folder_name = var.log_folder_name
    log_folder_content = var.log_folder_content

    temp_folder_name = var.temp_folder_name
    temp_folder_content = var.temp_folder_content

    staging_folder_name = var.staging_folder_name
    staging_folder_content = var.staging_folder_content
}
