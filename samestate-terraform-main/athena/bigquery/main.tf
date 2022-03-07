provider "google" {
    project = var.project_id
    region = var.provider_region
}

module "bq" {
    source = "./modules/bq"
    dataset_id = var.dataset_id
    friendly_name = var.friendly_name
    dataset_description = var.dataset_description
    location = var.location
    default_table_expiration_ms = var.default_table_expiration_ms
    project_id = var.project_id
    dataset_labels = var.dataset_labels
    
    access = var.access
    delete_contents_on_destroy = var.delete_contents_on_destroy

    tables = var.tables
    abctablename = var.abctablename
    errortablename = var.errortablename
    expiration_time = var.expiration_time
    abcschema = var.abcschema
    errorschema = var.errorschema

}
