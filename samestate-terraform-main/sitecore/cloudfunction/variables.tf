variable "project_id" {
    description = "Project ID of the GCP project where the resources will be created"
    type = string
}

variable "provider_region" {
    description = "Default GCP region for the resources"
    type = string
}

variable "function_name" {
    description = "name of the cloud function"
    type = string
    default = "adph_sitecore_function"
}

variable "function_description" {
    description = "Description of function"
    type = string
    default = "Created via Terraform"
}

variable "function_runtime" {
    description = "Runtime environmnet of the function"
    type = string
    default = "python37"
}

variable "function_memory" {
    description = "Available memory for the function"
    default = 128
}

variable "archive_bucket" {
    description = "Archive bucket where the code is stored"
    type = string
}

variable "archive_object" {
    description = "Zip file of the code"
    type = string
}

