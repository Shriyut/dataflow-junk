variable "archive_folder_name" {
    description = "Name of archive folder"
    default = "apdh_Archive_folder/"
    type = string
}

variable "archive_folder_content" {
    description = "Content for archive folder"
    default = "Sample content for archive folder"
    type = string
}

variable "bucketname" {
    description = "Bucketname"
    type = string
}

variable "log_folder_name" {
    description = "Name for logs folder"
    default = "apdh_log_folder/"
    type = string
}

variable "log_folder_content" {
    description = "Content for logs folder"
    default = "apdh_log_folder/"
    type = string
}

variable "temp_folder_name" {
    description = "Temp folder for dataflow"
    default = "apdh_temp_folder/"
    type = string
}

variable "temp_folder_content" {
    description = "Sample content for temp folder"
    default = "Sample content"
    type = string
}

variable "staging_folder_name" {
    description = "Name for staging folder for dataflow jobs"
    default = "apdh_Staging_folder/"
    type = string
}

variable "staging_folder_content" {
    description = "Sample content for staging folder"
    default = "Sample content"
    type = string
}