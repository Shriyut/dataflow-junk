variable "project_id" {
    description = "Project id of the GCP project"
    default = "cloudglobaldelivery-1000135575"
}

variable "provider_region" {
    description = "GCP region for the resources beinf created through the script"
    default = "us-central1"
}

variable "dataset_id" {
    description = "Dataset ID for the BigQuery dataset"
    default = "a-dataset-id"
}

variable "friendly_name" {
    description = "Friendly name for the dataset being created"
    default = "dataset_friendly_name"
}

variable "dataset_description" {
    description = "Description for dataset"
    default = ""
}

variable "location" {
    description = "Geogrphic location where dataset would reside, can be regional or multi-regional"
    default = "US"
}

variable "default_table_expiration_ms" {
    default = null
}

variable "domain_name" {
        default = "macys.com"
}
variable "dataset_labels" {
    default = {
        env = "default"
    }
}

variable "delete_contents_on_destroy" {
    default = true
}

variable "access" {
    description = "Specifying dataset access for different entities"
    type = any

    default = [
        {
            role = "roles/bigquery.dataOwner"
            special_group = "projectOwners"
        }
    ]
}

variable "tables" {
    description = "A list of maps that includes table_id, schema, clustering, time_partitioning, expiration_time, labels in wach element."
    default = []
    type = list(object({
        table_id = string,
        schema = string,
        expiration_time = string,
        labels = map(string),
    }))
}

variable "abctablename" {
    description = "Name for abc table"
    default = "ABC_BQ_Table"
    type = string
}

variable "abcschema" {
    default = <<EOF
    [
    {
      "description": "The unique ID for every job",
      "mode": "NULLABLE",
      "name": "Run_ID",
      "type": "STRING"
    },
    {
      "description": "Timestamp at which the job starts",
      "mode": "NULLABLE",
      "name": "Job_Start_Time",
      "type": "DATE"
    },
    {
      "description": "Timestamp at which job ends",
      "mode": "NULLABLE",
      "name": "Job_End_Time",
      "type": "DATE"
    },
    {
      "description": "Type of Job RealTime/Batch",
      "mode": "NULLABLE",
      "name": "Job_Type",
      "type": "STRING"
    },
    {
      "description": "Name of the Job",
      "mode": "NULLABLE",
      "name": "Job_Name",
      "type": "STRING"
    },
    {
        "description": "Status of Job",
        "mode": "NULLABLE",
        "name": "Job_Status",
        "type": "STRING"
    },
    {
        "description": "Name of the GCP resource which is being executed",
        "mode": "NULLABLE",
        "name": "GCP_Component",
        "type": "STRING"
    },
    {
        "description": "Name of source system",
        "mode": "NULLABLE",
        "name": "Src_System_Name",
        "type": "STRING"
    },
    {
        "description": "Name of file received from source system",
        "mode": "NULLABLE",
        "name": "Src_File_Name",
        "type": "STRING"
    },
    {
        "description": "Name of target table",
        "mode": "NULLABLE",
        "name": "Tgt_Name",
        "type": "STRING"
    },
    {
        "description": "Total number of records from source system",
        "mode": "NULLABLE",
        "name": "Total_Records",
        "type": "INTEGER"
    },
    {
        "description": "Count of valid records",
        "mode": "NULLABLE",
        "name": "Valid_Records",
        "type": "INTEGER"
    },
    {
        "description": "Count of Invalid records",
        "mode": "NULLABLE",
        "name": "Invalid_Records",
        "type": "INTEGER"
    },
    {
        "description": "GCS path where all records in this job are archived",
        "mode": "NULLABLE",
        "name": "Archive_Path",
        "type": "STRING"
    }
]
EOF
}

variable "expiration_time" {
    description = "Expiration time for table"
    default = null
}

variable "errortablename" {
    description = "Name for error events table"
    default = "Error_Events_Table"
    type = string
}

variable "errorschema" {
    default = <<EOF
    [
    {
      "description": "The unique ID for every job",
      "mode": "NULLABLE",
      "name": "Run_ID",
      "type": "STRING"
    },
    {
      "description": "Unique ID of error event",
      "mode": "NULLABLE",
      "name": "Error_ID",
      "type": "STRING"
    },
    {
      "description": "Timestamp at which the error occured",
      "mode": "NULLABLE",
      "name": "Error_Event_Timestamp",
      "type": "DATE"
    },
    {
      "description": "GCP resource name which failed to execute",
      "mode": "NULLABLE",
      "name": "GCP_Component",
      "type": "STRING"
    },
    {
      "description": "Type of error",
      "mode": "NULLABLE",
      "name": "Error_Type",
      "type": "STRING"
    },
    {
        "description": "Description of error",
        "mode": "NULLABLE",
        "name": "Error_Description",
        "type": "STRING"
    },
    {
        "description": "Error message",
        "mode": "NULLABLE",
        "name": "Error_Message",
        "type": "STRING"
    }
]
EOF
}