project_id = "testproject123456789-313307"
provider_region = "us-central1"
dataset_id = "bq_mdm_dataset_athena"
friendly_name = "testdataset"
dataset_description = "Created via Terraform"
location = "US"
default_table_expiration_ms = 3600000
domain_name = "deloitte.com"
delete_contents_on_destroy = true
tables = [
    {
        table_id = "DQ_VALIDATION_ERRORS_Athena",
        schema = "dqschema.json",
        expiration_time = null,
        labels = {
            env = "dev"
        },
    }
]

abctablename = "ABC_BQ_Table_Athena"
errortablename = "Error_Events_Table_Athena"
expiration_time = null

