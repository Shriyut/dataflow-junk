from google.cloud import bigquery

SERVICE_ACCOUNT_JSON=r'C:\Users\shriy\Desktop\testproject_owner_serviceaccount.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

table_id = "testproject123456789-313307.python_dataset.python_table"

schema = [
    bigquery.SchemaField("Run_ID", "STRING", description="The Unique ID for every job run"),
    bigquery.SchemaField("Dataflow_Name", "STRING", description="Dataflow and pipeline name"),
    bigquery.SchemaField("Resource_Name", "STRING", description="Source System Name"),
    bigquery.SchemaField("Table_Name", "STRING", description="Name of the table"),
    bigquery.SchemaField("Attribute_Name", "STRING", description="Name of attribute which has wrong data"),
    bigquery.SchemaField("Error_Type", "STRING", description="Error Category like NULL Error, format etc"),
    bigquery.SchemaField("Error_Description", "STRING", description="Error Description and data"),
    bigquery.SchemaField("Created_By", "STRING", description="Audit Column"),
    bigquery.SchemaField("Start_Date_Time", "DateTime", description="Audit Column"),
    bigquery.SchemaField("End_Date_Time", "DateTime", description="Audit Column"),
]

table = bigquery.Table(table_id, schema=schema)
table = client.create_table(table)
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)
