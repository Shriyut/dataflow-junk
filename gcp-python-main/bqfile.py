from google.cloud import bigquery

SERVICE_ACCOUNT_JSON = r'C:\Users\shrijha\Desktop\testproject_credentials.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
filename = r'C:\Users\shrijha\Desktop\site.json'
dataset_id = r'beam_samples'
table_id = r'testing'

dataset_ref = client.dataset(dataset_id)
table_ref = dataset_ref.table(table_id)

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.autodetect = True

with open(filename, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        location="US",
        job_config=job_config,
    )

job.result()
