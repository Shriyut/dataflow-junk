from google.cloud import bigquery

SERVICE_ACCOUNT_JSON = r'C:\Users\shrijha\Desktop\testproject_credentials.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

table_id = "testproject123456789-313307.beam_samples.test_table"

job_config = bigquery.LoadJobConfig(
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
uri = "gs://bqtest987654321/site.json"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
) 
load_job.result()
destination_table = client.get_table(table_id)
