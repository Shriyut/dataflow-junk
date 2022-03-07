from google.cloud import bigquery

SERVICE_ACCOUNT_JSON=r'C:\Users\shriy\Desktop\testproject_owner_serviceaccount.json'
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
#client = bigquery.Client()

dataset_id = "testproject123456789-313307.python_dataset"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "US"

dataset.description = "Dataset created via Python"

dataset = client.create_dataset(dataset, timeout=30)
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
