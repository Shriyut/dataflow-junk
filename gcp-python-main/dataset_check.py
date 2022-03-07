from google.cloud import bigquery
from google.cloud.exceptions import NotFound

SERVICE_ACCOUNT_JSON = r'C:\Users\shrijha\Desktop\testproject_credentials.json'

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

dataset_id = "testproject123456789-313307.beam_samples"

try:
    client.get_dataset(dataset_id)
    print ("Dataset exists")
except NotFound:
    print ("Dataset doesn't exist")
