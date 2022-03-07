import apache_beam as beam
import http.client
import json
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


class makeAPICall(beam.DoFn):
    def __init__(self, delimiter=','):
        self.delimiter = delimiter

    def replaceWords(self, input_dict):

        t = "true"
        n = "null"
        f = "false"

        str_json = json.dumps(input_dict, indent=4)
        str1 = str_json.replace(t, '\"true\"')
        str2 = str1.replace(n, '\"null\"')
        str3 = str2.replace(f, '\"false\"')
        output_dict = json.loads(str3)

        return output_dict

    def check_table(self, client, table_ref):
        try:
            client.get_table(table_ref)
            return True
        except NotFound:
            return False


    def performCDC(self):
        
        SERVICE_ACCOUNT_JSON = r'C:\Users\shrijha\Desktop\testproject_credentials.json'
        storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)
        bigquery_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

        prev_table_id = "testproject123456789-313307.beam_samples.prev_table"
        current_table_id = "testproject123456789-313307.beam_samples.test_table"

        #Loading content from current bq table to previous bq table
        #x = self.check_table(bigquery_client, prev_table_id)
        #if x == True:
            #delete table -> due to write_disposition not being covered -> need to check this, for now deleting table beforehand works
        #    bigquery_client.delete_table(prev_table_id)
        #    copy_job = bigquery_client.copy_table(current_table_id, prev_table_id)
        #    copy_job.result()
        #else:
        #    copy_job = bigquery_client.copy_table(current_table_id, prev_table_id)
        #    copy_job.result()

        copy_job_config = bigquery.QueryJobConfig(
        destination=prev_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        sql = """
            SELECT * FROM `testproject123456789-313307.beam_samples.test_table`
        """

        query_job = bigquery_client.query(sql, job_config=copy_job_config)
        query_job.result()

        bucket = storage_client.get_bucket("demo_bucket123456")

        blob = bucket.blob(r'C:\Users\shrijha\Desktop\testgs.json')
        blob.upload_from_filename(r'C:\Users\shrijha\Desktop\testgs.json')
        bucket.rename_blob(blob, "testgs.json")

        load_job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
        uri = "gs://demo_bucket123456/testgs.json"
        load_job = bigquery_client.load_table_from_uri(
            uri, current_table_id, job_config=load_job_config
        ) 
        load_job.result()
        

    def process(self, text):
        for url in text.split(self.delimiter):
            conn = http.client.HTTPSConnection(url)
            payload = "{\"facilityName\": \"\",\"filters\": {\"locationType\": [\"\"]},\"geoDistanceOptions\": {\"location\": \"Indianapolis, IN\",\"radius\": 5500},\"locationType\": \"\",\"page\": 1,\"pageSize\": 6000,\"stateCode\": \"\"}"
            headers = {
            'Content-Type': 'text/plain'
            }

            conn.request("POST", "/api/locations/search", payload, headers)
            res = conn.getresponse()
            print(res.status)

            data = res.read()
            decoded = data.decode("utf-8")
            json_data = json.loads(decoded)
            
            for key in json_data.keys():
                if key == "Results":
                    test_data = json_data.get("Results")
                    new_test = self.replaceWords(test_data)
                test_data_df = pd.DataFrame(new_test)
                test_data_df.to_json(r'C:\Users\shrijha\Desktop\testgs.json',orient="table")

        self.performCDC()


with beam.Pipeline() as pipeline:
    test = (
        pipeline
        | 'Initaiting pipeline' >> beam.Create(['at-sample-url.azs.net'])
        | 'Make API call & Perform CDC' >> beam.ParDo(makeAPICall(','))
    )
