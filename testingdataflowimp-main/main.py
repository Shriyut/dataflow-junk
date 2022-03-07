import apache_beam as beam
import http.client
import json
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import pubsub_v1
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


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

    def find_delta(self, client, prev_table_id, current_table_id):
        # run query to find delta records, figure out pushing to pubsub after that
        project_id = 'testproject123456789-313307'
        topic_id = 'messages'
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_id)
        query = "SELECT to_json_string(t1) FROM `current_place_holder` t1 EXCEPT DISTINCT ( SELECT to_json_string(t2) FROM `prev_place_holder` t2)"
        query1 = query.replace('prev_place_holder', prev_table_id)
        query2 = query1.replace('current_place_holder', current_table_id)
        delta_job_config = bigquery.QueryJobConfig(
            #flatten_results=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        #try pushing to pubsub in a separate program and test the query with nested -> done
        print ("running query job")
        query_job = client.query(query2, job_config=delta_job_config)
        #query_job.result()
        for row in query_job:
            x = row[0]
            pubsub_data = x.encode("utf-8")
            publisher.publish(topic_path, pubsub_data)        

    def performCDC(self):
        
        #SERVICE_ACCOUNT_JSON = r'C:\Users\shrijha\Desktop\testproject_credentials.json'
        
        storage_client = storage.Client()
        bigquery_client = bigquery.Client()

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

        blob = bucket.blob(r'./linuxtestgs.json')
        blob.upload_from_filename(r'./linuxtestgs.json')
        bucket.rename_blob(blob, "linuxtestgs.json")

        load_job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
        uri = "gs://demo_bucket123456/linuxtestgs.json"
        load_job = bigquery_client.load_table_from_uri(
            uri, current_table_id, job_config=load_job_config
        ) 
        load_job.result()

        self.find_delta(bigquery_client, prev_table_id, current_table_id)
        

    def process(self, text):
        import apache_beam as beam
        import http.client
        import json
        
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
                    file = open(r"./linuxtesting.json", "w")
                    file.write(json.dumps(new_test, indent=4))
                    file.close()
                with open(r"./linuxtesting.json", "r") as read_obj:
                    sample_data = json.load(read_obj)
                result = [json.dumps(record) for record in sample_data]
                with open(r"./linuxtestgs.json", "w") as write_obj:
                    for i in result:
                        write_obj.write(i+'\n')
        self.performCDC()

def run(save_main_session=True):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as pipeline:
        test = (
            pipeline
            | 'Initaiting pipeline' >> beam.Create(['at-sitecore-qa-93-nc-cm.azurewebsites.net'])
            | 'Make API call & Perform CDC' >> beam.ParDo(makeAPICall(','))
        )

if __name__ == "__main__":
    run()
