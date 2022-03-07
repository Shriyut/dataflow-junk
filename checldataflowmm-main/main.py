import apache_beam as beam
import http.client
import json
import argparse

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import pubsub_v1
import logging

def a_simple_function(item):
    logging.info('--------Invoking a simple function')
    return item

def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    # parser.add_argument('--my-arg', help='description')
    args, beam_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as pipeline:
        test = (
            pipeline
            | 'Initaiting pipeline' >> beam.Create(['atqa-93-nc-cm.azurewebsites.net'])
            | 'Make API call & Perform CDC' >> beam.Map(lambda item: a_simple_function(item))
        )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
