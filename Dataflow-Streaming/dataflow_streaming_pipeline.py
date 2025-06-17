import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

PROJECT_ID = 'dataflow-poc-462411'
SUBSCRIPTION = 'projects/dataflow-poc-462411/subscriptions/crypto-stream-sub'
table='dataflow-poc-462411:crypto_dataset.crypto_trades'

class ParseBinanceMessage(beam.DoFn):
    def process(self, element):
        record = json.loads(element.decode('utf-8'))
        yield {
            'event_time': record.get('E'),
            'symbol': record.get('s'),
            'price': float(record.get('p')),
            'quantity': float(record.get('q'))
        }

def run():
    options = PipelineOptions(
        streaming=True,
        project=PROJECT_ID,
        region='us-east1',
        temp_location='gs://crypto-stream-bucket-9/temp',
        runner='DataflowRunner'
    )
    p = beam.Pipeline(options=options)

    (p
     | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
     | 'ParseJson' >> beam.ParDo(ParseBinanceMessage())
     | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
        table,
        schema='event_time:INTEGER,symbol:STRING,price:FLOAT,quantity:FLOAT',
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
     )
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
