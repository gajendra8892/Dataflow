import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv

class FormatForBigQuery(beam.DoFn):
    def process(self, element):
        # Convert CSV line (dict) to desired format
        yield {
            'employee_id': int(element['employee_id']),
            'name': element['name'],
            'email': element['email'],
            'department': element['department'],
            'designation': element['designation'],
            'salary': float(element['salary']),
            'joining_date': element['joining_date'],
            'location': element['location'],
            'experience': float(element['experience']),
            'manager_id': int(element['manager_id']),
        }

def run():
    options = PipelineOptions(
        project='dataflow-poc-462411',
        region='us-east1',
        runner='DataflowRunner',
        temp_location='gs://dataflow-batch/temp/',
        staging_location='gs://dataflow-batch/staging/',
        save_main_session=True,
    )

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadCSV' >> beam.io.ReadFromText('gs://dataflow-batch/data/employee_data_200.csv', skip_header_lines=1)
         | 'ParseCSV' >> beam.Map(lambda line: next(csv.DictReader([line], fieldnames=[
             'employee_id', 'name', 'email', 'department', 'designation',
             'salary', 'joining_date', 'location', 'experience', 'manager_id'
         ])))
         | 'FormatForBQ' >> beam.ParDo(FormatForBigQuery())
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
            'dataflow-poc-462411:employee.employee_table',
            schema='employee_id:INTEGER,name:STRING,email:STRING,department:STRING,designation:STRING,salary:FLOAT,joining_date:STRING,location:STRING,experience:FLOAT,manager_id:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         )
        )

if __name__ == '__main__':
    run()
