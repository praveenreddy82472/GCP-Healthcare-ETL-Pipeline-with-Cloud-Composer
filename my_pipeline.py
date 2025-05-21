import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.options.pipeline_options import ValueProvider
from datetime import datetime

# Define custom pipeline options class for input/output parameters
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', type=str, help='Input file path')
        parser.add_value_provider_argument('--output', type=str, help='BigQuery table spec')

def run():
    # Parse known args first (project, region, staging, temp, runner, templateLocation)
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', required=True, help='Google Cloud project ID')
    parser.add_argument('--region', required=True, help='Google Cloud region')
    parser.add_argument('--stagingLocation', required=True, help='GCS staging location')
    parser.add_argument('--tempLocation', required=True, help='GCS temp location')
    parser.add_argument('--runner', required=True, help='Apache Beam runner')
    parser.add_argument('--templateLocation', required=False, help='GCS path to write Dataflow template')

    known_args, pipeline_args = parser.parse_known_args()

    # Add mandatory Beam options
    pipeline_options = PipelineOptions(pipeline_args)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = known_args.project
    google_cloud_options.region = known_args.region
    google_cloud_options.staging_location = known_args.stagingLocation
    google_cloud_options.temp_location = known_args.tempLocation

    if known_args.templateLocation:
        google_cloud_options.template_location = known_args.templateLocation
        pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    else:
        pipeline_options.view_as(StandardOptions).runner = known_args.runner

    # Enable setup.py dependencies if any (optional)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Add custom options (input/output) to pipeline_options
    pipeline_options.view_as(MyOptions)  # This just ensures options class is registered

    # Access the custom options
    my_options = pipeline_options.view_as(MyOptions)

    # Your BigQuery table schema
    table_schema = {
        "fields": [
            {"name": "Name", "type": "STRING"},
            {"name": "Age", "type": "INTEGER"},
            {"name": "Gender", "type": "STRING"},
            {"name": "Blood Type", "type": "STRING"},
            {"name": "Medical Condition", "type": "STRING"},
            {"name": "Date of Admission", "type": "DATE"},
            {"name": "Doctor", "type": "STRING"},
            {"name": "Hospital", "type": "STRING"},
            {"name": "Insurance Provider", "type": "STRING"},
            {"name": "Billing Amount", "type": "FLOAT"},
            {"name": "Room Number", "type": "INTEGER"},
            {"name": "Admission Type", "type": "STRING"},
            {"name": "Discharge Date", "type": "DATE"},
            {"name": "Medication", "type": "STRING"},
            {"name": "Test Results", "type": "STRING"}
        ]
    }

    def parse_csv(line):
        values = line.split(',')
        if len(values) != 15:
            logging.warning(f"Skipping row with incorrect number of values: {line}")
            return None
        try:
            return {
                "Name": values[0],
                "Age": int(values[1]),
                "Gender": values[2],
                "Blood Type": values[3],
                "Medical Condition": values[4],
                "Date of Admission": datetime.strptime(values[5], '%Y-%m-%d').strftime('%Y-%m-%d'),
                "Doctor": values[6],
                "Hospital": values[7],
                "Insurance Provider": values[8],
                "Billing Amount": float(values[9]),
                "Room Number": int(values[10]),
                "Admission Type": values[11],
                "Discharge Date": datetime.strptime(values[12], '%Y-%m-%d').strftime('%Y-%m-%d'),
                "Medication": values[13],
                "Test Results": values[14]
            }
        except Exception as e:
            logging.error(f"Error parsing row: {line}, Error: {e}")
            return None

    with beam.Pipeline(options=pipeline_options) as p:
        # Use ValueProvider.get() to access input/output values at runtime
        input_path = my_options.input.get()
        output_table = my_options.output.get()

        # Validate these are not None before pipeline execution
        if not input_path or not output_table:
            raise ValueError("Input and Output parameters must be provided at runtime")

        (p
         | 'ReadFromGCS' >> beam.io.ReadFromText(input_path, skip_header_lines=1)
         | 'ParseCSV' >> beam.Map(parse_csv)
         | 'FilterValidRows' >> beam.Filter(lambda row: row is not None)
         | 'WriteToBQ' >> beam.io.WriteToBigQuery(
                output_table,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

    logging.getLogger().setLevel(logging.INFO)
    logging.info("Pipeline submitted.")

if __name__ == '__main__':
    run()
