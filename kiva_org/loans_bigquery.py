from __future__ import absolute_import

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

class ParseLoanRecordFn(beam.DoFn):
    """Parses the raw loan record into a Python tuple.
       Each record has the following format:
        loan_id,loan_name,original_language,description,funded_amount,loan_amount,status,image_id,video_id,activity_name,sector_name,loan_use
        ...
    """
    def __init__(self):
        super(ParseLoanRecordFn, self).__init__()
        self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

    def process(self, elem):
        try:
            row = list(csv.reader([elem]))[0]

            yield row[0], float(row[5]), float(row[6]), row[7]
        except:
            # Log and count parse errors
            self.num_parse_errors.inc()
            logging.error('Parse error on "%s"', elem[:40])

class LoanRecordsDict(beam.DoFn):
    """Format the loan data to a dictionary of BigQuery columns with their values.

       Receives a tuple of the sanitized loans and formats it to a dictionary in
       the format {'bigquery_column': value}
    """
    def process(self, loan_record):
        (loan, amount, funded, status) = loan_record
        yield {
              'loan_id': loan,
              'funded_amount': amount,
              'loan_amount': funded,
              'status': status
            }

class WriteToBigQuery(beam.PTransform):
    """Generate, format and write BigQuery table row."""
    def __init__(self, table_name, dataset, schema, project):
        """Initializes the transform.
           Args:
             table_name: Name of the BigQuery table to use.
             dataset: Name of dataset.
             schema: Dictionary in the format {'column_name': 'bigquery_type'}
             project: Name of the GCP project to use.
        """
        super(WriteToBigQuery, self).__init__()
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """Build the output table schema."""
        return ', '.join(
          '%s:%s' % (col, self.schema[col]) for col in self.schema
        )

    def expand(self, pcoll):
        return (
          pcoll
          | 'ConvertToRow' >> beam.Map(lambda elem: {col: elem[col] for col in self.schema})
          | 'WriteToBQ' >> beam.io.WriteToBigQuery(self.table_name, self.dataset, self.project, self.get_schema())
        )

class SanitizeLoanData(beam.PTransform):
    def expand(self, pcoll):
        return (
          pcoll
          | 'ParseLoanRecordFn' >> beam.ParDo(ParseLoanRecordFn())
        )

def run(argv=None):
    """Defines and runs the loans pipeline."""
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        type=str,
                        default='/Users/ke-fvf2362hv2h/ownspace/workshops/data/kiva/loans_mini.csv',
                        help='Path to the data file(s) containing loan data.')
    parser.add_argument('--output',
                        type=str,
                        default='tmp/loans_bq',
                        help='Path to the output file(s).')
    parser.add_argument('--dataset',
                        type=str,
                        required=True,
                        help='BigQuery dataset')
    parser.add_argument('--table_name',
                        type=str,
                        required=True,
                        help='The table to be used')

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (p
         | 'ReadInputText' >> beam.io.ReadFromText(file_pattern=args.input,skip_header_lines=1)
         | 'SanitizeLoanData' >> SanitizeLoanData()
         | 'FormDict' >> beam.ParDo(LoanRecordsDict())
         | 'WriteToBigQuery' >> WriteToBigQuery(
              args.table_name, args.dataset, {
                'loan_id':'STRING',
                'funded_amount': 'FLOAT',
                'loan_amount': 'FLOAT',
                'status': 'STRING',
              }, options.view_as(GoogleCloudOptions).project)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()