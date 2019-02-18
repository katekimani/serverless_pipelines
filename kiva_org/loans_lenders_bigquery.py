from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import window

class ParseLoanLenderFn(beam.DoFn):
    """Parses the loans_lenders record to return the total lenders per loan.
    Each loan_lender entry has the format loan_id,lenders.
    e.g.
      483693,[muc888, niki3008, teresa9174]
    Returns: dictionary
    """
    def __init__(self):
        super(ParseLoanLenderFn, self).__init__()

    def process(self, element):
        try:
            row = element.split(',')

            yield { 'loan_id': row[0], 'lenders': row[1:] }
        except:
            logging.error('Parse error on "%s"', element[:40])

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

def run(argv=None):
    """Defines and runs the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        type=str,
                        required=True,
                        help='Path to file with raw data.'
                        )
    parser.add_argument('--output',
                        type=str,
                        required=True,
                        help='Path to write output data files to.'
                        )
    args, pipeline_args = parser.parse_known_args(argv)

    pipe_options = PipelineOptions(pipeline_args)

    # save_main_session option used because the DoFn's in workflow rely on global context
    pipe_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipe_options) as p:
        def format_output(lender_count):
            (loan, total_lenders) = lender_count

            return 'loan_id: %s total_lenders: %s' % (loan, total_lenders)

        # Determine lenders per loan
        lender_count_each_loan = ( p
                                    | 'Read Raw Data' >> beam.io.ReadFromText(file_pattern=args.input,skip_header_lines=1)
                                    | 'ParseData' >> beam.ParDo(ParseLoanLenderFn())
                                    | 'GetLenderCount' >> beam.Map(lambda x: (x['loan_id'], len(x['lenders'])))
                                    | 'FormatLenderCount' >> beam.Map(format_output)
                                    | 'WriteToBigQuery' >> WriteToBigQuery(
                                        args.table_name, args.dataset, {
                                        'loan_id':'STRING',
                                        'total_lenders': 'INTEGER',
              }, options.view_as(GoogleCloudOptions).project)
                                )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
