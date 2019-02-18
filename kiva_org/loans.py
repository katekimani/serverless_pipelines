from __future__ import absolute_import

import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.metrics.metric import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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
                      required=True,
                      help='Path to the data file(s) containing loan data.')
    parser.add_argument('--output',
                      type=str,
                      required=True,
                      help='Path to the output file(s).')

    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        def format_loan_records(loan_record):
            (loan, amount, funded, status) = loan_record
            return 'loan: %s, amount: %s, funded: %s, status: %s' % (loan, amount, funded, status)

        (p
         | 'ReadInputText' >> beam.io.ReadFromText(file_pattern=args.input,skip_header_lines=1)
         | 'SanitizeLoanData' >> beam.ParDo(ParseLoanRecordFn())
         | 'FormatLoanData' >> beam.Map(format_loan_records)
         | 'WriteSanitizedRecords' >> beam.io.WriteToText(args.output)
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()