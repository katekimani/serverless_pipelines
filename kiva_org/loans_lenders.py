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
                                    | 'SaveResultText' >> beam.io.WriteToText(args.output)
                                )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
