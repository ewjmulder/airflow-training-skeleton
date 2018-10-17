from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.examples.cookbook.coders import JsonCoder
from apache_beam.io import ReadFromText, BigQueryDisposition
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        help="Input file to process.",
    )
    parser.add_argument(
        "--table",
        dest="table",
        help="Target table.",
    )
    parser.add_argument(
        "--dataset",
        dest="dataset",
        help="Target dataset.",
    )
    parser.add_argument(
        "--project",
        dest="project",
        help="Google cloud project.",
    )
    parser.add_argument(
        "--bucket",
        dest="bucket",
        help="Temp bucket.",
    )
    parser.add_argument(
        "--name",
        dest="name",
        help="Job name.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend(
        [
            "--runner=DataflowRunner",
            "--project=" + known_args.project,
            "--staging_location=gs://" + known_args.bucket + "/dataflow-staging",
            "--temp_location=gs://" + known_args.bucket + "/dataflow-temp",
            "--job_name=" + known_args.name,
        ]
    )

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromGCS" >> ReadFromText(known_args.input, coder=JsonCoder())
            | WriteToBigQuery(
                known_args.table,
                dataset=known_args.dataset,
                project=known_args.project,
                schema="city:string, "
                       "county:string, "
                       "district:string, "
                       "duration:string, "
                       "locality:string, "
                       "newly_built:boolean, "
                       "paon:string, "
                       "postcode:string, "
                       "ppd_category_type:string, "
                       "price:numeric, "
                       "property_type:string, "
                       "record_status:string, "
                       "saon:string, "
                       "street:string, "
                       "transaction:string, "
                       "transfer_date:numeric",
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
