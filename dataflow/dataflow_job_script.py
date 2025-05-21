import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions

logging.basicConfig(level=logging.INFO)

def parse_order(message):
    try:
        record = json.loads(message.decode("utf-8"))
        logging.info(f"Parsed message: {record}")
        return {
            "order_id": record["order_id"],
            "user_id": record["user_id"],
            "product_id": record["product_id"],
            "category": record["category"],
            "price": float(record["price"]),
            "quantity": int(record["quantity"]),
            "total_amount": float(record["total_amount"]),
            "payment_method": record["payment_method"],
            "order_timestamp": record["order_timestamp"]
        }
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return None

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", help="GCP project ID", required=True)
    parser.add_argument("--region", help="Dataflow region", required=True)
    parser.add_argument("--input_subscription", help="Pub/Sub subscription", required=True)
    parser.add_argument("--output_table", help="BigQuery table (proj:ds.tbl)", required=True)
    parser.add_argument("--temp_location", help="GCS temp location", required=True)

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = "DataflowRunner"

    gcp_opts = options.view_as(GoogleCloudOptions)
    gcp_opts.project = known_args.project
    gcp_opts.region = known_args.region
    gcp_opts.temp_location = known_args.temp_location

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseJSON" >> beam.Map(parse_order)
            | "FilterErrors" >> beam.Filter(lambda r: r is not None)
            | "LogParsedRecords" >> beam.Map(lambda r: logging.info(f"Valid record: {r}") or r)
            | "WriteToBQ" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=(
                    "order_id:STRING, user_id:STRING, product_id:STRING, category:STRING, "
                    "price:FLOAT, quantity:INTEGER, total_amount:FLOAT, payment_method:STRING, "
                    "order_timestamp:TIMESTAMP"
                ),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

if __name__ == "__main__":
    run()
