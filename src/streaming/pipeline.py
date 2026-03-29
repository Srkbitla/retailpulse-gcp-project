import argparse
import json
import logging

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp

from src.streaming.schemas import RAW_EVENTS_SCHEMA, REALTIME_METRICS_SCHEMA
from src.streaming.transforms import (
    build_metric_seed,
    format_invalid_record,
    normalize_event,
    parse_message,
    validate_event,
)


class ParseValidateNormalizeFn(beam.DoFn):
    def process(self, raw_message: bytes):
        try:
            parsed = parse_message(raw_message)
            normalized = normalize_event(parsed)
            errors = validate_event(normalized)

            if errors:
                yield beam.pvalue.TaggedOutput("invalid", format_invalid_record(raw_message, errors))
                return

            yield normalized
        except Exception as exc:  # noqa: BLE001
            yield beam.pvalue.TaggedOutput("invalid", format_invalid_record(raw_message, [str(exc)]))


class FirstRecordCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return None

    def add_input(self, accumulator, element):
        if accumulator is None:
            return element
        return accumulator if accumulator["event_ts"] <= element["event_ts"] else element

    def merge_accumulators(self, accumulators):
        winner = None
        for accumulator in accumulators:
            if accumulator is None:
                continue
            if winner is None or accumulator["event_ts"] < winner["event_ts"]:
                winner = accumulator
        return winner

    def extract_output(self, accumulator):
        return accumulator


class RevenueMetricsCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return {"total_orders": 0, "gross_revenue": 0.0, "high_value_orders": 0}

    def add_input(self, accumulator, element):
        accumulator["total_orders"] += int(element["order_count"])
        accumulator["gross_revenue"] += float(element["gross_revenue"])
        accumulator["high_value_orders"] += int(element["high_value_orders"])
        return accumulator

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for accumulator in accumulators:
            merged["total_orders"] += accumulator["total_orders"]
            merged["gross_revenue"] += accumulator["gross_revenue"]
            merged["high_value_orders"] += accumulator["high_value_orders"]
        return merged

    def extract_output(self, accumulator):
        total_orders = accumulator["total_orders"]
        accumulator["avg_order_value"] = round(
            accumulator["gross_revenue"] / total_orders if total_orders else 0.0,
            2,
        )
        accumulator["gross_revenue"] = round(accumulator["gross_revenue"], 2)
        return accumulator


class FormatMetricsRowFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        (store_id, region), metrics = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat().replace("+00:00", "Z"),
            "window_end": window.end.to_utc_datetime().isoformat().replace("+00:00", "Z"),
            "store_id": store_id,
            "region": region,
            "total_orders": metrics["total_orders"],
            "gross_revenue": metrics["gross_revenue"],
            "avg_order_value": metrics["avg_order_value"],
            "high_value_orders": metrics["high_value_orders"],
            "pipeline_run_ts": Timestamp.now().to_utc_datetime().isoformat().replace("+00:00", "Z"),
        }


def build_pipeline_options(known_args: argparse.Namespace, pipeline_args: list[str]) -> PipelineOptions:
    options = PipelineOptions(
        pipeline_args,
        save_main_session=True,
        project=known_args.project_id,
        region=known_args.region,
        runner=known_args.runner,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
    )
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True
    return options


def run(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="RetailPulse real-time Dataflow pipeline.")
    parser.add_argument("--project_id", required=True, help="GCP project ID.")
    parser.add_argument("--region", default="us-central1", help="Dataflow region.")
    parser.add_argument("--input_subscription", required=True, help="Pub/Sub subscription path.")
    parser.add_argument("--raw_table", required=True, help="BigQuery target table for raw events.")
    parser.add_argument("--metrics_table", required=True, help="BigQuery target table for one-minute metrics.")
    parser.add_argument("--dead_letter_path", required=True, help="GCS path prefix for invalid events.")
    parser.add_argument("--runner", default="DirectRunner", help="Beam runner to use.")
    parser.add_argument("--temp_location", default="", help="GCS temp location for Dataflow.")
    parser.add_argument("--staging_location", default="", help="GCS staging location for Dataflow.")

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = build_pipeline_options(known_args, pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        parsed = (
            pipeline
            | "ReadFromPubSub" >> ReadFromPubSub(subscription=known_args.input_subscription)
            | "ParseValidateNormalize" >> beam.ParDo(ParseValidateNormalizeFn()).with_outputs(
                "invalid",
                main="valid",
            )
        )

        valid_events = (
            parsed.valid
            | "WindowValidEvents" >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
            | "KeyByEventId" >> beam.Map(lambda row: (row["event_id"], row))
            | "DeduplicateByEventId" >> beam.CombinePerKey(FirstRecordCombineFn())
            | "DropEventIdKey" >> beam.Values()
        )

        _ = (
            valid_events
            | "WriteRawEventsToBigQuery"
            >> WriteToBigQuery(
                table=known_args.raw_table,
                schema=RAW_EVENTS_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            valid_events
            | "FilterOrderCreated" >> beam.Filter(lambda row: row["event_type"] == "ORDER_CREATED")
            | "BuildMetricSeed" >> beam.Map(build_metric_seed)
            | "WindowMetricSeed" >> beam.WindowInto(FixedWindows(60), allowed_lateness=300)
            | "KeyMetrics" >> beam.Map(lambda row: ((row["store_id"], row["region"]), row))
            | "AggregateMetrics" >> beam.CombinePerKey(RevenueMetricsCombineFn())
            | "FormatMetricsRow" >> beam.ParDo(FormatMetricsRowFn())
            | "WriteMetricsToBigQuery"
            >> WriteToBigQuery(
                table=known_args.metrics_table,
                schema=REALTIME_METRICS_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            parsed.invalid
            | "WindowInvalidEvents" >> beam.WindowInto(FixedWindows(300))
            | "InvalidToJson" >> beam.Map(json.dumps)
            | "WriteInvalidEvents"
            >> WriteToText(
                file_path_prefix=known_args.dead_letter_path,
                file_name_suffix=".jsonl",
                shard_name_template="-SS-of-NN",
            )
        )


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    run()


if __name__ == "__main__":
    main()
