import argparse
import csv
import json
import time

from google.cloud import pubsub_v1
from mmlib.ops import log

logger = log.getLogger(__name__)

DEFAULT_FIELD_MAPPINGS: dict = {
    "client_id": "client_id",
    "customer_id": "customer_id",
    "subscriber_id": "subscriber_id",
    "unique_corp_id": "unique_corp_id",
    "dependent_id": "dependent_id",
    "first_name": "first_name",
    "last_name": "last_name",
    "date_of_birth": "date_of_birth",
    "email": "email",
}


def publish_record(p: pubsub_v1.PublisherClient, topic_path: str, d: bytes, a: dict):
    future = p.publish(topic_path, d, **a)
    logger.info(future.result())


def send_file_to_pubsub(file_path: str, mapping: dict, topic_path: str):
    with open(file_path) as file:
        reader = csv.DictReader(file)
        count = 0
        for record in reader:
            e9y_record: dict = {
                # Customer Identifiers
                "client_id": record.get(mapping.get("client_id")),
                "customer_id": record.get(mapping.get("customer_id")),
                # Member Identifiers
                "unique_corp_id": record.get(mapping.get("unique_corp_id")),
                "dependent_id": record.get(mapping.get("dependent_id")),
                # Other attributes
                "first_name": record.get(mapping.get("first_name")),
                "last_name": record.get(mapping.get("last_name")),
                "date_of_birth": record.get(mapping.get("date_of_birth")),
                "email": record.get(mapping.get("email")),
                "address": {
                    "address_1": "6596 Kaitlyn Harbor Suite 825",
                    "address_2": "6174 Marco Crossroad Apt. 770",
                    "city": "North Scott",
                    "state": "New Jersey",
                    "postal_code": "57628",
                    "postal_code_suffix": "83956",
                    "country_code": "UG",
                },
                "employer_assigned_id": "WKVIGBLTKPS",
                "record": record,
                "effective_range": {
                    "lower": "2021-01-10",
                    "upper": "2024-06-09",
                    "lower_inc": True,
                    "upper_inc": True,
                },
                "gender_code": "F",
                "do_not_contact": False,
            }

            attributes: dict = {
                "source": "optum",
                "external_id": e9y_record["client_id"],
                "external_name": e9y_record["customer_id"],
                "received_ts": str(time.monotonic_ns()),
            }
            json_str: str = json.dumps(e9y_record)
            data: bytes = json_str.encode("utf-8")
            publish_record(p=publisher, topic_path=topic_path, d=data, a=attributes)
            count += 1

    logger.info(f"Published {count} messages to {topic_path}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "CSV to Pubsub",
        description="Parse a CSV census file and send the records to pubsub",
        epilog="Defaults to QA1 pubsub",
    )
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Path to CSV file to send to pubsub",
    )
    parser.add_argument(
        "-p",
        "--project",
        type=str,
        help="Pubsub project ID",
        default="maven-clinic-qa1",
    )
    parser.add_argument(
        "-t",
        "--topic",
        type=str,
        help="Pubsub topic ID",
        default="eligibility-integrations-7d3e3061",
    )
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path: str = publisher.topic_path(args.project, args.topic)

    send_file_to_pubsub(args.file, DEFAULT_FIELD_MAPPINGS, topic_path)
