from __future__ import annotations

import hashlib
import re
from typing import Literal

import ddtrace
import structlog
from mmlib.ops import stats

import constants

logger = structlog.getLogger(__name__)


HASH_VERSION = 2


def generate_hash_for_external_record(record: dict, address: dict = None) -> (str, int):

    address_string = ""
    if address:
        address_string = ",".join(
            [
                address["address_1"],
                address["city"],
                address["state"],
                address["postal_code"],
                address.get("address_2", ""),
                address.get("postal_code_suffix", ""),
                address.get("country_code", ""),
            ]
        )

    # Smoosh all our values together
    raw_string = ",".join(
        [
            record.get("first_name", ""),
            record.get("last_name", ""),
            str(
                record.get("organization_id", ""),
            ),
            record.get("unique_corp_id", ""),
            str(record["date_of_birth"]),
            record["work_state"] if record["work_state"] else "",
            record.get("email", ""),
            record.get("dependent_id", ""),
            str(  # Remove values *we* generate from our hash
                sorted(
                    {
                        k: record["record"][k]
                        for k in record["record"]
                        if k != "received_ts"
                    }
                ),
            ),  # sort our items, so they will always be parsed in the same order
            address_string,
            record.get("do_not_contact", ""),
            record.get("gender_code", ""),
            record.get("employer_assigned_id", ""),
            str(record["effective_range"].upper)
            if record["effective_range"] and record["effective_range"].upper
            else "",
            str(record["effective_range"].lower)
            if record["effective_range"] and record["effective_range"].lower
            else "",
            str(
                sorted(
                    {
                        k: record["custom_attributes"][k]
                        for k in record["custom_attributes"]
                    }
                ),
            ),  # sort our items, so they will always be parsed in the same order
        ]
    )

    hash_result = hashlib.sha256(raw_string.encode()).hexdigest()
    hashed_value = ",".join([hash_result, str(record["organization_id"])])
    return hashed_value, HASH_VERSION


def generate_hash_for_file_based_record(record: dict) -> (str, int):
    # Smoosh all our values together

    # Smoosh custom attributes first
    custom_attributes = ""
    health_attributes = ""

    if record.get("custom_attributes"):
        health_plan_key = "health_plan_values"

        custom_attributes = str(
            sorted(
                {
                    k: record["custom_attributes"][k]
                    for k in record["custom_attributes"]
                    if k != health_plan_key
                }
            ),
        )  # sort our items, so they will always be parsed in the same order

        # If we have healthplan values, put them into a hash as well
        if record["custom_attributes"].get(health_plan_key):
            health_attributes = str(
                sorted(
                    {
                        k: record["custom_attributes"][health_plan_key][k]
                        for k in record["custom_attributes"][health_plan_key]
                    }
                ),
            )

    raw_string = ",".join(
        [
            record.get("first_name", ""),
            record.get("last_name", ""),
            str(record["organization_id"]),
            record.get("unique_corp_id", ""),
            str(record.get("date_of_birth", "")),
            record.get("state", ""),
            record.get("work_state", ""),
            record.get("country", ""),
            record.get("email", ""),
            record.get("dependent_id", ""),
            str(  # Remove items *we* add to the hash
                sorted(
                    [
                        f"{k}:{str(record['record'][k])}"
                        for k in record["record"]
                        if k != "file_id"
                    ]
                )
            ),  # sort our items, so they will always be parsed in the same order
            record.get("do_not_contact", ""),
            record.get("gender", ""),
            custom_attributes,
            health_attributes,
        ]
    )

    hash_result = hashlib.sha256(raw_string.encode()).hexdigest()
    hashed_value = ",".join([hash_result, str(record["organization_id"])])
    return hashed_value, HASH_VERSION


# Gender code mappings
GENDER_CODES: dict[_GenderCodeT, frozenset[str]] = {
    "F": frozenset(["F", "FEMALE", "W", "WOMAN"]),
    "M": frozenset(["M", "MAN", "MALE"]),
    "O": frozenset(
        ["OTHER", "O", "NON-BINARY", "NONBINARY", "GENDERQUEER", "GENDERFLUID", "X"]
    ),
    "U": frozenset(
        [
            "UNKNOWN",
            "U",
            "NOT SPECIFIED",
            "NOT DECLARED",
            "UNDECLARED",
            "UNSPECIFIED",
            "D",
            "DECLINE_TO_SELF_IDENTIFY",
            "DECLINE TO SAY",
        ]
    ),
}
_GenderCodeT = Literal["F", "M", "O", "U"]


@ddtrace.tracer.wrap()
def resolve_gender_code(
    provided_gender_code: str, **reporting_context
) -> _GenderCodeT | str:
    """Attempt to normalize the gender code field"""
    sanitized_gender_code = provided_gender_code.upper().strip()
    if sanitized_gender_code == "":
        return ""
    for code, aliases in GENDER_CODES.items():
        if sanitized_gender_code in aliases:
            return code

    # Log this value so we hopefully can implement new parsing logic in the future
    logger.info("Received non-standard gender code value", value=provided_gender_code)

    return provided_gender_code


SSN_REGEX = re.compile(r"^(?!0{3})(?!6{3})[0-8]\d{2}-?(?!0{2})\d{2}-?(?!0{4})\d{4}$")
SSN_EXACT_REGEX = re.compile(
    r"^(?!0{3})(?!6{3})[0-8]\d{2}-(?!0{2})\d{2}-(?!0{4})\d{4}$"
)


def detect_and_sanitize_possible_ssn(
    input_string: str,
    organization_id: int,
    client_id: int = None,
    customer_id: int = None,
    file_id: int = None,
) -> (str | None, bool):

    sanitized_input = None
    detected_possible_ssn = False

    # Check to see if the input resembles a SSN with hyphens -> sanitize it
    if SSN_EXACT_REGEX.match(input_string):
        sanitized_input = hashlib.sha256(input_string.encode()).hexdigest()
        logger.debug(
            "Encountered PK resembling SSN with hyphens",
            organization_id=organization_id,
            client_id=client_id,
            customer_id=customer_id,
            file_id=file_id,
        )
        detected_possible_ssn = True

    # Otherwise, if the input resembles an SSN *without* hyphens
    elif SSN_REGEX.match(input_string):
        detected_possible_ssn = True
        logger.debug(
            "Encountered PK resembling SSN",
            organization_id=organization_id,
            client_id=client_id,
            customer_id=customer_id,
            file_id=file_id,
        )

    if detected_possible_ssn:
        if file_id:
            stats.increment(
                metric_name="eligibility.process.possible_ssn",
                pod_name=constants.POD,
                tags=[f"org_id:{organization_id}", f"file_id:{file_id}"],
            )
        else:
            stats.increment(
                metric_name="eligibility.process.possible_ssn",
                pod_name=constants.POD,
                tags=[
                    f"org_id:{organization_id}",
                ],
            )

    return sanitized_input, detected_possible_ssn
