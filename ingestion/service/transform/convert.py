from __future__ import annotations

import datetime
from typing import Literal, Sequence, TypeVar

import ddtrace
import structlog
from ingestion import model

from app.eligibility import convert

__all__ = (
    "to_date",
    "resolve_member_address",
    "resolve_effective_range",
    "resolve_do_not_contact",
    "parse_custom_attributes",
    "OEEDM_ADDRESS_CODES",
    "EMPLOYER_ADDRESS_CODES",
    "ADDRESS_CODES",
)

from ingestion.model import OptumAttribute

logger = structlog.getLogger(__name__)

T = TypeVar("T")


@ddtrace.tracer.wrap()
def to_date(
    o: str | datetime.date,
    *,
    default: datetime.date = None,
) -> datetime.date | None:
    """A generic date parser which will automatically report parse failures and pass on the default."""

    try:
        if isinstance(o, datetime.date):
            return o
        return datetime.date.fromisoformat(o)
    except ValueError:
        # Don't throw errors on null values- they might be valid (ex null termination date)
        logger.error("Failed to parse provided data")

        return default


@ddtrace.tracer.wrap()
def resolve_member_address(
    address_records: Sequence[model.OptumAddress] | None,
) -> model.Address | None:
    """Using custom logic, extract the address we should be using with our member record"""

    if address_records is None:
        return

    oeedm_data = False
    employer_data = False

    categorized_addresses = {}
    for address in address_records:
        code = address["addressTypeCode"].upper()

        # Grab only the first address we see for a given address type
        if code in categorized_addresses:
            continue

        if code in OEEDM_ADDRESS_CODES:
            categorized_addresses[code] = address
            oeedm_data = True
        elif code in EMPLOYER_ADDRESS_CODES:
            categorized_addresses[code] = address
            employer_data = True

    # If we have both oeedm and employer data for a member, raise an error- we should only have one
    if oeedm_data and employer_data:
        return

    if not categorized_addresses:
        logger.info("Received address without acceptable data source")
        return

    for code in ADDRESS_CODES:
        if code not in categorized_addresses:
            continue

        selected_address = categorized_addresses[code]
        country_code = selected_address.get("isoCountryCode")

        if country_code not in [None, ""]:
            country_code = convert.to_country_code(country_code)
        else:
            country_code = ""

        return model.Address(
            address_1=selected_address.get("addressLine1"),
            address_2=selected_address.get("addressLine2"),
            city=selected_address.get("city"),
            state=selected_address.get("state"),
            postal_code=selected_address.get("postalCode"),
            postal_code_suffix=selected_address.get("postalSuffixCode"),
            country_code=country_code,
        )

    return


@ddtrace.tracer.wrap()
def resolve_effective_range(
    policies: Sequence[model.OptumPolicy],
    today,
) -> model.EffectiveRange | None:
    """Get the effective range of a series of policy entries.

    Notes:
        - All policies will be for the same client
        - We may receive new policy entries when a policy is renewed
    """
    # There must be at least 1 policy to be able to resolve the effective range
    # This will check for cases of None and for an empty list
    if not policies:
        return

    # Start w/ lower as max value and upper as min value so that they can be
    # replaced with valid values
    lower = datetime.date.max
    upper = datetime.date.min

    # Set the lower to the earliest effective date and the upper to
    # the latest termination date
    for policy in policies:
        # get the lower and upper values for the policy
        policy_lower, policy_upper = (
            to_date(
                policy["effectiveDate"],
                default=today,
            ),
            to_date(
                policy["terminationDate"],
            ),
        )

        # log policies that have effective dates in the future
        if policy_lower and policy_lower > today:
            logger.info(
                "Policy set to start in the future.",
                effective_date=policy_lower,
                term_date=policy_upper,
            )

        # replace the stored upper or lower with the value(s) from the policy
        # if necessary
        lower = min(policy_lower, lower)
        if upper is not None:
            if policy_upper is None or policy_upper > upper:
                upper = policy_upper

    if lower == datetime.date.max and upper == datetime.date.min:
        logger.info("No valid policies defined", policies=policies)
        return

    # If our upper date is before the start date, we have an issue
    if upper is not None and upper < lower:
        logger.info(
            "Encountered a termination date that occurred before start date",
            effective_date=lower,
            term_date=upper,
            policies=policies,
        )
        return

    return model.EffectiveRange(
        lower=lower, upper=upper, lower_inc=True, upper_inc=True
    )


@ddtrace.tracer.wrap()
def resolve_do_not_contact(provided_dnc: str, **reporting_context) -> _DNCCodeT | str:
    """Attempt to normalize the do_not_contact field"""
    sanitized_dnc = provided_dnc.upper().strip()
    if sanitized_dnc == "":
        return ""

    for code, aliases in DNC_CODES.items():
        if sanitized_dnc in aliases:
            return code

    logger.info("Received non-standard do-not-contact value", value=provided_dnc)
    return provided_dnc


@ddtrace.tracer.wrap()
def parse_custom_attributes(attributes: Sequence[OptumAttribute]) -> dict:
    return {attr["name"]: attr["value"] for attr in attributes}


# endregion

# region: constants
MAX_DATE = datetime.date(9999, 12, 31)

# We only want to examine specific address types- the below arrays are organized in priority of
# address we should take i.e. for oeedm, take 263 type addresses over 180 type address
# confidential address, home address
OEEDM_ADDRESS_CODES = ("263", "180")
# consumer permanent address, consumer mailing address
EMPLOYER_ADDRESS_CODES = ("P", "M")
# Full array of possible codes. This is a FIFO queue.
ADDRESS_CODES = OEEDM_ADDRESS_CODES + EMPLOYER_ADDRESS_CODES


# Do Not Contact Code mappings
DNC_CODES: dict[_DNCCodeT, frozenset[str]] = {
    "True": frozenset(["T", "TRUE"]),
    "False": frozenset(["F", "FALSE"]),
}
_DNCCodeT = Literal["True", "False"]
