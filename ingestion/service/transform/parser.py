import copy
import enum
import functools
from typing import ItemsView

import ddtrace
import pendulum
from ingestion import model, repository

from app.eligibility import convert

__all__ = ("EligibilityFileParser", "ParseErrorMessage", "ParseWarningMessage")

to_date_null = functools.partial(convert.to_date, default=None)


class EligibilityFileParser:
    """The Core API for transformation and validation of an eligibility file"""

    __slots__ = "converters"

    CONVERTERS_BY_KEY = {
        "beneficiaries_enabled": (
            convert.to_beneficiaries_enabled,
            "beneficiaries_enabled",
        ),
        "gender": (convert.to_can_get_pregnant, "can_get_pregnant"),
        "wallet_enabled": (convert.to_bool, "wallet_enabled"),
        "cobra_coverage": (convert.to_bool, "cobra_coverage"),
        "company_couple": (convert.to_bool, "company_couple"),
        "date_of_birth": (convert.to_date, "date_of_birth"),
        "employee_start_date": (to_date_null, "employee_start_date"),
        "employee_eligibility_date": (to_date_null, "employee_eligibility_date"),
        "country": (convert.to_country_code, "country"),
    }
    CLIENT_SPEC_PII_KEYS = frozenset({"date_of_birth", "unique_corp_id"})
    SECONDARY_PII_KEYS = frozenset(
        {"date_of_birth", "work_state", "first_name", "last_name"}
    )
    PRIMARY_PII_KEYS = frozenset({"date_of_birth", "email"})
    PRIMARY_KEY = "unique_corp_id"

    def __init__(self):
        self.converters: ItemsView = self.CONVERTERS_BY_KEY.items()

    @ddtrace.tracer.wrap()
    def parse(self, *, row: dict) -> model.ParsedRecord:  # noqa: C901
        # Get a shallow copy of the row.
        row = copy.copy(row)
        parsed = model.ParsedRecord(record=row)
        # This row can't be processed. Bail out.
        # Have to check for a single empty "extra", this can happen if a reader
        #   "fixed" a csv by adding an anonymous column on import.
        if repository.EXTRA_HEADER in parsed.record and any(
            parsed.record[repository.EXTRA_HEADER]
        ):
            parsed.errors.append(ParseErrorMessage.EXTRA_FIELD)
            return parsed
        parsed.record.pop(repository.EXTRA_HEADER, None)

        # Run initial conversions on our data.
        for key, (converter, target) in self.converters:
            if key in row:
                parsed.record[target] = converter(parsed.record[key])

        # If we have an email, it should be valid.
        if "email" in parsed.record:
            email = parsed.record["email"] = (parsed.record["email"] or "").strip()
            if email:
                match = convert.EMAIL_PATTERN.match(email)
                if not match:
                    parsed.errors.append(ParseErrorMessage.EMAIL)
            else:
                parsed.warnings.append(ParseWarningMessage.EMAIL)
        # If we don't have a dob, that's a problem!
        # TODO: Handle european date formats!
        if "date_of_birth" not in parsed.record:
            parsed.errors.append(ParseErrorMessage.DOB_MISSING)
        # Otherwise it shouldn't be in the future and should be parse-able.
        else:
            dob = parsed.record["date_of_birth"]
            parsed.record["date_of_birth"] = dob.to_date_string()

            if dob > pendulum.today().date():
                parsed.errors.append(ParseErrorMessage.DOB_FUTURE)
            # This is a sentinel value we use if the date is invalid on conversion.
            if dob is convert.DATE_UNKNOWN:
                parsed.errors.append(ParseErrorMessage.DOB_PARSE)
                # Replace with the original input so we can inspect it later.
                parsed.record["date_of_birth"] = row["date_of_birth"]

        # Gotta have the primary key!
        # Conversely to a true primary key in a database, this is configured on a
        #   per-org basis, and may be one of theoretically any key in the row.
        # Get the value and strip any whitespace.
        pk = parsed.record[self.PRIMARY_KEY] = (
            parsed.record.get(self.PRIMARY_KEY) or ""
        ).strip()
        if not pk:
            msg = ParseErrorMessage.CORP_ID_MISSING
            parsed.errors.append(msg)
        # This is the data we use to verify a user when they sign up, gotta have that.
        keys = parsed.record.keys()
        if not (
            self.SECONDARY_PII_KEYS.issubset(keys)
            or self.PRIMARY_PII_KEYS.issubset(keys)
            or self.CLIENT_SPEC_PII_KEYS.issubset(keys)
        ):
            parsed.errors.append(ParseErrorMessage.PII_MISSING)

        # Ensure address normalization was successful.
        # We'll use `country_code` for out state lookups, so we set the default here.
        country_code = convert.COUNTRY_DEFAULT
        if country := parsed.record.get("country", ""):
            if country == convert.COUNTRY_UNKNOWN:
                parsed.warnings.append(ParseWarningMessage.COUNTRY)
                parsed.record["country"] = row["country"]
            else:
                # Update the country code for state lookups.
                country_code = parsed.record["country"]
        if state := parsed.record.get("state", ""):
            state = convert.to_state_code(state, country_code=country_code)
            if state != convert.STATE_UNKNOWN:
                parsed.record["state"] = state
            else:
                parsed.warnings.append(ParseWarningMessage.STATE)
        if (work_state := parsed.record.get("work_state", "")) and work_state != state:
            work_state = convert.to_state_code(work_state, country_code=country_code)
            # Default to `state` if we have it and `work_state` couldn't be parsed.
            work_state = (
                state if state and work_state == convert.STATE_UNKNOWN else work_state
            )
            if work_state != convert.STATE_UNKNOWN:
                parsed.record["work_state"] = work_state
            else:
                parsed.warnings.append(ParseWarningMessage.STATE)
        # If they've provided a dependent id, make sure we strip any whitespace.
        if "dependent_id" in parsed.record:
            parsed.record["dependent_id"] = str(
                parsed.record["dependent_id"] or ""
            ).strip()

        return parsed


class ParseErrorMessage(str, enum.Enum):
    DOB_MISSING = "dob_missing"
    DOB_PARSE = "dob_parsing_error"
    DOB_FUTURE = "dob_in_future"
    CORP_ID_MISSING = "unique_corp_id_missing"
    EMAIL = "email_parsing_error"
    PII_MISSING = "required_pii_missing"
    EXTRA_FIELD = "row_contains_extra_fields"
    CLIENT_ID_NO_MAPPING = "client_id_not_configured"


class ParseWarningMessage(str, enum.Enum):
    EMAIL = "null_email_provided"
    STATE = "unknown_state"
    COUNTRY = "unknown_country"
