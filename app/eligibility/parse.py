"""The Eligibility File Processor.

This module implements the core logic for normalizing an eligibility file received by a
client.
"""
from __future__ import annotations

import csv
import enum
import functools
import io
import itertools
import re
from typing import (
    AnyStr,
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    Protocol,
    Sequence,
    Type,
    TypeVar,
)

import pendulum
import typic
from mmlib.ops import log, stats

import constants
from app.eligibility.domain.model import ParsedFileRecords
from app.utils import feature_flag, utils
from db import model

from ..utils.utils import detect_and_sanitize_possible_ssn, resolve_gender_code
from .constants import ORGANIZATIONS_NOT_SENDING_DOB
from .convert import (
    COUNTRY_DEFAULT,
    COUNTRY_UNKNOWN,
    DATE_UNKNOWN,
    DEFAULT_DATE_OF_BIRTH,
    STATE_UNKNOWN,
    to_beneficiaries_enabled,
    to_bool,
    to_can_get_pregnant,
    to_country_code,
    to_date,
    to_state_code,
)

logger = log.getLogger(__name__)
to_date_null = functools.partial(to_date, default=None)


class DelimiterError(Exception):
    """Raise when we encounter a non-standard CSV delimiter"""

    pass


class EligibilityCSVReader:
    """A ReaderProtocol for ingesting CSV data."""

    __slots__ = "headers", "data", "encoding"

    def __init__(
        self,
        headers: model.HeaderMapping,
        data: AnyStr,
        *,
        encoding: str = "utf-8",
    ):
        self.headers = headers
        self.data = data
        self.encoding = encoding

    def _remap_headers(self, fields: Iterable[str]) -> Sequence[str]:
        # With the headers provided to us in a file, attempt to map the client-provided
        # values to our internal set of expected header values. This is called when we
        # first set up our CSV reader- the result of it is then used when we parse each row.

        # First, sanitize our input header row from the client file, removing whitespace etc.
        headers = [
            h.lower()
            # Whitespace
            .strip()
            # Encoded or quoted data
            .strip("'\"")
            # Internal line-breaks in the header.
            .replace("\r", " ").replace("\n", " ")
            for h in fields
        ]

        # Then generate the mapping of our internal values to the externally configured header aliases
        internally_configured_headers = {
            **self.headers.with_defaults(),
            **self.headers.optional_headers(),
        }
        alias_to_header = {
            orgk.lower(): internalk.lower()
            for internalk, orgk in internally_configured_headers.items()
        }

        # Finally, generate an ordered list of our headers, using default values if possible, otherwise
        # use client provide values. The below can be acomplished in a very slick
        # `return [mapping.get(h, h) for h in headers if h]` but breaking it down for readability
        final_header_list = [
            # Get the mapped alias for this header, defaulting to the provided header if we don't have an alias for it
            alias_to_header.get(h, h)
            # For every header detected in the CSV
            for h in headers
            # If the header isn't empty due to white-space we previously stripped
            if h
        ]

        return final_header_list

    def _get_reader(self) -> csv.DictReader:
        buffer = (
            io.TextIOWrapper(io.BytesIO(self.data), encoding=self.encoding)
            if isinstance(self.data, bytes)
            else io.StringIO(self.data)
        )
        try:
            dialect = csv.Sniffer().sniff(buffer.readline(), delimiters=",\t")
        except Exception:
            logger.error(
                "Error in processing file- non-standard delimiter used for csv"
            )
            stats.increment(
                metric_name="eligibility.process.file_parse.invalid_file_delimiter",
                pod_name=constants.POD,
                tags=["eligibility:error"],
            )
            raise DelimiterError
        buffer.seek(0)
        reader = csv.DictReader(buffer, restkey=_EXTRA_HEADER, dialect=dialect)
        reader.fieldnames = self._remap_headers(reader.fieldnames)
        return reader

    def __iter__(self) -> Iterator[dict]:
        yield from self._get_reader()


class EligibilityFileParser:
    """The Core API for transformation and validation of an eligibility file.

    This class contains the core business logic for normalizing eligibility data received by a client.

    See Also:
        https://www.notion.so/mavenclinic/File-based-Eligibility-5c1a73c9aba144b58db1c4b7880f113f#58a077d2455f4db59e1527c990e61e04
    """

    __slots__ = (
        "file",
        "configuration",
        "headers",
        "data",
        "external_id_mappings",
        "custom_attributes",
        "logger",
        "converters",
        "today",
        "reader",
        "logged_file_without_dob",
        "parse_line_no",
    )

    CONVERTERS_BY_KEY = {
        "beneficiaries_enabled": (to_beneficiaries_enabled, "beneficiaries_enabled"),
        "gender": (to_can_get_pregnant, "can_get_pregnant"),
        "wallet_enabled": (to_bool, "wallet_enabled"),
        "cobra_coverage": (to_bool, "cobra_coverage"),
        "company_couple": (to_bool, "company_couple"),
        "date_of_birth": (to_date, "date_of_birth"),
        "employee_start_date": (to_date_null, "employee_start_date"),
        "employee_eligibility_date": (to_date_null, "employee_eligibility_date"),
        "country": (to_country_code, "country"),
    }
    CLIENT_SPEC_PII_KEYS = frozenset({"date_of_birth", "unique_corp_id"})
    SECONDARY_PII_KEYS = frozenset(
        {"date_of_birth", "work_state", "first_name", "last_name"}
    )
    PRIMARY_PII_KEYS = frozenset({"date_of_birth", "email"})
    ORGANIZATIONS_WITHOUT_DOB_PII_KEYS = frozenset(
        {"email", "first_name", "last_name", "unique_corp_id"}
    )
    PRIMARY_KEY = "unique_corp_id"
    HEALTH_PLAN_FIELDS = frozenset(
        {
            "maternity_indicator_date",
            "maternity_indicator",
            "delivery_indicator_date",
            "delivery_indicator",
            "fertility_indicator_date",
            "fertility_indicator",
            "p_and_p_indicator",
            "client_name",
        }
    )

    def __init__(
        self,
        file: model.File,
        configuration: model.Configuration,
        headers: model.HeaderMapping,
        data: AnyStr,
        external_id_mappings: dict = {},
        custom_attributes: dict = {},
        *,
        reader_cls: Type[ReaderProtocolT] = EligibilityCSVReader,
    ):
        self.file = file
        self.configuration = configuration
        self.headers = headers
        self.custom_attributes = custom_attributes
        self.data = data
        self.external_id_mappings = external_id_mappings
        self.logger = logger.bind(
            file=file.name,
            organization=file.organization_id,
            reader=reader_cls.__name__,
        )
        self.converters: ItemsView = self.CONVERTERS_BY_KEY.items()
        self.today = pendulum.today().date()
        self.reader = reader_cls(self.headers, self.data, encoding=self.file.encoding)
        self.logged_file_without_dob = set()
        self.parse_line_no = 0

    def _is_optum_provider(self):
        return (
            self.configuration.data_provider
            and self.configuration.directory_name.startswith("optum")
        )

    def _get_parser(self) -> Callable:  # noqa: C901
        # The main reason for the factory function is pinning all of these values to
        #   the local function namespace. This is useless in normal circumstances,
        #   but can provide a decent boost when in hot loops, since we can circumvent a
        #   globals lookup.

        # Regex based on RFC 3696
        email_regex_pattern = re.compile(
            r"(?=^.{1,64}@)^(?P<username>[\w!#$%&'*+\/=?`{|}~^-]+(?:\.[\w!#$%&'*+\/=?`{|}~^-]+)*)(?=@.{2,255}$)@(?P<domain>([a-zA-Z0-9-]+\.)+[a-zA-Z]+)$"
        )

        def _parse_rows(
            row: dict,
            *,
            today=self.today,
            converters=self.converters,
            primary_key=self.PRIMARY_KEY,
            file_id=self.file.id,
            organization_id=self.file.organization_id,
            email_pattern=email_regex_pattern,
            country_default=COUNTRY_DEFAULT,
            country_unknown=COUNTRY_UNKNOWN,
            state_unknown=STATE_UNKNOWN,
            primary_pii_keys=self.PRIMARY_PII_KEYS,
            secondary_pii_keys=self.SECONDARY_PII_KEYS,
            client_spec_pii_keys=self.CLIENT_SPEC_PII_KEYS,
            organizations_without_dob_pii_keys=self.ORGANIZATIONS_WITHOUT_DOB_PII_KEYS,
            health_plan_keys=self.HEALTH_PLAN_FIELDS,
            ParseErrorMessage=ParseErrorMessage,
            extra=_EXTRA_HEADER,
            to_state_code=to_state_code,
            external_id_mappings=self.external_id_mappings,
            custom_attributes=self.custom_attributes,
        ) -> dict:

            # Get a shallow copy of the row.
            errors = []
            warnings = []
            out: dict = {
                "file_id": file_id,
                "organization_id": organization_id,
                "errors": errors,
                "warnings": warnings,
                **row,
            }
            # This row can't be processed. Bail out.
            # Have to check for a single empty "extra", this can happen if a reader
            #   "fixed" a csv by adding an anonymous column on import.
            if extra in out and any(out[extra]):
                errors.append(ParseErrorMessage.EXTRA_FIELD)
                return out
            out.pop(extra, None)

            # If we are looking at a row coming from a data provider + we have a 'client_id' header and external_id_mappings,
            # overwrite our org_id with that actually pertaining to the row (instead of the file)
            # If no 'client_id' header, treat our row like a standard row and skip the below block
            external_client_id = None
            external_customer_id = None
            if self.configuration.data_provider:
                external_client_id = row.get("client_id", None)
                external_customer_id = row.get("customer_id", None)
                overwrite_org_id = None

                # case 1, both client_id and customer_id exist
                if external_client_id and external_customer_id:
                    # composite key
                    overwrite_org_id = external_id_mappings.get(
                        (external_client_id, external_customer_id)
                    )

                if not overwrite_org_id and external_client_id:
                    # just client_id
                    overwrite_org_id = external_id_mappings.get(external_client_id)

                if overwrite_org_id:
                    out["organization_id"] = overwrite_org_id
                    out["data_provider_organization_id"] = organization_id
                else:
                    # for optum file, check whether we want to log the client_id errors
                    if (
                        self._is_optum_provider()
                        and not feature_flag.is_optum_file_logging_enabled()
                    ):
                        stats.increment(
                            metric_name="eligibility.process.file_parse.client_id_errors",
                            pod_name=constants.POD,
                            tags=[
                                f"organization_id:{organization_id}",
                                f"file_id:{file_id}",
                                f"external_client_id:{external_client_id}",
                            ],
                        )
                        out["record"] = out.copy()
                        errors.append(ParseErrorMessage.CLIENT_ID_NO_MAPPING)
                        return out

                    self.logger.error(
                        "Received a record from data provider that did not have a mapped external client_id",
                        file_id=file_id,
                        organization_id=organization_id,
                        external_client_id=external_client_id,
                        external_customer_id=external_customer_id,
                    )
                    out["record"] = out.copy()
                    errors.append(ParseErrorMessage.CLIENT_ID_NO_MAPPING)
                    return out

            # Run initial conversions on our data.
            for key, (converter, target) in converters:
                if key in row:
                    out[target] = converter(row[key])

            # Extract any custom attributes and remove the "unrefined" form from out
            out["custom_attributes"] = {
                value: out.get(key, None) for key, value in custom_attributes.items()
            }
            for key in custom_attributes:
                out.pop(key, None)

            # Construct our healthplan fields - these are optional values that may/may not be included
            health_plan_values = {
                key: out.get(key, None)
                for key in health_plan_keys
                if out.get(key, None) is not None
            }
            if health_plan_values != {}:
                out["custom_attributes"]["health_plan_values"] = health_plan_values

            for key in health_plan_keys:
                out.pop(key, None)

            # If we have an email, it should be valid.
            if "email" in out:
                email = out["email"] = (out["email"] or "").strip()
                if email:
                    match = email_pattern.match(email)
                    if not match:
                        errors.append(ParseErrorMessage.EMAIL)
                else:
                    warnings.append(ParseWarningMessage.EMAIL)
            # check if org is supposed to send us dob and skip parsing dob altogether if not
            client_organization_id = out["organization_id"]
            is_org_not_sending_dob = (
                client_organization_id in ORGANIZATIONS_NOT_SENDING_DOB
            )
            if is_org_not_sending_dob:
                # only log when we encounter this the first time
                if client_organization_id not in self.logged_file_without_dob:
                    self.logged_file_without_dob.add(client_organization_id)
                    self.logger.info(
                        "Received a file from an organization that doesn't send date_of_birth",
                        file_id=file_id,
                        organization_id=client_organization_id,
                    )
                # populate a default value if absent to satisfy not null constraint
                if "date_of_birth" not in out:
                    out["date_of_birth"] = DEFAULT_DATE_OF_BIRTH
            # if org is supposed to send us dob continue parsing it
            else:
                # If we don't have a dob, that's a problem!
                if "date_of_birth" not in out:
                    errors.append(ParseErrorMessage.DOB_MISS)
                # Otherwise it shouldn't be in the future and should be parse-able.
                else:
                    dob = out["date_of_birth"]
                    # This is a sentinel value we use if the date is invalid on conversion.
                    if dob == DATE_UNKNOWN:
                        # Replace with the original input so we can inspect it later.
                        errors.append(ParseErrorMessage.DOB_PARSE)
                        out["date_of_birth"] = row["date_of_birth"]

                    # Handle case where we get the default date representing unknown value 0001-01-01
                    elif dob is None:
                        errors.append(ParseErrorMessage.DOB_UNKNOWN)
                        out["date_of_birth"] = DEFAULT_DATE_OF_BIRTH
                    elif dob > today:
                        errors.append(ParseErrorMessage.DOB_FUT)

            # Gotta have the primary key!
            # Conversely to a true primary key in a database, this is configured on a
            #   per-org basis, and may be one of theoretically any key in the row.
            # Get the value and strip any whitespace.
            pk = out[primary_key] = (out.get(primary_key) or "").strip()
            if not pk:
                msg = ParseErrorMessage.CORP_ID_MISS
                errors.append(msg)

            # Check to see if the PK resembles a SSN
            sanitized_pk, possible_ssn = detect_and_sanitize_possible_ssn(
                input_string=pk, organization_id=organization_id, file_id=file_id
            )
            if possible_ssn:
                warnings.append(ParseWarningMessage.SSN)

            # Overwrite the PK if it looked like an SSN with hyphens
            if sanitized_pk:
                out[primary_key] = sanitized_pk

            # This is the data we use to verify a user when they sign up, gotta have that.
            keys = out.keys()
            # if org is not sending dob make sure we all the necessary pii
            if (
                is_org_not_sending_dob
                and not organizations_without_dob_pii_keys.issubset(keys)
            ):
                errors.append(ParseErrorMessage.PII_MISS)

            if not is_org_not_sending_dob and not (
                secondary_pii_keys.issubset(keys)
                or primary_pii_keys.issubset(keys)
                or client_spec_pii_keys.issubset(keys)
            ):
                errors.append(ParseErrorMessage.PII_MISS)

            # Ensure address normalization was successful.
            # We'll use `country_code` for out state lookups, so we set the default here.
            country_code = country_default
            if country := out.get("country", ""):
                if country == country_unknown:
                    warnings.append(ParseWarningMessage.COUNTRY)
                    out["country"] = row["country"]
                else:
                    # Update the country code for state lookups.
                    country_code = out["country"]
            if state := out.get("state", ""):
                state = to_state_code(state, country_code=country_code)
                if state != state_unknown:
                    out["state"] = state
                else:
                    warnings.append(ParseWarningMessage.STATE)
            if (work_state := out.get("work_state", "")) and work_state != state:
                work_state = to_state_code(work_state, country_code=country_code)
                # Default to `state` if we have it and `work_state` couldn't be parsed.
                work_state = (
                    state if state and work_state == state_unknown else work_state
                )
                if work_state != state_unknown:
                    out["work_state"] = work_state
                else:
                    warnings.append(ParseWarningMessage.STATE)
            # If they've provided a dependent id, make sure we strip any whitespace.
            if "dependent_id" in out:
                out["dependent_id"] = str(out["dependent_id"] or "").strip()

            out["record"] = out.copy()

            # Generate our hash value for the record
            (
                out["hash_value"],
                out["hash_version"],
            ) = utils.generate_hash_for_file_based_record(out)

            # Set a marker if we had to sanitized our unique_corp_id
            if sanitized_pk:
                out["record"]["id-resembling-hyphenated-ssn"] = True

            gender_code = resolve_gender_code(row.get("gender", ""))
            out["gender_code"] = gender_code

            if errors:
                logger.error(
                    "Errors encountered during parsing",
                    file_id=file_id,
                    pk=pk,
                    organization_id=organization_id,
                    external_client_id=external_client_id,
                    external_customer_id=external_customer_id,
                    errors=errors,
                )

            return out

        return _parse_rows

    def iterparse(self) -> Iterator[dict]:
        """Iterate over the data in `self.reader` and process each in turn."""
        parser = self._get_parser()
        if self.reader:
            return map(parser, self.reader)
        return None

    __iter__ = iterparse

    def parse(self, *, batch_size: int = 10_000) -> Iterator[ParsedFileRecords]:
        """
        Returns a generator for batches of size batch_size, batches are objects that
        contain batch_size number of errors and records

        Args:
            batch_size:

        Returns:

        """
        for batch in chunker(self, batch_size):
            errors = []
            valid = []
            for parsed in batch:
                self.parse_line_no += 1
                if (
                    isinstance(parsed, dict)
                    and ("record" in parsed)
                    and isinstance(parsed["record"], dict)
                ):
                    parsed["record"]["parse_line_no"] = self.parse_line_no
                if parsed["errors"]:
                    errors.append(typic.transmute(model.FileParseError, parsed))
                else:
                    valid.append(typic.transmute(model.FileParseResult, parsed))
            yield ParsedFileRecords(errors=errors, valid=valid)


_EXTRA_HEADER = "extra"


class ReaderProtocolT(Protocol):
    """The expected API for a 'reader' of a specific format.

    The `EligibilityFileProcessor` will instantiate an instance of the reader with the
    defined values and expects to be able to iterate over a series of dictionaries.

    The Reader is responsible for remapping any headers and returning each row from
    the data sources as a dictionary.

    See Also:

    """

    configuration: model.Configuration
    data: AnyStr

    def __init__(
        self,
        headers: model.HeaderMapping,
        data: AnyStr,
        *,
        encoding: str = "utf-8",
    ):
        ...

    def __iter__(self) -> Iterator[dict]:
        ...


def chunker(iterable: Iterable[T], n: int) -> Iterator[list[T]]:
    """
    Takes in an iterable and returns iterator of lists of size n,
    last one is trimmed to size of remainder

    Args:
        iterable: an iterable
        n: size of chunks to return

    Returns:
        Iterator[list[T]]

    """
    it = iter(iterable)
    while batch := list(itertools.islice(it, n)):
        yield batch


class ParseErrorMessage(str, enum.Enum):
    DOB_MISS = "Missing DOB."
    DOB_PARSE = "Couldn't parse DOB."
    DOB_UNKNOWN = "Unknown date provided - 0001-01-01"
    DOB_FUT = "DOB in future."
    CORP_ID_MISS = f"Row missing {EligibilityFileParser.PRIMARY_KEY!r}."
    EMAIL = "Bad email."
    PII_MISS = "Missing required PII."
    EXTRA_FIELD = "Row contains extra fields."
    CLIENT_ID_NO_MAPPING = "Missing organization_external_id mapping for a client_id in row originating from a data provider"


class ParseWarningMessage(str, enum.Enum):
    EMAIL = "Null email provided."
    STATE = "Unknown State."
    COUNTRY = "Unknown Country."
    SSN = "PK resembles SSN"


T = TypeVar("T")
