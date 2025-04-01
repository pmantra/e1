import datetime
import os
from collections import Counter
from typing import Type
from unittest import mock

import pendulum
import pytest
from tests.factories.data_models import ConfigurationFactory, FileFactory

from app.eligibility.constants import ORGANIZATIONS_NOT_SENDING_DOB
from app.eligibility.convert import DEFAULT_DATE_OF_BIRTH
from app.eligibility.parse import (
    _EXTRA_HEADER,
    DelimiterError,
    EligibilityCSVReader,
    EligibilityFileParser,
    ParseErrorMessage,
    ParseWarningMessage,
    ReaderProtocolT,
    chunker,
)
from db import model

# region header remapping


@pytest.mark.parametrize(
    argnames="reader_cls,delimiter",
    argvalues=[(EligibilityCSVReader, ","), (EligibilityCSVReader, "\t")],
)
def test_reader_remap_headers(reader_cls: Type[ReaderProtocolT], delimiter: str):
    # Given
    header_mapping = model.HeaderMapping(date_of_birth="dob")
    # When

    headers = delimiter.join(header_mapping.with_defaults().values())
    line = delimiter.join("f" for _ in header_mapping.with_defaults())
    data = f"{headers}\n{line}"
    # Then
    reader = reader_cls(header_mapping, data)
    mapped = next(iter(reader))
    assert mapped.keys() == header_mapping.with_defaults().keys()


@pytest.mark.parametrize(argnames="reader_cls", argvalues=[EligibilityCSVReader])
def test_remap_header_with_whitespace(reader_cls: Type[ReaderProtocolT]):
    # Given
    header_mapping = model.HeaderMapping(date_of_birth="dob")
    # When
    headers = ",".join(header_mapping.with_defaults().values())
    headers.replace("dob", "d\no\rb ")
    line = ",".join("f" for _ in header_mapping.with_defaults())
    data = f"{headers}\n{line}"
    # Then
    reader = reader_cls(header_mapping, data)
    mapped = next(iter(reader))
    assert mapped.keys() == header_mapping.with_defaults().keys()


@pytest.mark.parametrize(argnames="reader_cls", argvalues=[EligibilityCSVReader])
def test_remap_header_with_optional_default_header(reader_cls: Type[ReaderProtocolT]):
    # Given
    header_mapping = model.HeaderMapping()

    # When
    client_header_row = ",".join(header_mapping.with_all_headers().values())
    line = ",".join("f" for _ in header_mapping.with_all_headers())
    data = f"{client_header_row}\n{line}"

    # Generate our fake data

    # Then
    reader = reader_cls(header_mapping, data)
    mapped = next(iter(reader))
    assert mapped.keys() == header_mapping.with_all_headers().keys()


@pytest.mark.parametrize(argnames="reader_cls", argvalues=[EligibilityCSVReader])
def test_remap_header_with_optional_default_header_custom_value(
    reader_cls: Type[ReaderProtocolT],
):
    # Given
    header_mapping = model.HeaderMapping(client_id="client identifier")

    # When
    client_header_row = ",".join(header_mapping.with_all_headers().values())
    line = ",".join("f" for _ in header_mapping.with_all_headers())
    data = f"{client_header_row}\n{line}"

    # Then
    reader = reader_cls(header_mapping, data)
    mapped = next(iter(reader))
    assert mapped.keys() == header_mapping.with_all_headers().keys()


# endregion


@pytest.fixture
def default_external_id_mappings():
    return {
        "external_id": "foo",
        "internal_maven_org_id": "12345",
    }


@pytest.fixture
def default_external_id_with_customer_id_mappings():
    return {
        "external_id": "foo:bar",
        "internal_maven_org_id": "12345",
    }


@pytest.fixture
def file_parser(file_data, default_external_id_mappings) -> EligibilityFileParser:
    file: model.File = FileFactory.create()
    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id
    )
    headers = model.HeaderMapping()
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=file_data,
        headers=headers,
        external_id_mappings={
            default_external_id_mappings["external_id"]: default_external_id_mappings[
                "internal_maven_org_id"
            ]
        },
        custom_attributes={
            "custom_attributes.sub_population_identifier": "sub_population_identifier"
        },
    )
    return parser


@pytest.fixture
def file_parser_with_composite_external_id(
    file_data, default_external_id_with_customer_id_mappings
) -> EligibilityFileParser:
    file: model.File = FileFactory.create()
    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id
    )
    headers = model.HeaderMapping()
    client_id, customer_id = default_external_id_with_customer_id_mappings[
        "external_id"
    ].split(":")
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=file_data,
        headers=headers,
        external_id_mappings={
            (client_id, customer_id): default_external_id_with_customer_id_mappings[
                "internal_maven_org_id"
            ]
        },
    )
    return parser


@pytest.fixture
def file_parser_with_orgs_that_dont_send_dob(file_parser) -> EligibilityFileParser:
    org_id = next(iter(ORGANIZATIONS_NOT_SENDING_DOB))
    file: model.File = FileFactory.create(organization_id=org_id)
    file_parser.file = file
    return file_parser


@pytest.fixture
def row_parser(file_parser):
    return file_parser._get_parser()


@pytest.fixture
def row_parser_skip_dob(file_parser_with_orgs_that_dont_send_dob):
    return file_parser_with_orgs_that_dont_send_dob._get_parser()


# region file parser test


@pytest.mark.parametrize(
    argnames=("external_id_mapping", "row", "expected_org"),
    argvalues=[
        ({("fizz", "fuzz"): 1}, {"client_id": "fizz", "customer_id": "fuzz"}, 1),
        (
            {("fizz", "fuzz"): 2, "fizz": 1},
            {"client_id": "fizz", "customer_id": "fuzz"},
            2,
        ),
        ({"fizz": 1}, {"client_id": "fizz", "customer_id": "fuzz"}, 1),
    ],
    ids=[
        "client-id-and-customer-id-mapped",
        "client-id-and-customer-id-mapped-client-id-only-mapped",
        "client-id-mapped",
    ],
)
def test_composite_key_mapping(external_id_mapping, row, expected_org, file_parser):
    # Given
    file_parser.external_id_mappings = external_id_mapping
    file_parser.configuration.data_provider = True

    parser = file_parser._get_parser()
    # When

    parsed = parser(row)

    # Then
    assert parsed["organization_id"] == expected_org


@pytest.mark.parametrize(
    argnames=("external_id_mapping", "row"),
    argvalues=[
        ({("tik", "tok"): 1}, {"client_id": "fizz", "customer_id": "fuzz"}),
        ({("tik"): 1}, {"client_id": "fizz", "customer_id": "fuzz"}),
    ],
    ids=["nothing-mapped-composite", "nothing-mapped-client-id-only"],
)
def test_composite_key_mapping_not_mapped(external_id_mapping, row, file_parser):
    # Given
    file_parser.external_id_mappings = external_id_mapping
    file_parser.configuration.data_provider = True

    parser = file_parser._get_parser()
    # When

    parsed = parser(row)

    # Then
    assert ParseErrorMessage.CLIENT_ID_NO_MAPPING in parsed["errors"]


def test_parse_file_parser(file_parser):
    # When
    num_valid, num_error = 0, 0
    for batch in file_parser.parse(batch_size=10):
        num_valid += len(batch.valid)
        num_error += len(batch.errors)
    # Then
    assert num_error == 0
    assert num_valid == 10


def test_parse_file_invalid_delimiter():
    # given
    sample_data = "foo..bar..buzz.."
    file: model.File = FileFactory.create()
    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id
    )
    headers = model.HeaderMapping()

    parser = EligibilityFileParser(
        file=file, configuration=config, data=sample_data, headers=headers
    )
    with pytest.raises(DelimiterError):
        for _ in parser.parse():
            continue


@pytest.mark.parametrize(
    argnames="name,expected",
    argvalues=[
        ("primary/clean.csv", {}),
        ("primary/email-null-12-email-invalid-22.csv", {"ParseErrorMessage.EMAIL": 1}),
        (
            "primary/missing-employee-id-17.csv",
            {"ParseErrorMessage.CORP_ID_MISS": 1},
        ),
        (
            "primary/row-offset-2-bad-date-9.csv",
            {"ParseErrorMessage.EXTRA_FIELD": 1, "ParseErrorMessage.DOB_PARSE": 1},
        ),
        ("secondary/clean.csv", {}),
        (
            "secondary/dependent-missing-17-email-missing-18.csv",
            {"ParseErrorMessage.EMAIL": 1},
        ),
        ("secondary/employee-id-changed-21.csv", {}),
        (
            "secondary/header-case-row-offset-2-date-format-8.csv",
            {"ParseErrorMessage.EXTRA_FIELD": 1},
        ),
    ],
)
@pytest.mark.asyncio
async def test_parse_file_with_known_issues(name, expected, manager):
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)
    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
    )
    headers = model.HeaderMapping()
    parser = EligibilityFileParser(
        file=file, configuration=config, data=data, headers=headers
    )

    errors = []

    for batch in parser.parse():
        for record in batch.errors:
            errors.extend(record.errors)

    assert Counter(errors) == expected


@pytest.mark.parametrize(
    argnames="name,expected",
    argvalues=[
        (
            "tertiary/data_provider_standard_client_id_header.csv",
            {"valid": 20, "errors": 0, "missing": 0},
        ),
        (
            "tertiary/data_provider_unmapped_external_ids.csv",
            {"valid": 15, "errors": 5, "missing": 0},
        ),
    ],
)
@pytest.mark.asyncio
async def test_parse_data_provider_file_with_sub_org(name, expected, manager):
    # Given
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)

    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
        data_provider=True,
    )
    headers = model.HeaderMapping()
    external_id_mappings = {"foo": 123, "bar": 234, "buzz": 456}
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=data,
        headers=headers,
        external_id_mappings=external_id_mappings,
    )

    # When
    num_valid, num_error, num_missing = 0, 0, 0
    for batch in parser.parse():
        num_valid += len(batch.valid)
        num_error += len(batch.errors)
        num_missing += len(batch.missing)

    # Then

    assert num_valid == expected["valid"]
    assert num_missing == expected["missing"]
    assert num_error == expected["errors"]


@pytest.mark.asyncio
async def test_parse_orphan_rows_with_line_no(manager):
    # Given
    name = "tertiary/data_provider_unmapped_external_ids.csv"
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)

    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
        data_provider=True,
    )
    headers = model.HeaderMapping()
    external_id_mappings = {"foo": 123, "bar": 234, "buzz": 456}
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=data,
        headers=headers,
        external_id_mappings=external_id_mappings,
    )

    error_lines = set()
    # When
    for batch in parser.parse():
        for error in batch.errors:
            error_lines.add(error.record["parse_line_no"])

    # Then
    assert error_lines == {16, 17, 18, 19, 20}


@pytest.mark.parametrize(
    argnames="name,expected",
    argvalues=[
        ("primary/clean.csv", set()),
        ("primary/email-null-12-email-invalid-22.csv", {22}),
        (
            "primary/missing-employee-id-17.csv",
            {17},
        ),
        ("secondary/clean.csv", set()),
        (
            "secondary/dependent-missing-17-email-missing-18.csv",
            {25},
        ),
        ("secondary/employee-id-changed-21.csv", set()),
    ],
)
@pytest.mark.asyncio
async def test_parse_error_with_correct_line_no(name, expected, manager):
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)
    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
    )
    headers = model.HeaderMapping()
    parser = EligibilityFileParser(
        file=file, configuration=config, data=data, headers=headers
    )

    error_lines = set()

    for batch in parser.parse():
        for error in batch.errors:
            error_lines.add(error.record["parse_line_no"])

    assert error_lines == expected


@pytest.mark.asyncio
async def test_parse_data_provider_file_with_no_sub_org(manager):
    # Given
    name = "tertiary/data_provider_no_client_data.csv"
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)

    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
    )
    headers = model.HeaderMapping()
    external_id_mappings = {"foo": 123, "bar": 234, "buzz": 456}
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=data,
        headers=headers,
        external_id_mappings=external_id_mappings,
    )

    # When
    valid_records = []
    num_valid, num_error, num_missing = 0, 0, 0
    for batch in parser.parse():
        valid_records.extend(batch.valid)
        num_valid += len(batch.valid)
        num_error += len(batch.errors)
        num_missing += len(batch.missing)

    # Then
    assert [num_valid, num_missing, num_error] == [20, 0, 0]

    # Ensure we did not overwrite the data provider's organizationID
    for rec in valid_records:
        assert rec.organization_id == file.organization_id


@pytest.mark.asyncio
async def test_parse_data_provider_file_non_standard_headers(manager):
    # Given
    name = "tertiary/data_provider_nonstandard_client_id_header.csv"
    data = await manager.get(name, "census-files")
    file: model.File = FileFactory.create(name=name, encoding="utf-8-sig")
    directory, _ = os.path.split(name)

    config: model.Configuration = ConfigurationFactory.create(
        organization_id=file.organization_id,
        directory_name=directory,
        data_provider=True,
    )
    headers = model.HeaderMapping(client_id="client-identifier")
    external_id_mappings = {"foo": 123, "bar": 234, "buzz": 456}
    parser = EligibilityFileParser(
        file=file,
        configuration=config,
        data=data,
        headers=headers,
        external_id_mappings=external_id_mappings,
    )

    # When
    valid_records = []
    num_valid, num_error, num_missing = 0, 0, 0
    for batch in parser.parse():
        valid_records.extend(batch.valid)
        num_valid += len(batch.valid)
        num_error += len(batch.errors)
        num_missing += len(batch.missing)

    # Then
    assert [num_valid, num_missing, num_error] == [15, 0, 0]

    # Ensure we successfully overwrote the data provider's organizationID
    for rec in valid_records:
        assert rec.organization_id != file.organization_id
        assert rec.record["data_provider_organization_id"] == file.organization_id


# endregion

# region single row parsing


@pytest.fixture()
def sample_row():
    return {
        "unique_corp_id": "1",
        "email": "email@blah.net",
        "date_of_birth": "1990-01-1",
        "first_name": "first_name",
        "last_name": "last_name",
        "gender": "F",
    }


@pytest.mark.parametrize(
    argnames="row,error",
    argvalues=[
        ({"email": "not an email"}, ParseErrorMessage.EMAIL),
        (
            {
                "email": "f2345678911234567892123456789312345678941234567895"
                "123456789612345@example.com"
            },
            ParseErrorMessage.EMAIL,  # 64 email username character limit
        ),
        (
            {
                "email": "foo@eabcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcdef.com"
            },
            ParseErrorMessage.EMAIL,  # 255 email domain character limit
        ),
        ({"date_of_birth": "not a dob"}, ParseErrorMessage.DOB_PARSE),
        ({"date_of_birth": "10/10/3000"}, ParseErrorMessage.DOB_FUT),
        ({"date_of_birth": "01/01/0001"}, ParseErrorMessage.DOB_UNKNOWN),
        ({}, ParseErrorMessage.DOB_MISS),
        ({}, ParseErrorMessage.PII_MISS),
        ({}, ParseErrorMessage.CORP_ID_MISS),
        ({_EXTRA_HEADER: ["read all about it"]}, ParseErrorMessage.EXTRA_FIELD),
    ],
)
def test_row_parsing_with_errors(row, error, row_parser):
    # When
    out = row_parser(row)
    # Then
    assert error in out["errors"]


@pytest.mark.parametrize(
    argnames="row",
    argvalues=[
        {"email": "foo@foo.com"},
        {"email": "f2345678911234567892123456789312345678941234567895@gmail.com"},
        {"email": "foo@foobar.foobar.com"},
        {"email": "foo@foobar.foobar"},
    ],
)
def test_row_parsing_email_valid(row, row_parser):
    # When
    out = row_parser(row)
    # Then
    assert ParseErrorMessage.EMAIL not in out["errors"]


@pytest.mark.parametrize(
    argnames="row,out",
    argvalues=[
        # Unknown country is a warning.
        (
            {"country": "Mordor"},
            {"country": "Mordor", "warnings": [ParseWarningMessage.COUNTRY]},
        ),
        # Correctly guessed the country
        ({"country": "United States"}, {"country": "USA"}),
        # Unknown State is a warning.
        (
            {"state": "Helles Planitia"},
            {"state": "Helles Planitia", "warnings": [ParseWarningMessage.STATE]},
        ),
        # Unknown Work State is a warning.
        (
            {"work_state": "Helles Planitia"},
            {"work_state": "Helles Planitia", "warnings": [ParseWarningMessage.STATE]},
        ),
        # Unknown Work State defaults to Known State.
        (
            {"state": "Oklahoma", "work_state": "Helles Planitia"},
            {"state": "OK", "work_state": "OK"},
        ),
        # Known Country is mapped, Unknown Work State defaults to Known State.
        (
            {
                "country": "United States",
                "state": "Oklahoma",
                "work_state": "Helles Planitia",
            },
            {"country": "USA", "state": "OK", "work_state": "OK"},
        ),
        # Unknown Country is a warning, Unknown Work State defaults to Known State.
        (
            {
                "country": "Martian Coalition",
                "state": "Oklahoma",
                "work_state": "Helles Planitia",
            },
            {
                "country": "Martian Coalition",
                "state": "OK",
                "work_state": "OK",
                "warnings": [ParseWarningMessage.COUNTRY],
            },
        ),
        # No eligibility date provided
        (
            {"employee_eligibility_date": ""},
            {"employee_eligibility_date": None},
        ),
        # Invalid eligibility date
        (
            {"employee_eligibility_date": "not a date"},
            {"employee_eligibility_date": None},
        ),
        # Valid eligiblity date- mm/dd/yy
        (
            {"employee_eligibility_date": "10/1/88"},
            {"employee_eligibility_date": datetime.date(1988, 10, 1)},
        ),
        # Valid eligiblity date- YYYY/MM/DD
        (
            {"employee_eligibility_date": "1988-10-01"},
            {"employee_eligibility_date": datetime.date(1988, 10, 1)},
        ),
        # Extra value
        ({"fooobar": "1234"}, {}),
        # ClientID provided, but no external mappings provided
        ({"client_id": "12345"}, {}),
        # Custom attributes
        (
            {"custom_attributes.sub_population_identifier": "7357"},
            {"custom_attributes": {"sub_population_identifier": "7357"}},
        ),
        ({"gender": "Female"}, {"gender_code": "F"}),  # gender header
        # Healthplan fields
        (
            {
                "maternity_indicator_date": "1900-01-01",
                "maternity_indicator": "True",
                "delivery_indicator_date": "1900-01-02",
                "delivery_indicator": "False",
                "fertility_indicator_date": "1900-01-03",
                "fertility_indicator": "Test",
                "p_and_p_indicator": "foobar",
                "client_name": "Maven",
            },
            {
                "custom_attributes": {
                    "health_plan_values": {
                        "client_name": "Maven",
                        "delivery_indicator": "False",
                        "delivery_indicator_date": "1900-01-02",
                        "fertility_indicator": "Test",
                        "fertility_indicator_date": "1900-01-03",
                        "maternity_indicator": "True",
                        "maternity_indicator_date": "1900-01-01",
                        "p_and_p_indicator": "foobar",
                    },
                    "sub_population_identifier": None,
                }
            },
        ),
    ],
)
def test_row_parsing(sample_row, row, out, row_parser):
    row = {**row, **sample_row}
    parsed = row_parser(row)
    assert {k: parsed[k] for k in out} == out


@pytest.mark.parametrize(
    argnames="row,out",
    argvalues=[
        (
            {"unique_corp_id": "123-45-6789"},
            {"unique_corp_id": mock.ANY, "warnings": [ParseWarningMessage.SSN]},
        ),
        (
            {"unique_corp_id": "123456789"},
            {"unique_corp_id": "123456789", "warnings": [ParseWarningMessage.SSN]},
        ),
        (
            {"unique_corp_id": "abc-45-6789"},
            {"unique_corp_id": "abc-45-6789", "warnings": []},
        ),
    ],
)
def test_row_parsing_ssn(sample_row, row, out, row_parser):
    del sample_row["unique_corp_id"]
    row = {**row, **sample_row}
    parsed = row_parser(row)
    assert {k: parsed[k] for k in out} == out


def test_row_parsing_ssn_overwrite(sample_row, row_parser):
    # Given
    input_ssn = "123-45-6789"
    del sample_row["unique_corp_id"]
    row = {
        **sample_row,
        **{"unique_corp_id": input_ssn},
    }

    # When
    parsed = row_parser(row)

    # Then
    assert parsed["unique_corp_id"] != input_ssn
    assert parsed["unique_corp_id"] == parsed["record"]["unique_corp_id"]
    assert parsed["record"]["id-resembling-hyphenated-ssn"] is True


@pytest.mark.parametrize(
    argnames="row,out",
    argvalues=[
        (
            {"date_of_birth": "0001-01-01"},
            {"date_of_birth": pendulum.date(1900, 1, 1)},
        ),  # unknown date
        (
            {"date_of_birth": "1999-01-01"},
            {"date_of_birth": pendulum.date(1999, 1, 1)},
        ),  # valid date
        ({"date_of_birth": "foobar"}, {"date_of_birth": "foobar"}),  # invalid date
        (
            {"date_of_birth": "01/01/99"},
            {"date_of_birth": pendulum.date(1999, 1, 1)},
        ),  # 2 digit year  format
    ],
)
def test_row_parsing_dates(sample_row, row, out, row_parser):
    del sample_row["date_of_birth"]
    row = {**row, **sample_row}
    parsed = row_parser(row)
    assert {k: parsed[k] for k in out} == out


# endregion

# region row parsing for data providers
def test_row_parsing_data_provider_valid_external_org_id(
    default_external_id_mappings, file_parser, sample_row
):
    # Given
    # Ensure we are looking at a dataprovider for this test
    file_parser.configuration.data_provider = True
    row_parser = file_parser._get_parser()

    row = {**sample_row, **{"client_id": default_external_id_mappings["external_id"]}}
    expected = {
        "organization_id": default_external_id_mappings["internal_maven_org_id"]
    }
    # When
    parsed = row_parser(row)
    # Then
    for k, v in expected.items():
        assert parsed[k] == v


def test_row_parsing_data_provider_valid_client_id_and_customer_id(
    default_external_id_with_customer_id_mappings,
    file_parser_with_composite_external_id,
    sample_row,
):
    # Given
    # Ensure we are looking at a dataprovider for this test
    file_parser_with_composite_external_id.configuration.data_provider = True
    row_parser = file_parser_with_composite_external_id._get_parser()

    client_id, customer_id = default_external_id_with_customer_id_mappings[
        "external_id"
    ].split(":")
    organization_id = default_external_id_with_customer_id_mappings[
        "internal_maven_org_id"
    ]
    row = {
        **sample_row,
        **{
            "client_id": client_id,
            "customer_id": customer_id,
        },
    }
    expected = {"organization_id": organization_id}
    # When
    parsed = row_parser(row)
    # Then
    for k, v in expected.items():
        assert parsed[k] == v


def test_row_parsing_data_provider_no_client_id_provided(file_parser, sample_row):
    # Replicate us getting rows from a data provider with data-provider only rows- i.e. no sub-org rows

    # Given
    row_parser = file_parser._get_parser()
    row = sample_row
    # Ensure we do not remap the original organization_id in this case
    expected = {"organization_id": file_parser.configuration.organization_id}

    # When
    parsed = row_parser(row)

    # Then
    for k, v in expected.items():
        assert parsed[k] == v


def test_row_parsing_data_provider_missing_external_id_mapping(file_parser, sample_row):
    # Given
    file_parser.configuration.data_provider = True
    row_parser = file_parser._get_parser()

    # Replicate us getting rows from a data provider with data-provider only rows- i.e. no sub-org rows
    row = {**sample_row, **{"client_id": "random_client_id"}}

    # We expect to raise an error that we got a row without an external_id mapping
    expected = {"errors": [ParseErrorMessage.CLIENT_ID_NO_MAPPING.value]}

    # When
    parsed = row_parser(row)

    # Then
    for k, v in expected.items():
        assert parsed[k] == v


def test_row_parsing_data_provider_no_external_id_mappings_configured(
    file_parser, sample_row
):
    # Given
    file_parser.external_id_mappings = {}
    file_parser.configuration.data_provider = True
    row_parser = file_parser._get_parser()

    # Replicate us getting rows from a data provider with data-provider only rows- i.e. no sub-org rows
    row = {**sample_row, **{"client_id": "random_client_id"}}

    # We expect to raise an error that we got a row without an external_id mapping
    expected = {"errors": [ParseErrorMessage.CLIENT_ID_NO_MAPPING]}

    # When
    parsed = row_parser(row)

    # Then
    for k, v in expected.items():
        assert parsed[k] == v


@pytest.mark.parametrize(
    "flag_value,expected",
    [
        (True, {"errors": [ParseErrorMessage.CLIENT_ID_NO_MAPPING]}),
        (False, {}),
    ],
)
def test_row_parsing_optum_data_provider_no_external_id_mappings_configured(
    flag_value, expected, file_parser, sample_row
):
    # Given
    file_parser.external_id_mappings = {}
    file_parser.configuration.data_provider = True
    file_parser.configuration.directory_name = "optum-data-provider"
    row_parser = file_parser._get_parser()

    # Replicate us getting rows from a data provider with data-provider only rows- i.e. no sub-org rows
    row = {**sample_row, **{"client_id": "random_client_id"}}

    # When
    with mock.patch("maven.feature_flags.bool_variation") as mock_bool_variation:
        mock_bool_variation.return_value = flag_value
        parsed = row_parser(row)

        # Then
        for k, v in expected.items():
            assert parsed[k] == v


# endregion


def test_chunker_no_remainder():
    # Given
    numbers = range(100)
    chunk_size = 10

    # When
    chunks = []
    for chunk in chunker(numbers, chunk_size):
        chunks.append(chunk)

    # Then
    # All chunks are same size
    assert all(len(chunk) == chunk_size for chunk in chunks)
    # We get all the elements
    assert set(numbers) == set([num for chunk in chunks for num in chunk])


def test_chunker_remainder():
    # Given
    size = 100
    numbers = range(size)
    chunk_size = 11

    # When
    chunks = []
    for chunk in chunker(numbers, chunk_size):
        chunks.append(chunk)

    # Then
    # All but the last are of length chunk_size
    assert all(len(chunk) == chunk_size for chunk in chunks[:-1])
    # Last one is of length remainder
    assert len(chunks[-1]) == size % chunk_size
    # We get all the elements
    assert set(numbers) == set([num for chunk in chunks for num in chunk])


def test_chunker_empty():
    # Given
    size = 0
    numbers = range(size)
    chunk_size = 10

    # When
    chunks = []
    for chunk in chunker(numbers, chunk_size):
        chunks.append(chunk)

    # Then
    assert not chunks


def test_row_with_no_dob_for_orgs_that_dont_send_dob(
    sample_row, row_parser, row_parser_skip_dob
):
    del sample_row["date_of_birth"]

    row_without_dob = {"work_state": "WA", **sample_row}

    # When
    parsed_row_no_dob = row_parser(row_without_dob)
    parsed_row_skip_dob = row_parser_skip_dob(row_without_dob)

    # Then
    assert ParseErrorMessage.DOB_MISS in parsed_row_no_dob["errors"]
    assert parsed_row_skip_dob["errors"] == []
    assert parsed_row_skip_dob["date_of_birth"] == DEFAULT_DATE_OF_BIRTH


def test_row_with_blank_dob_for_orgs_that_dont_send_dob(
    sample_row, row_parser, row_parser_skip_dob
):
    sample_row["date_of_birth"] = ""
    row_with_empty_dob = {"work_state": "WA", **sample_row}

    # When
    parsed_row_empty_dob = row_parser(row_with_empty_dob)
    parsed_row_skip_empty_dob = row_parser_skip_dob(row_with_empty_dob)

    # Then
    assert ParseErrorMessage.DOB_PARSE in parsed_row_empty_dob["errors"]
    assert parsed_row_skip_empty_dob["errors"] == []


@pytest.mark.parametrize(
    argnames=("pii_key", "expected_error"),
    argvalues=[
        ("email", ParseErrorMessage.PII_MISS),
        ("first_name", ParseErrorMessage.PII_MISS),
        ("last_name", ParseErrorMessage.PII_MISS),
        ("unique_corp_id", ParseErrorMessage.CORP_ID_MISS),
    ],
    ids=[
        "missing_email",
        "missing_first_name",
        "missing_last_name",
        "missing_unique_corp_id",
    ],
)
def test_row_with_missing_pii_for_orgs_that_dont_send_dob(
    sample_row, pii_key, expected_error, row_parser_skip_dob
):
    # Given
    sample_row["work_state"] = "WA"
    del sample_row["date_of_birth"]
    del sample_row[pii_key]
    row_with_no_dob = {**sample_row}

    # When
    parsed_row_skip_no_dob = row_parser_skip_dob(row_with_no_dob)

    # Then
    assert expected_error in parsed_row_skip_no_dob["errors"]
