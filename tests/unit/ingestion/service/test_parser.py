from __future__ import annotations

import datetime
from typing import Dict

import pytest
from ingestion import model, repository, service


class TestParse:
    @staticmethod
    def test_parse_valid(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.record == {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000-01-01",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
        }

    @staticmethod
    def test_parse_contains_extra(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            repository.EXTRA_HEADER: "this is an extra field",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.errors == [service.ParseErrorMessage.EXTRA_FIELD]

    @staticmethod
    @pytest.mark.parametrize(
        argnames="email",
        argvalues=[
            ("this is an not an email field"),
            (
                "foo@eabcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd.abcd."
                "abcd.abcd.abcd.abcd.abcdef.com"
            ),
            (
                "f2345678911234567892123456789312345678941234567895"
                "123456789612345@example.com"
            ),
        ],
        ids=[
            "invalid-email",
            "email-exceeds-255-chars-domain-limit",
            "email-exceeds-64-char-username-limit",
        ],
    )
    def test_parse_bad_email(parser_service: service.EligibilityFileParser, email: str):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": email,
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.errors == [service.ParseErrorMessage.EMAIL]

    @staticmethod
    def test_parse_empty_email(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.warnings == [service.ParseWarningMessage.EMAIL]

    @staticmethod
    @pytest.mark.parametrize(
        argnames="dob,expected_error",
        argvalues=[
            ("not a dob", service.ParseErrorMessage.DOB_PARSE),
            ("14-34-2304", service.ParseErrorMessage.DOB_PARSE),
            ("12-01-2304", service.ParseErrorMessage.DOB_FUTURE),
        ],
        ids=["invalid-date-1", "invalid-date-2", "dob-in-future"],
    )
    def test_parse_bad_dob(
        parser_service: service.EligibilityFileParser,
        dob: str,
        expected_error: service.ParseErrorMessage,
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": dob,
            "unique_corp_id": "1",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.errors == [expected_error]

    @staticmethod
    def test_parse_missing_dob(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "unique_corp_id": "1",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert service.ParseErrorMessage.DOB_MISSING in parsed.errors

    @staticmethod
    def test_parse_missing_pii(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "unique_corp_id": "1",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert service.ParseErrorMessage.PII_MISSING in parsed.errors

    @staticmethod
    def test_parse_missing_unique_corp_id(
        parser_service: service.EligibilityFileParser,
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert service.ParseErrorMessage.CORP_ID_MISSING in parsed.errors

    @staticmethod
    @pytest.mark.parametrize(
        argnames="dependent_id",
        argvalues=[("FDMKNFDS"), ("  FDMKNFDS   ")],
        ids=["valid-id", "valid-id-with-whitespace"],
    )
    def test_parse_dependent_id(
        parser_service: service.EligibilityFileParser,
        dependent_id: str,
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "05-23-1993",
            "unique_corp_id": "1",
            "dependent_id": dependent_id,
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.record["dependent_id"] == "FDMKNFDS"

    @staticmethod
    def test_parse_country_valid(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            "country": "United States",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.record["country"] == "USA"

    @staticmethod
    def test_parse_country_unknown(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            "country": "Mordor",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert service.ParseWarningMessage.COUNTRY in parsed.warnings

    @staticmethod
    @pytest.mark.parametrize(
        argnames="state_field",
        argvalues=[("state"), ("work_state")],
        ids=["invalid-state", "invalid-work-state"],
    )
    def test_parse_state_unknown(
        parser_service: service.EligibilityFileParser, state_field: str
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            state_field: "Old York",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert service.ParseWarningMessage.STATE in parsed.warnings

    @staticmethod
    def test_parse_work_state_defaults_to_valid_state(
        parser_service: service.EligibilityFileParser,
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            "state": "New York",
            "work_state": "Old York",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.record["work_state"] == "NY"

    @staticmethod
    @pytest.mark.parametrize(
        argnames="employee_eligibility_date,expected_parsed_value",
        argvalues=[
            ("", None),
            ("not a date", None),
            ("10/1/88", datetime.date(1988, 10, 1)),
            ("1988-10-01", datetime.date(1988, 10, 1)),
        ],
        ids=[
            "empty-string",
            "not-a-date",
            "valid-date-format-a",
            "valid-date-format-b",
        ],
    )
    def test_employee_eligibility_date(
        parser_service: service.EligibilityFileParser,
        employee_eligibility_date: str,
        expected_parsed_value: datetime.datetime | None,
    ):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            "employee_eligibility_date": employee_eligibility_date,
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert parsed.record["employee_eligibility_date"] == expected_parsed_value

    @staticmethod
    def test_parse_extra_values(parser_service: service.EligibilityFileParser):
        # Given
        row: Dict = {
            "first_name": "Ted",
            "last_name": "Lasso",
            "date_of_birth": "2000/1/1",
            "unique_corp_id": "1",
            "email": "ted@afcrichmond.com",
            "favorite_color": "Blue",
        }

        # When
        parsed: model.ParsedRecord = parser_service.parse(row=row)

        # Then
        assert "favorite_color" in parsed.record
