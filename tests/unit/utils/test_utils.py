from unittest import mock

import pytest
from tests.factories import data_models as factory

from app.utils.utils import (
    HASH_VERSION,
    detect_and_sanitize_possible_ssn,
    generate_hash_for_external_record,
    generate_hash_for_file_based_record,
    resolve_gender_code,
)


@pytest.fixture
def external_record():
    member = factory.MemberFactory.create()
    address = factory.AddressFactory.create()
    return {
        "first_name": member.first_name,
        "last_name": member.last_name,
        "date_of_birth": member.date_of_birth,
        "work_state": member.work_state,
        "email": member.email,
        "unique_corp_id": member.unique_corp_id,
        "effective_range": member.effective_range,
        "do_not_contact": member.do_not_contact,
        "gender_code": member.gender_code,
        "organization_id": member.organization_id,
        "record": member.record,
        "dependent_id": member.dependent_id,
        "employer_assigned_id": member.unique_corp_id,
        "external_id": member.unique_corp_id,
        "external_name": "foobar",
        "custom_attributes": {"foo": "bar"},
        "address": address,
    }


@pytest.fixture
def file_based_record():
    member = factory.MemberFactory.create(effective_range=None)
    return {
        "first_name": member.first_name,
        "last_name": member.last_name,
        "date_of_birth": member.date_of_birth,
        "work_state": member.work_state,
        "email": member.email,
        "unique_corp_id": member.unique_corp_id,
        "effective_range": member.effective_range,
        "do_not_contact": member.do_not_contact,
        "gender": member.gender_code,
        "organization_id": member.organization_id,
        "record": {"file_id": 1234},
        "dependent_id": member.dependent_id,
    }


def test_generate_hash_for_external_record_same_values(external_record):
    # Given/When
    hash_result = generate_hash_for_external_record(external_record)
    hash_result_2 = generate_hash_for_external_record(external_record)

    # Then
    assert hash_result == hash_result_2
    assert hash_result == (mock.ANY, HASH_VERSION)


def test_generate_hash_for_external_record_different_values(external_record):
    # Given/When
    hash_result = generate_hash_for_external_record(external_record)
    external_record["first_name"] = "different_valueeeee"
    hash_result_2 = generate_hash_for_external_record(external_record)

    # Then
    assert hash_result != hash_result_2


def test_generate_hash_for_external_record_different_values_record(external_record):
    # Given/When
    hash_result = generate_hash_for_external_record(external_record)
    external_record["record"] = {"different": "valueeeee"}
    hash_result_2 = generate_hash_for_external_record(external_record)

    # Then
    assert hash_result != hash_result_2


def test_generate_hash_for_external_record_ensure_values_removed_(external_record):
    # Given/When
    external_record["record"]["received_ts"] = "foobar"
    hash_result = generate_hash_for_external_record(external_record)

    external_record["record"]["received_ts"] = "barfoo"
    hash_result_2 = generate_hash_for_external_record(external_record)

    # Then
    assert hash_result == hash_result_2


def test_generate_hash_for_file_record_same_values(file_based_record):
    # Given/When
    hash_result = generate_hash_for_file_based_record(file_based_record)
    file_based_record["record"]["file_id"] = 23456
    hash_result_2 = generate_hash_for_file_based_record(file_based_record)

    # Then
    assert hash_result == hash_result_2
    assert hash_result == (mock.ANY, HASH_VERSION)


def test_generate_hash_for_file_record_different_dict_keys(file_based_record):
    # Given/When
    hash_result = generate_hash_for_file_based_record(file_based_record)
    file_based_record["first_name"] = "different_valueeeee"
    file_based_record["record"] = {"different_value": True, "pls_different": "True"}
    hash_result_2 = generate_hash_for_file_based_record(file_based_record)

    # Then
    assert hash_result != hash_result_2


def test_generate_hash_for_file_record_different_dict_values(file_based_record):
    # Given/When
    file_based_record["record"] = {"different_value": True, "different_string": "Foo"}
    hash_result = generate_hash_for_file_based_record(file_based_record)
    file_based_record["record"] = {"different_value": False, "different_string": "Bar"}
    hash_result_2 = generate_hash_for_file_based_record(file_based_record)

    # Then
    assert hash_result != hash_result_2


@pytest.mark.parametrize(
    argnames="custom_attribute",
    argvalues=[
        ({"custom_attributes": {"foo": "bar"}}),
        ({"custom_attributes": {"health_plan_values": {"value1": "foo"}}}),
    ],
)
def test_generate_hash_for_file_record_same_custom_attributes(
    file_based_record, custom_attribute
):
    # Given/When
    file_based_record["record"] = custom_attribute
    hash_result = generate_hash_for_file_based_record(file_based_record)
    file_based_record["record"] = custom_attribute
    hash_result_2 = generate_hash_for_file_based_record(file_based_record)

    # Then
    assert hash_result == hash_result_2


@pytest.mark.parametrize(
    argnames="custom_attribute_1,custom_attribute_2",
    argvalues=[
        ({"custom_attributes": {"foo": "bar"}}, {"custom_attributes": {"bar": "foo"}}),
        (
            {"custom_attributes": {"health_plan_values": {"value1": "foo"}}},
            {"custom_attributes": {"health_plan_values": {"value2": "foobar"}}},
        ),
    ],
)
def test_generate_hash_for_file_record_different_custom_attributes(
    file_based_record, custom_attribute_1, custom_attribute_2
):
    # Given/When
    file_based_record["record"] = custom_attribute_1
    hash_result = generate_hash_for_file_based_record(file_based_record)
    file_based_record["record"] = custom_attribute_2
    hash_result_2 = generate_hash_for_file_based_record(file_based_record)

    # Then
    assert hash_result != hash_result_2


@pytest.mark.parametrize(
    argnames="input,expected",
    argvalues=[
        ("", ""),
        ("Female", "F"),
        ("F", "F"),
        ("F    ", "F"),
        ("MALE", "M"),
        ("M", "M"),
        ("Unknown", "U"),
        ("U", "U"),
        ("Other", "O"),
        ("o", "O"),
        ("foo", "foo"),
    ],
    ids=[
        "empty_string",
        "female_standard",
        "female_single_char",
        "female_whitespace",
        "male_standard",
        "false_single_char",
        "unknown_standard",
        "unknown_char",
        "other_standard",
        "other_char",
        "random",
    ],
)
def test_resolve_gender_code(input, expected):
    # When
    resolved = resolve_gender_code(input)
    # Then
    assert expected == resolved


@pytest.mark.parametrize(
    argnames="input,expected",
    argvalues=[
        ("foobar", False),
        ("123456879", True),
        ("666112345", False),
        ("912-34-1233", False),
        ("000-12-1234", False),
        ("123-00-1234", False),
        ("123-45-0000", False),
    ],
)
def test_detect_ssn_no_hyphen(input, expected):
    # When
    sanitized_string, detected = detect_and_sanitize_possible_ssn(
        input, organization_id=1
    )
    # Then
    assert sanitized_string is None
    assert detected == expected


@pytest.mark.parametrize(
    argnames="input,expected",
    argvalues=[("123-45-6789", True)],
)
def test_detect_ssn_hyphen(input, expected):
    # When
    sanitized_string, detected = detect_and_sanitize_possible_ssn(
        input, organization_id=1
    )
    # Then
    assert sanitized_string is not None
    assert detected == expected
