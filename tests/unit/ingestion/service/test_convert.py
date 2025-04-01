import datetime

import pytest
from faker import Faker
from ingestion import model, service
from tests.factories import optum as optum_factories


def generate_mock_addresses(address_type_code):
    faker = Faker()
    mock_address_1 = faker.street_address()
    mock_city = faker.city()
    mock_state = faker.state()
    mock_zip = faker.zipcode()

    optum_address = model.OptumAddress(
        addressTypeCode=address_type_code,
        addressTypeDescription="Foobar",
        addressLine1=mock_address_1,
        postalCode=mock_zip,
        city=mock_city,
        state=mock_state,
        isoCountryCode="USA",
    )
    address_object = model.Address(
        address_1=mock_address_1,
        address_2=None,
        city=mock_city,
        state=mock_state,
        postal_code=mock_zip,
        postal_code_suffix=None,
        country_code="USA",
    )
    return [optum_address, address_object]


def test_resolve_member_address_oeedm_priority():
    addresses = [generate_mock_addresses(code) for code in service.OEEDM_ADDRESS_CODES]

    # Ensure we give highest priority to appropriate address
    returned_address = service.resolve_member_address(a[0] for a in addresses)
    assert returned_address == addresses[0][1]


def test_resolve_member_address_employer_priority():
    # Given
    addresses = [
        generate_mock_addresses(code) for code in service.EMPLOYER_ADDRESS_CODES
    ]
    # When
    # Ensure we give highest priority to appropriate address
    returned_address = service.resolve_member_address(a[0] for a in addresses)
    # Then
    assert returned_address == addresses[0][1]


def test_resolve_member_address_multiple_data_sources():
    # Given
    addresses = [generate_mock_addresses(code) for code in service.ADDRESS_CODES]
    # When
    result = service.resolve_member_address(a[0] for a in addresses)
    # Then
    assert result is None


@pytest.mark.parametrize(
    argnames="input,expected",
    argvalues=[
        ("", ""),
        ("TRUE", "True"),
        ("T", "True"),
        ("T    ", "True"),
        ("FALSE", "False"),
        ("F", "False"),
        ("foo", "foo"),
    ],
    ids=[
        "empty_string",
        "true_standard",
        "true_single_char",
        "true_whitespace",
        "false_standard",
        "false_single_char",
        "unknown_value",
    ],
)
def test_resolve_do_not_contact(input, expected):
    # When
    resolved = service.resolve_do_not_contact(input)
    # Then
    assert expected == resolved


@pytest.mark.parametrize(
    argnames="policies,today,expected",
    argvalues=[
        # "one_policy",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "one_policy_9999",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate="2005-01-01",
                    terminationDate="9999-12-31"
                    # effectiveDate=datetime.date(2005, 1, 1), terminationDate=datetime.date(9999, 12, 31)
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(9999, 12, 31)
            ),
        ),
        # "one_policy_effective_date_empty_string",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate="", terminationDate=datetime.date(2035, 1, 1)
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2020, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "one_policy_effective_date_none",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=None, terminationDate=datetime.date(2035, 1, 1)
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2020, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "one_policy_termination_date_empty_string",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1), terminationDate=""
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=None
            ),
        ),
        # "one_policy_termination_date_none",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1), terminationDate=None
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=None
            ),
        ),
        # "one_policy_effective_and_term_dates_nones",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=None, terminationDate=None
                )
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2020, 1, 1), upper=None
            ),
        ),
        # "two_sorted_policies_one_past_one_present",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2015, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2025, 1, 1)
            ),
        ),
        # "two_sorted_policies_one_present_one_future",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2025, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2015, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "two_sorted_policies_one_past_one_future",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2015, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2025, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "two_sorted_policies_both_present",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "two_unsorted_policies_one_present_one_past",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2015, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2025, 1, 1)
            ),
        ),
        # "two_unsorted_policies_one_future_one_present",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2025, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2015, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "two_unsorted_policies_one_future_one_past",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2015, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2025, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "two_unsorted_policies_both_present",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
        # "three_sorted_policies_all_valid_second_with_latest_term_date",
        (
            [
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2005, 1, 1),
                    terminationDate=datetime.date(2025, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2020, 1, 1),
                    terminationDate=datetime.date(2035, 1, 1),
                ),
                optum_factories.OptumPolicyFactory(
                    effectiveDate=datetime.date(2021, 1, 1),
                    terminationDate=datetime.date(2030, 1, 1),
                ),
            ],
            datetime.date(2020, 1, 1),
            optum_factories.EffectiveRangeFactory(
                lower=datetime.date(2005, 1, 1), upper=datetime.date(2035, 1, 1)
            ),
        ),
    ],
    ids=[
        "one_policy",
        "one_policy_9999",
        "one_policy_effective_date_empty_string",
        "one_policy_effective_date_none",
        "one_policy_termination_date_empty_string",
        "one_policy_termination_date_none",
        "one_policy_effective_and_term_dates_nones",
        "two_sorted_policies_one_past_one_present",
        "two_sorted_policies_one_present_one_future",
        "two_sorted_policies_one_past_one_future",
        "two_sorted_policies_both_present",
        "two_unsorted_policies_one_present_one_past",
        "two_unsorted_policies_one_future_one_present",
        "two_unsorted_policies_one_future_one_past",
        "two_unsorted_policies_both_present",
        "three_sorted_policies_all_valid_second_with_latest_term_date",
    ],
)
def test_resolve_effective_range(policies, today, expected):
    # Given
    # policies and today parametrized

    # When
    effective_range = service.resolve_effective_range(policies=policies, today=today)

    # Then
    assert effective_range == expected


@pytest.mark.parametrize(
    argnames="policies,today",
    argvalues=[
        # "policies_none",
        (None, datetime.date.today()),
        # "empty_policies",
        ([], datetime.date.today()),
        # "upper_less_than_lower_error",
        (
            [
                model.OptumPolicy(
                    effectiveDate=datetime.date(2015, 1, 1),
                    terminationDate=datetime.date(2005, 1, 1),
                )
            ],
            datetime.date(2020, 1, 1),
        ),
    ],
    ids=[
        "policies_none",
        "empty_policies",
        "upper_less_than_lower_error",
    ],
)
def test_resolve_effective_range_errors(policies, today):
    # Given
    # policies and today parametrized

    # When
    effective_range = service.resolve_effective_range(policies=policies, today=today)

    # Then
    assert effective_range is None
