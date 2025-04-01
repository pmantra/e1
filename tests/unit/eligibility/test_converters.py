import pendulum
import pytest

from app.eligibility import convert


@pytest.mark.parametrize(
    argnames="value",
    argvalues={*convert._BOOLS, *(str(b).upper() for b in convert._BOOLS)},
)
def test_bool_converter(value):
    assert convert.to_bool(value) is True


@pytest.mark.parametrize(
    argnames="value",
    argvalues={
        *convert._BENEFICIARY_FLAGS,
        *(str(b).upper() for b in convert._BENEFICIARY_FLAGS),
        *(str(b).replace("+", " + ").capitalize() for b in convert._BENEFICIARY_FLAGS),
    },
)
def test_benificiaries_enabled_converter(value):
    assert convert.to_beneficiaries_enabled(value) is True


@pytest.mark.parametrize(
    argnames="value",
    argvalues={
        *convert._PREGNANCY_SEXES,
        *(str(b).upper() for b in convert._PREGNANCY_SEXES),
    },
)
def test_can_get_pregnant_converter(value):
    assert convert.to_can_get_pregnant(value) is True


@pytest.mark.parametrize(
    argnames="value,expected",
    argvalues=[
        ("01/01/01", pendulum.Date(2001, 1, 1)),
        ("01/01/50", pendulum.Date(1950, 1, 1)),
        ("1/1/50", pendulum.Date(1950, 1, 1)),
        ("1/01/50", pendulum.Date(1950, 1, 1)),
        ("1/01/50 00:00:00", pendulum.Date(1950, 1, 1)),
        ("2000-01-01", pendulum.Date(2000, 1, 1)),
        ("2000-01-01 00:00:00", pendulum.Date(2000, 1, 1)),
        ("", pendulum.Date(1, 1, 1)),
        ("01/01/0001", None),
        ("01-01-0001", None),
        ("0001/01/01", None),
    ],
)
def test_to_date(value, expected):
    assert convert.to_date(value) == expected


@pytest.mark.parametrize(
    argnames="value,expected",
    argvalues=[
        ("AU", "AUS"),
        ("US", "USA"),
        (" US", "USA"),
        ("USA", "USA"),
        ("United States", "USA"),
        ("United States of America", "USA"),
        ("US of A", convert.COUNTRY_UNKNOWN),
        ("", convert.COUNTRY_UNKNOWN),
        (None, convert.COUNTRY_UNKNOWN),
    ],
)
def test_to_country(value, expected):
    assert convert.to_country_code(value) == expected


@pytest.mark.parametrize(
    argnames="value,expected",
    argvalues=[
        ("OK", "OK"),
        ("Oklahoma", "OK"),
        ("US-OK", "OK"),
        ("ok", "OK"),
        (" ok", "OK"),
        ("NY", "NY"),
        ("New York", "NY"),
        ("CO", "CO"),
        ("N", convert.STATE_UNKNOWN),
        ("", convert.STATE_UNKNOWN),
        (None, convert.STATE_UNKNOWN),
    ],
)
def test_to_state(value, expected):
    assert convert.to_state_code(value) == expected
