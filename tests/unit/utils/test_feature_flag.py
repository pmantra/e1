from unittest import mock

import pytest

from app.eligibility import constants as e9y_constants
from app.utils import feature_flag


@pytest.mark.parametrize(
    argnames="input, enabled_orgs,expected",
    argvalues=[
        (1, [1], True),
        (2, [], False),
        (3, [2], False),
    ],
)
def test_organization_enabled_for_e9y_2_read(input, enabled_orgs, expected):
    with mock.patch("maven.feature_flags.json_variation", return_value=enabled_orgs):
        res = feature_flag.organization_enabled_for_e9y_2_read(input)
        assert res == expected


@pytest.mark.parametrize(
    argnames="input, enabled_orgs,expected",
    argvalues=[
        (1, [1], True),
        (2, [], False),
        (3, [2], False),
    ],
)
def test_organization_enabled_for_e9y_2_write(input, enabled_orgs, expected):
    with mock.patch("maven.feature_flags.json_variation", return_value=enabled_orgs):
        res = feature_flag.organization_enabled_for_e9y_2_write(input)
        assert res == expected


@pytest.fixture
def mock_json_variation():
    with mock.patch("maven.feature_flags.json_variation") as mock_var:
        yield mock_var


def test_all_orgs_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {"enabled_all_orgs": True, "organizations": {}}

    organization_ids = frozenset([101, 102, 103])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is True


def test_specific_orgs_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {
        "enabled_all_orgs": False,
        "organizations": [101, 102],
    }

    organization_ids = frozenset([101, 102])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is True


def test_not_all_orgs_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {
        "enabled_all_orgs": False,
        "organizations": [101, 102],
    }

    organization_ids = frozenset([101, 102, 103])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is False


def test_empty_organization_ids(mock_json_variation):
    # Given
    mock_json_variation.return_value = {
        "enabled_all_orgs": False,
        "organizations": [101, 102],
    }

    organization_ids = frozenset()

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is False


def test_no_organizations_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {"enabled_all_orgs": False, "organizations": []}

    organization_ids = frozenset([101, 102])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is False


def test_flag_with_empty_orgs_but_all_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {"enabled_all_orgs": True, "organizations": []}

    organization_ids = frozenset([101, 102, 103])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is True


def test_flag_with_empty_orgs_and_not_all_enabled(mock_json_variation):
    # Given
    mock_json_variation.return_value = {"enabled_all_orgs": False, "organizations": []}

    organization_ids = frozenset([101, 102, 103])

    # When
    result = feature_flag.are_all_organizations_enabled_for_overeligibility(
        organization_ids
    )

    # Then
    assert result is False


@pytest.mark.parametrize(
    "flag_value,expected",
    [
        (True, True),  # Write is disabled
        (False, False),  # Write is enabled
    ],
)
def test_is_write_disabled(flag_value, expected):
    with mock.patch("maven.feature_flags.bool_variation") as mock_bool_variation:
        mock_bool_variation.return_value = flag_value

        result = feature_flag.is_write_disabled()

        mock_bool_variation.assert_called_once_with(
            e9y_constants.E9yFeatureFlag.E9Y_DISABLE_WRITE,
            default=False,
        )
        assert result == expected


@pytest.mark.parametrize(
    "flag_value,expected",
    [
        (True, True),  # Write is disabled
        (False, False),  # Write is enabled
    ],
)
def test_is_optum_file_logging_enabled(flag_value, expected):
    with mock.patch("maven.feature_flags.bool_variation") as mock_bool_variation:
        mock_bool_variation.return_value = flag_value

        result = feature_flag.is_optum_file_logging_enabled()

        mock_bool_variation.assert_called_once_with(
            e9y_constants.E9yFeatureFlag.RELEASE_OPTUM_FILE_LOGGING_SWITCH,
            default=False,
        )
        assert result == expected
