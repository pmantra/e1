from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest
from tests.factories import data_models as factory

from app.eligibility import errors
from app.utils.eligibility_validation import (
    check_member_org_active,
    check_member_org_active_and_single_org,
    is_effective_range_activated,
    is_organization_activated,
)
from db.clients import configuration_client
from db.model import DateRange, MemberVersioned

pytestmark = pytest.mark.asyncio


def future_date():
    return datetime.combine(date.today(), datetime.min.time()) + timedelta(days=365)


@pytest.fixture
def current_date():
    return datetime.combine(date.today(), datetime.min.time())


@pytest.fixture
def member_list():
    return [
        MemberVersioned(
            organization_id=i % 2 + 1,
            first_name=f"first_name_{i}",
            last_name=f"last_name_{i}",
            date_of_birth=date(1990, 12, 12),
            created_at=datetime.now() - timedelta(days=i),
        )
        for i in range(5)
    ]


class MockConfigurations(configuration_client.Configurations):
    def __init__(self, **kwargs):
        pass


class TestValidateion:

    historical_date = datetime.combine(date.today(), datetime.min.time()) - timedelta(
        days=365
    )
    current_date = datetime.today()
    future_date = datetime.combine(date.today(), datetime.min.time()) + timedelta(
        days=365
    )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="activated_at, effective_range,expected",
        argvalues=[
            (current_date, None, True),
            (current_date, DateRange(upper=None), True),
            (current_date, DateRange(upper=historical_date.date()), False),
            (current_date, DateRange(upper=future_date.date()), True),
        ],
        ids=[
            "effective_range_none",
            "effective_upper_none",
            "range_ends_before_activated",
            "range_ends_after_activated",
        ],
    )
    def test_is_effective_range_activated(effective_range, expected, activated_at):
        # When
        result = is_effective_range_activated(activated_at, effective_range)

        # Then
        assert result == expected

    @pytest.mark.parametrize(
        argnames="activated_at,terminated_at,expected",
        argvalues=[
            (historical_date, None, True),  # activated, not terminated
            (
                historical_date,
                current_date,
                False,
            ),  # activated, terminated
            (
                future_date,
                None,
                False,
            ),  # activated in future, not terminated
            (None, None, False),  # not activated, not terminated
            (
                historical_date,
                future_date,
                True,
            ),  # activated , terminated in future
            (
                future_date,
                future_date,
                False,
            ),  # activated in future, terminated in future
        ],
        ids=[
            "activated_true_terminated_null",
            "activated_true_terminated_true",
            "activated_future_terminated_false",
            "activated_null_terminated_null",
            "activated_true_terminated_in_future",
            "activated_future_terminated_in_future",
        ],
    )
    def test_is_organization_activated(self, activated_at, terminated_at, expected):
        # Given
        config = factory.ConfigurationFactory.create(
            activated_at=activated_at, terminated_at=terminated_at
        )

        # When
        configuration_activated = is_organization_activated(config)

        # Then
        assert configuration_activated == expected

    @staticmethod
    async def test_check_member_org_active_and_single_org_error_when_no_active_org(
        member_list,
    ):
        with patch(
            "app.utils.eligibility_validation.is_cached_organization_active",
            return_value=False,
        ):
            mock_configs = MockConfigurations()
            with pytest.raises(errors.MatchError):
                # when
                _ = await check_member_org_active_and_single_org(
                    mock_configs, member_list
                )

    @staticmethod
    async def test_check_member_org_active_and_single_org_error_when_multiple_active_org(
        member_list,
    ):
        with patch(
            "app.utils.eligibility_validation.is_cached_organization_active",
            return_value=True,
        ):
            mock_configs = MockConfigurations()
            with pytest.raises(errors.MatchMultipleError):
                # when
                _ = await check_member_org_active_and_single_org(
                    mock_configs, member_list
                )

    @staticmethod
    async def test_check_member_org_active_and_single_org_return_most_recent(
        member_list,
    ):
        async def org_1_active(*args, **kwargs):
            org_id, _ = args
            return org_id == 1

        mock_configs = MockConfigurations()

        with patch(
            "app.utils.eligibility_validation.is_cached_organization_active"
        ) as mock_active:
            mock_active.side_effect = org_1_active

            # when
            res = await check_member_org_active_and_single_org(
                mock_configs, member_list
            )
            assert res is not None
            assert res.organization_id == 1
            assert res.first_name == "first_name_0"

    @staticmethod
    async def test_check_member_org_active_and_single_org_when_org_active(member_list):
        with patch(
            "app.utils.eligibility_validation.is_cached_organization_active",
            return_value=True,
        ):
            member = member_list[0]
            mock_configs = MockConfigurations()
            # when
            result = await check_member_org_active(mock_configs, member)

            assert result is not None
            assert result == member

    @staticmethod
    async def test_check_member_org_active_and_single_org_when_org_not_active(
        member_list,
    ):
        with patch(
            "app.utils.eligibility_validation.is_cached_organization_active",
            return_value=False,
        ):
            member = member_list[0]
            mock_configs = MockConfigurations()
            # when
            with pytest.raises(errors.MatchError):
                _ = await check_member_org_active(mock_configs, member)
