from datetime import date, datetime, timedelta

import pytest

from app.eligibility.errors import MatchError
from app.utils.eligibility_validation import check_member_org_active_and_overeligibility

pytestmark = pytest.mark.asyncio
from tests.factories import data_models


class TestCheckMemberOrgActiveandOvereligiblity:
    async def test_single_org_active(self, test_config, configuration_test_client):
        # Given
        member_1 = data_models.MemberVersionedFactory.create(
            organization_id=test_config.organization_id
        )

        # When
        result = await check_member_org_active_and_overeligibility(
            configuration_test_client, [member_1]
        )

        # Then
        assert result == [member_1]

    async def test_multiple_orgs_active(self, configuration_test_client):
        # Given
        org_1 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        member_1 = data_models.MemberVersionedFactory.create(
            organization_id=org_1.organization_id
        )
        org_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        member_2 = data_models.MemberVersionedFactory.create(
            organization_id=org_2.organization_id
        )

        # When
        result = await check_member_org_active_and_overeligibility(
            configuration_test_client, [member_1, member_2]
        )

        # Then
        assert len(result) == 2
        assert member_1 in result
        assert member_2 in result

    async def test_inactive_org_only(self, configuration_test_client):
        # Given
        org_1 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(activated_at=None)
        )
        member_1 = data_models.MemberVersionedFactory.create(
            organization_id=org_1.organization_id
        )

        # When
        with pytest.raises(MatchError):
            await check_member_org_active_and_overeligibility(
                configuration_test_client, [member_1]
            )

    async def test_multiple_orgs_various_activeness(self, configuration_test_client):
        # Given
        org_1 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create(activated_at=None)
        )
        member_1 = data_models.MemberVersionedFactory.create(
            organization_id=org_1.organization_id
        )
        org_2 = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        member_2 = data_models.MemberVersionedFactory.create(
            organization_id=org_2.organization_id
        )

        # When
        result = await check_member_org_active_and_overeligibility(
            configuration_test_client, [member_1, member_2]
        )

        # Then
        assert result == [member_2]

    async def test_multiple_result_per_org(
        self, test_config, configuration_test_client
    ):
        # Given
        historical_date = datetime.combine(
            date.today(), datetime.min.time()
        ) - timedelta(days=365)
        current_date = datetime.today()

        member_1 = data_models.MemberVersionedFactory.create(
            organization_id=test_config.organization_id, updated_at=historical_date
        )
        member_2 = data_models.MemberVersionedFactory.create(
            organization_id=test_config.organization_id, updated_at=current_date
        )

        # When
        result = await check_member_org_active_and_overeligibility(
            configuration_test_client, [member_1, member_2]
        )

        # Then
        assert result == [member_2]
