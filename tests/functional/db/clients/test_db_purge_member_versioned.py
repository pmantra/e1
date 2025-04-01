from __future__ import annotations

import pytest
from tests.factories import data_models
from tests.factories.data_models import ExpiredDateRangeFactory

pytestmark = pytest.mark.asyncio


class TestMemberVersionedPurgeClient:
    @pytest.fixture
    def test_unique_corp_id(self):
        return "1243abcd"

    @pytest.fixture
    async def original_wallet(
        self, member_versioned_test_client, test_file, test_unique_corp_id
    ):
        original_wallet = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
            )
        )
        return original_wallet

    @pytest.fixture
    async def expired_wallet(
        self, member_versioned_test_client, test_file, test_unique_corp_id
    ):
        wallet = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
                effective_range=ExpiredDateRangeFactory.create(),
            )
        )
        return wallet

    @pytest.fixture()
    async def optum_record(
        self, member_versioned_test_client, test_file, test_unique_corp_id
    ):
        return await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=None,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
            )
        )

    @pytest.fixture()
    async def expired_optum_record(
        self, member_versioned_test_client, test_file, test_unique_corp_id
    ):
        return await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=None,
                effective_range=ExpiredDateRangeFactory.create(),
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
            )
        )

    @pytest.fixture()
    async def active_verification(
        self,
        member_versioned_test_client,
        member_verification_test_client,
        verification_test_client,
        test_file,
        test_unique_corp_id,
    ):

        # Create an active record used in a verification
        member_versioned_valid = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
            )
        )
        verification_record_valid = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_file.organization_id
            )
        )

        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=member_versioned_valid.id,
                verification_id=verification_record_valid.id,
            )
        )

        return member_versioned_valid

    @pytest.fixture()
    async def expired_verification(
        self,
        member_versioned_test_client,
        member_verification_test_client,
        verification_test_client,
        test_file,
        test_unique_corp_id,
    ):
        member_versioned_invalid = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
                effective_range=ExpiredDateRangeFactory.create(),
            )
        )
        verification_record_invalid = await verification_test_client.persist(
            model=data_models.VerificationFactory.create(
                organization_id=test_file.organization_id
            )
        )
        await member_verification_test_client.persist(
            model=data_models.MemberVerificationFactory.create(
                member_id=member_versioned_invalid.id,
                verification_id=verification_record_invalid.id,
            )
        )

        return member_versioned_invalid

    @staticmethod
    async def test_purge_no_records_removed(
        test_file,
        member_versioned_test_client,
        verification_test_client,
        optum_record,
        original_wallet,
    ):
        """Test case where no records match search conditions"""

        # Given
        # When
        len_purged = await member_versioned_test_client.purge_expired_records(
            organization_id=original_wallet.organization_id
        )

        # Then
        # Ensure only our original wallet record and the optum record remain
        remaining_records = await member_versioned_test_client.all()
        assert original_wallet in remaining_records
        assert optum_record in remaining_records

        purged_records = await member_versioned_test_client.get_all_historical()
        assert purged_records == []
        assert len_purged == 0

    @staticmethod
    async def test_purge_remove_expired_new_wallet(
        member_versioned_test_client, original_wallet, expired_wallet
    ):
        """Test case where we only remove a single wallet/file-based record - the expired new record"""

        # Given
        # When
        len_purged = await member_versioned_test_client.purge_expired_records(
            organization_id=original_wallet.organization_id
        )

        # Then
        remaining_records = await member_versioned_test_client.all()
        assert remaining_records == [original_wallet]

        purged_records = await member_versioned_test_client.get_all_historical()
        assert purged_records == [expired_wallet]
        assert len_purged == 1

    @staticmethod
    async def test_purge_retain_verification_records(
        member_versioned_test_client,
        original_wallet,
        active_verification,
        expired_verification,
    ):
        """Test case where we exclude records which have been used in verifications"""

        # Given
        # When
        len_purged = await member_versioned_test_client.purge_expired_records(
            organization_id=original_wallet.organization_id
        )

        # Then
        # Ensure only our original wallet record and the optum record remain
        remaining_records = await member_versioned_test_client.all()
        assert len(remaining_records) == 3
        assert original_wallet in remaining_records
        assert active_verification in remaining_records
        assert expired_verification in remaining_records

        purged_records = await member_versioned_test_client.get_all_historical()
        assert purged_records == []
        assert len_purged == 0

    @staticmethod
    async def test_purge_exhaustive(
        member_versioned_test_client,
        configuration_test_client,
        test_file,
        original_wallet,
        active_verification,
        expired_verification,
        optum_record,
        expired_optum_record,
        test_unique_corp_id,
    ):
        """Test case- remove only records which:
        - are NOT the original wallet record (excludes 'original wallet')
        - are expired
        - are tied to a file_id (excludes optum records)
        - are NOT tied to a verification (excludes verification records)

        """
        # Given
        valid_file_record = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
            )
        )

        expired_file_record = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=test_file.organization_id,
                file_id=test_file.id,
                unique_corp_id=test_unique_corp_id,
                dependent_id=test_unique_corp_id,
                effective_range=ExpiredDateRangeFactory.create(),
            )
        )

        # Let's make sure we don't touch records from other organizations
        secondary_org = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        different_org_record = await member_versioned_test_client.persist(
            model=data_models.MemberVersionedFactory.create(
                organization_id=secondary_org.organization_id
            )
        )

        # When
        len_purged = await member_versioned_test_client.purge_expired_records(
            organization_id=original_wallet.organization_id
        )

        # Then
        # Ensure we didn't remove any records we should have kept
        remaining_records = await member_versioned_test_client.all()

        assert original_wallet in remaining_records
        assert active_verification in remaining_records
        assert expired_verification in remaining_records
        assert optum_record in remaining_records
        assert expired_optum_record in remaining_records
        assert valid_file_record in remaining_records
        assert different_org_record in remaining_records

        # Ensure we remove the record we expect to be purged and copied it over to the historical table
        assert expired_file_record not in remaining_records
        purged_records = await member_versioned_test_client.get_all_historical()
        assert purged_records == [expired_file_record]
        assert len_purged == 1
