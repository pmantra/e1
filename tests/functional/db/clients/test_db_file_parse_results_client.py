from __future__ import annotations

import datetime
from typing import List

import pytest
from tests.factories import data_models as factory

from db import model
from db.clients import file_client, file_parse_results_client

pytestmark = pytest.mark.asyncio


class TestFileParseResultsClient:

    # region create/update
    @staticmethod
    async def test_bulk_persist_file_parse_results_single_org(
        test_file: file_client.File,
        file_parse_results_test_client,
    ):
        # Given
        inputs: List[
            file_parse_results_client.FileParseResult
        ] = factory.FileParseResultFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )
        num_persisted = (
            await file_parse_results_test_client.bulk_persist_file_parse_results(
                results=inputs
            )
        )

        # When
        created = await file_parse_results_test_client.get_file_parse_results_for_file(
            test_file.id
        )

        assert len(created) == num_persisted
        assert set(
            (r.organization_id, r.dependent_id, r.unique_corp_id) for r in inputs
        ) == set((r.organization_id, r.dependent_id, r.unique_corp_id) for r in created)

    @staticmethod
    async def test_bulk_persist_file_parse_results_multi_org(
        file_parse_results_test_client, configuration_test_client, file_test_client
    ):
        # Given
        num_records_org_a = 10
        num_records_org_b = 20

        org_data_provider = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=True)
        )

        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=False)
        )

        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=False)
        )

        file_data_provider = await file_test_client.persist(
            model=factory.FileFactory(organization_id=org_data_provider.organization_id)
        )

        fpr_org_a = factory.FileParseResultFactory.create_batch(
            num_records_org_a,
            organization_id=org_a.organization_id,
            file_id=file_data_provider.id,
        )

        fpr_org_b = factory.FileParseResultFactory.create_batch(
            num_records_org_b,
            organization_id=org_b.organization_id,
            file_id=file_data_provider.id,
        )

        # When
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=fpr_org_a + fpr_org_b
        )

        # Then
        persisted_records = (
            await file_parse_results_test_client.get_file_parse_results_for_file(
                file_id=file_data_provider.id
            )
        )

        # Check that we have persisted the records that we think that we have persisted
        assert set(
            [
                (r.organization_id, r.unique_corp_id, r.dependent_id)
                for r in persisted_records
            ]
        ) == set(
            [
                (r.organization_id, r.unique_corp_id, r.dependent_id)
                for r in fpr_org_a + fpr_org_b
            ]
        )

    @staticmethod
    async def test_bulk_persist_file_parse_errors_single_org(
        test_config, test_file, file_parse_results_test_client
    ):
        # Given
        inputs = factory.FileParseErrorFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )

        # When
        num_created = (
            await file_parse_results_test_client.bulk_persist_file_parse_errors(
                errors=inputs
            )
        )
        output = await file_parse_results_test_client.get_all_file_parse_errors()

        # Then
        assert len(output) == num_created

    @staticmethod
    async def test_bulk_persist_file_parse_errors_multi_org(
        file_parse_results_test_client, configuration_test_client, file_test_client
    ):
        # Given
        num_errors_org_a = 10
        num_errors_org_b = 20

        org_data_provider = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=True)
        )

        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=False)
        )

        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory.create(data_provider=False)
        )

        file_data_provider = await file_test_client.persist(
            model=factory.FileFactory(organization_id=org_data_provider.organization_id)
        )

        errors_org_a = factory.FileParseErrorFactory.create_batch(
            num_errors_org_a,
            organization_id=org_a.organization_id,
            file_id=file_data_provider.id,
        )

        errors_org_b = factory.FileParseErrorFactory.create_batch(
            num_errors_org_b,
            organization_id=org_b.organization_id,
            file_id=file_data_provider.id,
        )

        # When
        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=errors_org_a + errors_org_b
        )

        # Then
        persisted_errors_org_a = (
            await file_parse_results_test_client.get_file_parse_errors_for_org(
                org_id=org_a.organization_id
            )
        )

        persisted_errors_org_b = (
            await file_parse_results_test_client.get_file_parse_errors_for_org(
                org_id=org_b.organization_id
            )
        )

        assert num_errors_org_a == len(
            persisted_errors_org_a
        ) and num_errors_org_b == len(persisted_errors_org_b)

    @staticmethod
    async def test_expire_missing_records_for_file_single_org(
        test_config,
        file_test_client,
        member_test_client,
        file_parse_results_test_client,
    ):
        # Given
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Members that will be in new file
        members_not_expired = factory.MemberFactory.create_batch(
            10,
            organization_id=test_config.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        # Members that will not be in new file
        members_expired = factory.MemberFactory.create_batch(
            8,
            organization_id=test_config.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )

        # Persist members that we will not expire
        await member_test_client.bulk_persist(models=members_not_expired)
        # Persist members that we will expire
        expected_expired = await member_test_client.bulk_persist(models=members_expired)
        # Grab ids that we expect to be expired
        expected_expired_ids = {m["id"] for m in expected_expired}

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=[
                factory.FileParseResultFactory.create(
                    file_id=new_file.id,
                    organization_id=to_expire.organization_id,
                    unique_corp_id=to_expire.unique_corp_id,
                    dependent_id=to_expire.dependent_id,
                )
                for to_expire in members_not_expired
            ]
        )

        # When we expire_missing_records_for_files
        await file_parse_results_test_client.expire_missing_records_for_file(
            file_id=new_file.id, organization_id=new_file.organization_id
        )

        # Then
        expired_ids = {
            member.id
            for member in await member_test_client.all()
            if member.effective_range.upper is not None
        }

        assert expected_expired_ids == expired_ids

    @staticmethod
    async def test_expire_missing_records_for_file_multi_org(
        file_test_client,
        member_test_client,
        file_parse_results_test_client,
        configuration_test_client,
    ):
        # Given
        org_data_provider = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=True)
        )
        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_external_id_a = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_a.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        org_external_id_b = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_b.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        await configuration_test_client.bulk_add_external_id(
            [org_external_id_a, org_external_id_b]
        )
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id
            )
        )
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id
            )
        )
        # Members that will be in new file
        members_valid_org_a = factory.MemberFactory.create_batch(
            10,
            organization_id=org_a.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        members_valid_org_b = factory.MemberFactory.create_batch(
            10,
            organization_id=org_b.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        # Members that will not be in new file
        members_expired_org_a = factory.MemberFactory.create_batch(
            8,
            organization_id=org_a.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        members_expired_org_b = factory.MemberFactory.create_batch(
            8,
            organization_id=org_b.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )

        # Persist members that we will not expire
        await member_test_client.bulk_persist(
            models=members_valid_org_a + members_valid_org_b
        )
        # Persist members that we will expire
        expected_expired = await member_test_client.bulk_persist(
            models=members_expired_org_a + members_expired_org_b
        )
        # Grab ids that we expect to be expired
        expected_expired_ids = {m["id"] for m in expected_expired}

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=[
                factory.FileParseResultFactory.create(
                    file_id=new_file.id,
                    organization_id=to_expire.organization_id,
                    unique_corp_id=to_expire.unique_corp_id,
                    dependent_id=to_expire.dependent_id,
                )
                for to_expire in (members_valid_org_a + members_valid_org_b)
            ]
        )

        # When we expire_missing_records_for_files
        await file_parse_results_test_client.expire_missing_records_for_file(
            file_id=new_file.id, organization_id=new_file.organization_id
        )

        # Then
        expired_ids = {
            member.id
            for member in await member_test_client.all()
            if member.effective_range.upper is not None
        }

        assert expected_expired_ids == expired_ids

    @staticmethod
    async def test_expire_missing_records_for_file_single_org_versioned(
        test_config,
        file_test_client,
        member_versioned_test_client,
        file_parse_results_test_client,
    ):
        # Given
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Members that will be in new file
        members_valid = factory.MemberVersionedFactory.create_batch(
            10,
            organization_id=test_config.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2001-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        # Members that will be in old file
        members_expired = factory.MemberVersionedFactory.create_batch(
            8,
            organization_id=test_config.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )

        # Persist members that we will not expire
        expected_valid = await member_versioned_test_client.bulk_persist(
            models=members_valid
        )
        # Persist members that we will expire
        expected_expired = await member_versioned_test_client.bulk_persist(
            models=members_expired
        )
        # Grab ids that we expect to be expired
        expected_expired_ids = {m.id for m in expected_expired}
        expected_valid_ids = {m.id for m in expected_valid}

        # When we expire_missing_records_for_files
        expired = await file_parse_results_test_client.expire_missing_records_for_file_versioned(
            file_id=new_file.id, organization_id=new_file.organization_id
        )

        # Then
        all_members = await member_versioned_test_client.all()
        expired_ids = {
            member.id
            for member in all_members
            if member.effective_range.upper is not None
        }
        valid_ids = {
            member.id for member in all_members if member.effective_range.upper is None
        }

        assert len(expected_expired_ids) == expired
        assert expected_expired_ids == expired_ids and expected_valid_ids == valid_ids

    @staticmethod
    async def test_expire_missing_records_for_file_single_org_versioned_expire_previous_3_files(
        test_config,
        file_test_client,
        member_versioned_test_client,
        file_parse_results_test_client,
    ):
        """Make sure we only try to expire the previous 3 file's records and not everything."""
        # Given
        await file_test_client.bulk_persist(
            models=factory.FileFactory.create_batch(
                size=5, organization_id=test_config.organization_id
            )
        )

        files = await file_test_client.all()
        files = sorted(files, key=lambda x: x.id)

        members = []

        for file in files:
            members = members + factory.MemberVersionedFactory.create_batch(
                10,
                organization_id=test_config.organization_id,
                file_id=file.id,
                effective_range=factory.DateRangeFactory(
                    lower=datetime.datetime.fromisoformat("2001-01-01 00:00:00.000"),
                    upper=None,
                ),
            )

        members = await member_versioned_test_client.bulk_persist(models=members)

        # Grab ids that we expect to be expired
        expected_expired_ids = {
            m.id for m in members if m.file_id in {f.id for f in files[1:4]}
        }

        # When we expire_missing_records_for_files
        expired = await file_parse_results_test_client.expire_missing_records_for_file_versioned(
            file_id=files[-1].id, organization_id=files[-1].organization_id
        )

        # Then
        all_members = await member_versioned_test_client.all()
        expired_ids = {
            member.id
            for member in all_members
            if member.effective_range.upper is not None
        }

        assert len(expected_expired_ids) == expired
        assert expected_expired_ids == expired_ids

    @staticmethod
    async def test_expire_missing_records_for_file_multi_org_versioned(
        file_test_client,
        member_versioned_test_client,
        file_parse_results_test_client,
        configuration_test_client,
    ):
        # Given
        org_data_provider = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=True)
        )
        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_external_id_a = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_a.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        org_external_id_b = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_b.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        await configuration_test_client.bulk_add_external_id(
            [org_external_id_a, org_external_id_b]
        )
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id
            )
        )
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id
            )
        )
        # Members that will be in new file
        members_valid_org_a = factory.MemberVersionedFactory.create_batch(
            10,
            organization_id=org_a.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        members_valid_org_b = factory.MemberVersionedFactory.create_batch(
            10,
            organization_id=org_b.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        # Members that will not be in new file
        members_expired_org_a = factory.MemberVersionedFactory.create_batch(
            8,
            organization_id=org_a.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        members_expired_org_b = factory.MemberVersionedFactory.create_batch(
            8,
            organization_id=org_b.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )

        # Persist members that we will not expire
        expected_valid = await member_versioned_test_client.bulk_persist(
            models=members_valid_org_a + members_valid_org_b
        )
        # Persist members that we will expire
        expected_expired = await member_versioned_test_client.bulk_persist(
            models=members_expired_org_a + members_expired_org_b
        )
        # Grab ids that we expect to be expired
        expected_expired_ids = {m.id for m in expected_expired}
        expected_valid_ids = {m.id for m in expected_valid}

        # When we expire_missing_records_for_files
        expired = await file_parse_results_test_client.expire_missing_records_for_file_versioned(
            file_id=new_file.id, organization_id=new_file.organization_id
        )

        # Then
        all_members = await member_versioned_test_client.all()
        expired_ids = {
            member.id
            for member in all_members
            if member.effective_range.upper is not None
        }
        valid_ids = {
            member.id for member in all_members if member.effective_range.upper is None
        }

        assert len(expected_expired_ids) == expired
        assert expected_expired_ids == expired_ids and expected_valid_ids == valid_ids

    @staticmethod
    async def test_expire_missing_records_clears_hash_version_and_value(
        test_config,
        file_test_client,
        member_versioned_test_client,
        file_parse_results_test_client,
    ):
        # Given
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        # Members that will be in new file
        members_valid = factory.MemberVersionedFactoryWithHash.create_batch(
            10,
            organization_id=test_config.organization_id,
            file_id=new_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2001-01-01 00:00:00.000"),
                upper=None,
            ),
        )
        # Members that will be in old file
        members_expired = factory.MemberVersionedFactoryWithHash.create_batch(
            8,
            organization_id=test_config.organization_id,
            file_id=old_file.id,
            effective_range=factory.DateRangeFactory(
                lower=datetime.datetime.fromisoformat("2000-01-01 00:00:00.000"),
                upper=None,
            ),
        )

        # Persist members that we will not expire
        await member_versioned_test_client.bulk_persist(models=members_valid)
        # Persist members that we will expire
        await member_versioned_test_client.bulk_persist(models=members_expired)

        # When we expire_missing_records_for_files
        await file_parse_results_test_client.expire_missing_records_for_file_versioned(
            file_id=new_file.id, organization_id=new_file.organization_id
        )

        # Then
        old_members = await member_versioned_test_client.get_for_file(
            file_id=old_file.id
        )
        new_members = await member_versioned_test_client.get_for_file(
            file_id=new_file.id
        )

        assert {(m.hash_value, m.hash_version) for m in old_members} == {
            (None, None)
        } and (None, None) not in {(m.hash_value, m.hash_version) for m in new_members}

    # region fetch
    @staticmethod
    async def test_get_all_file_parse_results(
        test_config, file_test_client, file_parse_results_test_client
    ):
        # Given a bunch of records we want to persist
        file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )

        records = factory.FileParseResultFactory.create_batch(
            100, organization_id=test_config.organization_id, file_id=file.id
        )

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        # When we fetch all of the file_parse_records
        fetched_records = (
            await file_parse_results_test_client.get_all_file_parse_results()
        )

        # Then what we fetched should be all of what we persisted
        assert len(records) == len(fetched_records)

    @staticmethod
    async def test_get_all_file_parse_errors(
        test_config, file_test_client, file_parse_results_test_client
    ):
        # Given a bunch of records we want to persist
        file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )

        errors = factory.FileParseErrorFactory.create_batch(
            100, organization_id=test_config.organization_id, file_id=file.id
        )

        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=errors
        )

        # When we fetch all of the file_parse_errors
        fetched_records = (
            await file_parse_results_test_client.get_all_file_parse_errors()
        )

        # Then what we fetched should be all of what we persisted
        assert len(errors) == len(fetched_records)

    @staticmethod
    async def test_get_file_parse_results_for_file(
        test_config,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        test_file_1 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_1_inputs = factory.FileParseResultFactory.create_batch(
            5, organization_id=test_config.organization_id, file_id=test_file_1.id
        )

        test_file_2 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2_inputs = factory.FileParseResultFactory.create_batch(
            10, organization_id=test_config.organization_id, file_id=test_file_2.id
        )

        # Save our records
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=file_1_inputs + file_2_inputs
        )

        # When
        file_1_outputs = (
            await file_parse_results_test_client.get_file_parse_results_for_file(
                test_file_1.id
            )
        )
        file_2_outputs = (
            await file_parse_results_test_client.get_file_parse_results_for_file(
                test_file_2.id
            )
        )

        # Then
        assert len(file_1_inputs) == len(file_1_outputs)
        assert len(file_2_inputs) == len(file_2_outputs)

    @staticmethod
    async def test_get_file_parse_errors_for_file(
        test_config, test_file, file_parse_results_test_client
    ):
        # Given
        inputs = factory.FileParseErrorFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )

        # When
        num_created = (
            await file_parse_results_test_client.bulk_persist_file_parse_errors(
                errors=inputs
            )
        )
        output = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=test_file.id
        )

        # Then
        assert len(inputs) == len(output) == num_created

    @staticmethod
    async def test_get_file_parse_results_for_org(
        test_config,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        test_file_1 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_1_inputs = factory.FileParseResultFactory.create_batch(
            5, organization_id=test_config.organization_id, file_id=test_file_1.id
        )

        test_file_2 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2_inputs = factory.FileParseResultFactory.create_batch(
            10, organization_id=test_config.organization_id, file_id=test_file_2.id
        )

        # Save our records
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=file_1_inputs + file_2_inputs
        )

        # When
        results = await file_parse_results_test_client.get_file_parse_results_for_org(
            test_config.organization_id
        )

        # Then
        assert len(results) == len(file_1_inputs) + len(file_2_inputs)

    @staticmethod
    async def test_get_file_parse_errors_for_org(
        test_config, test_file, file_parse_results_test_client
    ):
        # Given
        inputs = factory.FileParseErrorFactory.create_batch(
            10, organization_id=test_file.organization_id, file_id=test_file.id
        )

        # When
        num_created = (
            await file_parse_results_test_client.bulk_persist_file_parse_errors(
                errors=inputs
            )
        )
        output = await file_parse_results_test_client.get_file_parse_errors_for_org(
            org_id=test_config.organization_id
        )

        # Then
        assert len(inputs) == len(output) == num_created

    @staticmethod
    async def test_get_incomplete_files_by_org_single_org(
        test_config,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        old_success_count = 38
        old_failure_count = 0
        new_success_count = 30
        new_failure_count = 8

        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id,
                success_count=old_success_count,
                failure_count=old_failure_count,
                raw_count=(old_success_count + old_failure_count),
            )
        )
        await file_test_client.set_completed_at(id=old_file.id)

        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id,
                success_count=new_success_count,
                failure_count=new_failure_count,
                raw_count=(new_success_count + new_failure_count),
            )
        )

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                new_success_count,
                organization_id=test_config.organization_id,
                file_id=new_file.id,
            )
        )

        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=factory.FileParseErrorFactory.create_batch(
                new_failure_count,
                organization_id=test_config.organization_id,
                file_id=new_file.id,
            )
        )

        # When
        incomplete_files = (
            await file_parse_results_test_client.get_incomplete_files_by_org()
        )
        files_by_org: model.IncompleteFilesByOrg = incomplete_files[0]
        incomplete = files_by_org.incomplete[0]
        # Then
        assert files_by_org.total_members == old_success_count
        assert incomplete["total_missing"] == (old_success_count - new_success_count)
        assert incomplete["total_parsed"] == new_success_count
        assert incomplete["total_errors"] == new_failure_count

    @staticmethod
    async def test_get_incomplete_files_by_org_multiple_orgs(
        configuration_test_client,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        old_success_count_a = 38
        old_failure_count_a = 0
        old_success_count_b = 38
        old_failure_count_b = 0
        new_success_count_a = 30
        new_failure_count_a = 8
        new_success_count_b = 30
        new_failure_count_b = 8

        # Given
        org_data_provider = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=True)
        )
        org_a = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_b = await configuration_test_client.persist(
            model=factory.ConfigurationFactory(data_provider=False)
        )
        org_external_id_a = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_a.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        org_external_id_b = factory.MavenOrgExternalIDFactory.create(
            organization_id=org_b.organization_id,
            data_provider_organization_id=org_data_provider.organization_id,
        )
        await configuration_test_client.bulk_add_external_id(
            [org_external_id_a, org_external_id_b]
        )
        # Create an old file
        old_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id,
                success_count=(old_success_count_a + old_success_count_b),
                failure_count=(old_failure_count_a + old_failure_count_b),
            )
        )
        await file_test_client.set_completed_at(old_file.id)
        # Create a new file
        new_file = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=org_data_provider.organization_id,
                success_count=(new_success_count_a + new_success_count_b),
                failure_count=(new_failure_count_a + new_failure_count_b),
            )
        )

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                new_success_count_a,
                organization_id=org_a.organization_id,
                file_id=new_file.id,
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                new_success_count_b,
                organization_id=org_b.organization_id,
                file_id=new_file.id,
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=factory.FileParseErrorFactory.create_batch(
                new_failure_count_a,
                organization_id=org_a.organization_id,
                file_id=new_file.id,
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=factory.FileParseErrorFactory.create_batch(
                new_failure_count_b,
                organization_id=org_b.organization_id,
                file_id=new_file.id,
            )
        )

        # When
        incomplete_files = (
            await file_parse_results_test_client.get_incomplete_files_by_org()
        )
        files_by_org: model.IncompleteFilesByOrg = incomplete_files[0]
        incomplete = files_by_org.incomplete[0]
        # Then
        assert files_by_org.total_members == (old_success_count_a + old_success_count_b)
        assert incomplete["total_missing"] == (
            old_success_count_a + old_success_count_b
        ) - (new_success_count_a + new_success_count_a)
        assert incomplete["total_parsed"] == new_success_count_a + new_success_count_a
        assert incomplete["total_errors"] == new_failure_count_a + new_failure_count_b

    # endregion

    # region delete
    @staticmethod
    async def test_delete_file_parse_results_for_files(
        test_config, file_test_client, file_parse_results_test_client
    ):
        # Given
        file_1 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_3 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_1_records = factory.FileParseResultFactory.create_batch(
            74, organization_id=test_config.organization_id, file_id=file_1.id
        )
        file_2_records = factory.FileParseResultFactory.create_batch(
            42, organization_id=test_config.organization_id, file_id=file_2.id
        )
        file_3_records = factory.FileParseResultFactory.create_batch(
            18, organization_id=test_config.organization_id, file_id=file_3.id
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=file_1_records + file_2_records + file_3_records
        )

        # When
        files_to_delete = [file_1.id, file_2.id]

        await file_parse_results_test_client.delete_file_parse_results_for_files(
            *files_to_delete
        )

        # Then
        remaining = await file_parse_results_test_client.get_all_file_parse_results()

        assert len(remaining) == len(file_3_records)

    # region delete
    @staticmethod
    async def test_delete_file_parse_errors_for_files(
        test_config, file_test_client, file_parse_results_test_client
    ):
        # Given
        file_1 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_3 = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_1_errors = factory.FileParseErrorFactory.create_batch(
            74, organization_id=test_config.organization_id, file_id=file_1.id
        )
        file_2_errors = factory.FileParseErrorFactory.create_batch(
            42, organization_id=test_config.organization_id, file_id=file_2.id
        )
        file_3_errors = factory.FileParseErrorFactory.create_batch(
            18, organization_id=test_config.organization_id, file_id=file_3.id
        )
        await file_parse_results_test_client.bulk_persist_file_parse_errors(
            errors=file_1_errors + file_2_errors + file_3_errors
        )

        # When
        files_to_delete = [file_1.id, file_2.id]

        await file_parse_results_test_client.delete_file_parse_errors_for_files(
            *files_to_delete
        )

        # Then
        remaining = await file_parse_results_test_client.get_all_file_parse_errors()

        assert len(remaining) == len(file_3_errors)

    # endregion


class TestBulkPersistParsedRecords:

    # region bulk_persist_parsed_records_for_files
    @staticmethod
    async def test_bulk_persist_parsed_records_for_files(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files(
            file.id
        )

        # Then
        members = await member_test_client.get_for_file(file_id=file.id)

        assert len(members) == expected_num_of_members

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_moves_correct_records(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
    ):
        # Given
        expected_num_of_members_file_1 = 120
        expected_num_of_members_file_2 = 420
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                expected_num_of_members_file_1,
                organization_id=test_config.organization_id,
                file_id=file_1.id,
            )
            + factory.FileParseResultFactory.create_batch(
                expected_num_of_members_file_2,
                organization_id=test_config.organization_id,
                file_id=file_2.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files(
            file_1.id
        )

        # Then
        members_file_1 = await member_test_client.get_for_file(file_id=file_1.id)
        members_file_2 = await member_test_client.get_for_file(file_id=file_2.id)

        assert (
            len(members_file_1) == expected_num_of_members_file_1 and not members_file_2
        )

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_deletes_from_file_parse_results(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files(
            file.id
        )

        # Then
        file_parse_results: List[
            model.FileParseResult
        ] = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=file.id
        )
        assert not file_parse_results

    # endregion bulk_persist_parsed_records_for_files

    # region bulk_persist_parsed_records_for_files_dual_write
    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write(
            file.id
        )

        # Then
        members = await member_test_client.get_for_file(file_id=file.id)
        members_versioned = await member_versioned_test_client.get_for_file(
            file_id=file.id
        )

        assert len(members) == expected_num_of_members
        assert len(members_versioned) == expected_num_of_members

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_deletes_from_file_parse_results(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactory.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write(
            file.id
        )

        # Then
        file_parse_results: List[
            model.FileParseResult
        ] = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=file.id
        )

        assert not file_parse_results

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_inserts(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        records = factory.FileParseResultFactory.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )
        # Same records, just new file
        for record in records:
            record.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write(
            file_1.id
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write(
            file_2.id
        )

        # Then
        members = await member_test_client.get_for_org(
            organization_id=test_config.organization_id
        )
        members_versioned = await member_versioned_test_client.get_for_org(
            organization_id=test_config.organization_id
        )

        assert (
            len(members) == expected_num_of_members
            and len(members_versioned) == expected_num_of_members * 2
        )

    # endregion bulk_persist_parsed_records_for_files_dual_write

    # region bulk_persist_parsed_records_for_files_dual_write_hash
    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_hash(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactoryWithHash.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file.id
        )

        # Then
        members = await member_test_client.get_for_file(file_id=file.id)
        members_versioned = await member_versioned_test_client.get_for_file(
            file_id=file.id
        )

        assert len(members) == expected_num_of_members
        assert len(members_versioned) == expected_num_of_members
        assert all(m.employer_assigned_id is not None for m in members_versioned)

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_hash_deletes_from_file_parse_results(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactoryWithHash.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file.id
        )

        # Then
        file_parse_results: List[
            model.FileParseResult
        ] = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=file.id
        )

        assert not file_parse_results

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_hash_inserts(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        records = factory.FileParseResultFactoryWithHash.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )
        # Same records, just new file
        for record in records:
            record.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_1.id
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_2.id
        )

        # Then
        members = await member_test_client.get_for_org(
            organization_id=test_config.organization_id
        )
        members_versioned = await member_versioned_test_client.get_for_org(
            organization_id=test_config.organization_id
        )

        assert len(members) == len(members_versioned) == expected_num_of_members

        # Ensure we updated the file ID to point to the new file
        for m in members_versioned:
            assert m.file_id == file_2.id

    @staticmethod
    async def test_bulk_persist_parsed_records_for_files_dual_write_hash_duplicate_inserts_some_valid(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        """Insert records to member-versioned- some are duplicates and should not be re-inserted"""
        # Given
        expected_num_of_members = 100
        expected_second_num_members = 50

        # Write records to file parse results table
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )

        hashed_data = factory.FileParseResultFactoryWithHash.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data
        )

        # Perform our first insert
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_1.id
        )

        # When
        # Perform a second insert of the same data with some new data added
        hashed_data_2 = factory.FileParseResultFactoryWithHash.create_batch(
            expected_second_num_members,
            organization_id=test_config.organization_id,
            file_id=file_2.id,
        )
        for h in hashed_data:
            h.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data + hashed_data_2
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_2.id
        )

        # Then
        # Confirm we did not insert any previously existing data to the table
        members = await member_test_client.get_for_org(
            organization_id=test_config.organization_id
        )
        member_versioned = await member_versioned_test_client.get_for_org(
            organization_id=test_config.organization_id
        )

        assert (
            len(member_versioned)
            == expected_num_of_members + expected_second_num_members
        )
        assert len(members) == expected_num_of_members + expected_second_num_members

        # Ensure we've updated our file id to point to the new records
        for m in member_versioned:
            assert m.file_id == file_2.id

    # endregion bulk_persist_parsed_records_for_files_dual_write_hash

    # region bulk_persist_parsed_records_for_file_and_org_dual_write_hash
    @staticmethod
    async def test_bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactoryWithHash.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file.id, test_config.organization_id
        )

        # Then
        members = await member_test_client.get_for_file(file_id=file.id)
        members_versioned = await member_versioned_test_client.get_for_file(
            file_id=file.id
        )

        assert len(members) == expected_num_of_members
        assert len(members_versioned) == expected_num_of_members
        assert all(m.employer_assigned_id is not None for m in members_versioned)

    @staticmethod
    async def test_bulk_persist_parsed_records_for_file_and_org_dual_write_hash_deletes_from_file_parse_results(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=factory.FileParseResultFactoryWithHash.create_batch(
                expected_num_of_members,
                organization_id=test_config.organization_id,
                file_id=file.id,
            )
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file.id, test_config.organization_id
        )

        # Then
        file_parse_results: List[
            model.FileParseResult
        ] = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=file.id
        )

        assert not file_parse_results

    @staticmethod
    async def test_bulk_persist_parsed_records_for_file_and_org_dual_write_hash_inserts(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        # Given
        expected_num_of_members = 100
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        records = factory.FileParseResultFactoryWithHash.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )
        # Same records, just new file
        for record in records:
            record.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        # When
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file_1.id, test_config.organization_id
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file_2.id, test_config.organization_id
        )

        # Then
        members = await member_test_client.get_for_org(
            organization_id=test_config.organization_id
        )
        members_versioned = await member_versioned_test_client.get_for_org(
            organization_id=test_config.organization_id
        )

        assert len(members) == len(members_versioned) == expected_num_of_members

        # Ensure we updated the file ID to point to the new file
        for m in members_versioned:
            assert m.file_id == file_2.id

    @staticmethod
    async def test_bulk_persist_parsed_records_for_file_and_org_dual_write_hash_duplicate_inserts_some_valid(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        """Insert records to member-versioned- some are duplicates and should not be re-inserted"""
        # Given
        expected_num_of_members = 100
        expected_second_num_members = 50

        # Write records to file parse results table
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )

        hashed_data = factory.FileParseResultFactoryWithHash.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data
        )

        # Perform our first insert
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file_1.id, test_config.organization_id
        )

        # When
        # Perform a second insert of the same data with some new data added
        hashed_data_2 = factory.FileParseResultFactoryWithHash.create_batch(
            expected_second_num_members,
            organization_id=test_config.organization_id,
            file_id=file_2.id,
        )
        for h in hashed_data:
            h.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data + hashed_data_2
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_file_and_org_dual_write_hash(
            file_2.id, test_config.organization_id
        )

        # Then
        # Confirm we did not insert any previously existing data to the table
        members = await member_test_client.get_for_org(
            organization_id=test_config.organization_id
        )
        member_versioned = await member_versioned_test_client.get_for_org(
            organization_id=test_config.organization_id
        )

        assert (
            len(member_versioned)
            == expected_num_of_members + expected_second_num_members
        )
        assert len(members) == expected_num_of_members + expected_second_num_members

        # Ensure we've updated our file id to point to the new records
        for m in member_versioned:
            assert m.file_id == file_2.id

    # endregion bulk_persist_parsed_records_for_file_and_org_dual_write_hash

    @staticmethod
    async def test_get_count_hashed_inserted_for_file(
        test_config: model.Configuration,
        file_test_client,
        file_parse_results_test_client,
        member_test_client,
        member_versioned_test_client,
    ):
        """Insert records to member-versioned- some are duplicates and should not be re-inserted"""
        # Given
        expected_num_of_members = 5
        expected_second_num_members = 10

        # Write records to file parse results table
        file_1: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )
        file_2: model.File = await file_test_client.persist(
            model=factory.FileFactory.create(
                organization_id=test_config.organization_id
            )
        )

        hashed_data = factory.FileParseResultFactoryWithHash.create_batch(
            expected_num_of_members,
            organization_id=test_config.organization_id,
            file_id=file_1.id,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data
        )

        # Perform our first insert
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_1.id
        )
        # Manually set the timestamp of when we expected the previous data to be created- we want to mock new data being inserted at a later time
        members = await member_versioned_test_client.all()
        for m in members:
            await member_versioned_test_client.set_created_at(
                id=m.id,
                created_at=m.created_at - datetime.timedelta(days=10),
            )

        # When
        # Perform a second insert of the same data with some new data added
        hashed_data_2 = factory.FileParseResultFactoryWithHash.create_batch(
            expected_second_num_members,
            organization_id=test_config.organization_id,
            file_id=file_2.id,
        )
        for h in hashed_data:
            h.file_id = file_2.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=hashed_data + hashed_data_2
        )
        await file_parse_results_test_client.bulk_persist_parsed_records_for_files_dual_write_hash(
            file_2.id
        )

        # Then
        result = (
            await file_parse_results_test_client.get_count_hashed_inserted_for_file(
                file_2.id, file_2.created_at
            )
        )
        assert result["hashed_count"] == expected_num_of_members
        assert result["new_count"] == expected_second_num_members
