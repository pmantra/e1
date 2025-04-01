import copy
from typing import List

import pytest
from tests.factories.data_models import (
    ConfigurationFactory,
    DateRangeFactory,
    ExternalIDFactory,
    FileFactory,
    FileParseErrorFactory,
    FileParseResultFactory,
    FileParseResultFactoryWithHash,
    MemberFactory,
    MemberVerificationFactory,
    MemberVersionedFactory,
    VerificationFactory,
)

from app.eligibility.domain.model.parsed_records import (
    ParsedFileRecords,
    ProcessedRecords,
)
from app.eligibility.domain.repository import ParsedRecordsDatabaseRepository
from db import model as db_model
from db.clients.configuration_client import Configurations
from db.clients.file_client import Files
from db.clients.file_parse_results_client import FileParseResults
from db.clients.member_client import Members
from db.clients.member_verification_client import MemberVerifications
from db.clients.member_versioned_client import MembersVersioned
from db.clients.verification_client import Verifications

pytestmark = pytest.mark.asyncio


class TestMember:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="n_valid, n_errors",
        argvalues=[
            (20, 2),
            (2, 20),
            (0, 22),
            (22, 0),
            (11, 11),
        ],
    )
    async def test_persist(
        file_test_client: Files,
        test_config: db_model.Configuration,
        member_test_client: Members,
        file_parse_results_test_client: FileParseResults,
        n_valid,
        n_errors,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_client=member_test_client,
            file_client=file_test_client,
        )

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(id=2, organization_id=test_config.organization_id)
        )

        # Create some valid rows and some error rows
        errors = FileParseErrorFactory.create_batch(
            n_errors,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
        )
        valid = FileParseResultFactory.create_batch(
            n_valid,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        parsed = ParsedFileRecords(errors=errors, valid=valid)

        # When
        processed: ProcessedRecords = await repo.persist(
            parsed_records=parsed, file=new_file
        )

        # Then
        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        n_total = n_valid + n_errors
        assert n_valid == processed.valid == len(results)
        assert n_errors == processed.errors == len(errors)
        assert n_total == processed.valid + processed.errors + processed.missing

    @staticmethod
    async def test_persist_and_flush(
        file_test_client: Files,
        test_config: db_model.Configuration,
        member_test_client: Members,
        file_parse_results_test_client: FileParseResults,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_client=member_test_client,
            file_client=file_test_client,
        )

        n_errors = 2
        n_valid = 20
        n_missing = 5

        # Old e9y file that we will associate some member records to
        old_file = await file_test_client.persist(
            model=FileFactory.create(id=1, organization_id=test_config.organization_id)
        )

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(id=2, organization_id=test_config.organization_id)
        )

        # Create some valid rows and some error rows
        errors = FileParseErrorFactory.create_batch(
            n_errors,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
        )
        valid = FileParseResultFactory.create_batch(
            n_valid,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        # Create some members that won't be in the new file
        await member_test_client.bulk_persist(
            models=MemberFactory.create_batch(
                n_missing,
                organization_id=test_config.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )

        parsed = ParsedFileRecords(errors=errors, valid=valid)

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        # Check that
        #  - errors are cleared out for file
        #  - missing member records are expired
        #  - valid members are persisted in db

        members_from_old_file = await member_test_client.get_for_file(
            file_id=old_file.id
        )
        members_from_new_file = await member_test_client.get_for_file(
            file_id=new_file.id
        )

        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        # Check that the temp tables are flushed out
        assert not errors and not results
        # Check that all missing members are expired
        assert all([m.effective_range.upper for m in members_from_old_file])
        # Check that all our new members are not expired
        assert all([not m.effective_range.upper for m in members_from_new_file])
        # Check that all members from new file are persisted
        assert set(
            [
                (m.organization_id, m.unique_corp_id, m.dependent_id)
                for m in members_from_new_file
            ]
        ) == set(
            [
                (
                    m.organization_id,
                    m.unique_corp_id,
                    m.dependent_id,
                )
                for m in valid
            ]
        )

    @staticmethod
    async def test_persist_and_flush_org_affiliations(
        file_test_client: Files,
        member_test_client: Members,
        file_parse_results_test_client: FileParseResults,
        configuration_test_client: Configurations,
    ):
        # Given
        n_errors_a = 2
        n_errors_b = 1

        n_valid_a = 20
        n_valid_b = 30

        n_missing_a = 5
        n_missing_b = 3

        config_data_provider = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )

        config_a = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )

        config_b = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_a.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="a",
            )
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_b.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="b",
            )
        )

        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_client=member_test_client,
            file_client=file_test_client,
        )

        # Old e9y file attached to data provider that we will associate some member records to
        old_file = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )

        # Create some valid rows and some error rows
        errors_a = FileParseErrorFactory.create_batch(
            n_errors_a,
            file_id=new_file.id,
            organization_id=config_a.organization_id,
        )
        errors_b = FileParseErrorFactory.create_batch(
            n_errors_b,
            file_id=new_file.id,
            organization_id=config_b.organization_id,
        )
        valid_a = FileParseResultFactory.create_batch(
            n_valid_a,
            file_id=new_file.id,
            organization_id=config_a.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )
        valid_b = FileParseResultFactory.create_batch(
            n_valid_b,
            file_id=new_file.id,
            organization_id=config_b.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        # Create some members that won't be in the new file
        await member_test_client.bulk_persist(
            models=MemberFactory.create_batch(
                n_missing_a,
                organization_id=config_a.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )
        await member_test_client.bulk_persist(
            models=MemberFactory.create_batch(
                n_missing_b,
                organization_id=config_b.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )

        parsed = ParsedFileRecords(errors=errors_a + errors_b, valid=valid_a + valid_b)

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        # Check that
        #  - errors are cleared out for file
        #  - missing member records are expired
        #  - valid members are persisted in db

        members_from_old_file = await member_test_client.get_for_file(
            file_id=old_file.id
        )
        members_from_new_file = await member_test_client.get_for_file(
            file_id=new_file.id
        )

        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        # Check that the temp tables are flushed out
        assert not errors and not results
        # Check that all missing members are expired
        assert all([m.effective_range.upper for m in members_from_old_file])
        # Check that all our new members are not expired
        assert all([not m.effective_range.upper for m in members_from_new_file])
        # Check that all members from new file are persisted
        assert set(
            [
                (m.organization_id, m.unique_corp_id, m.dependent_id)
                for m in members_from_new_file
            ]
        ) == set(
            [
                (
                    m.organization_id,
                    m.unique_corp_id,
                    m.dependent_id,
                )
                for m in valid_a + valid_b
            ]
        )


class TestMemberVersioned:
    @staticmethod
    async def test_persist(
        file_test_client: Files,
        test_config: db_model.Configuration,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
        )

        n_errors = 2
        n_valid = 20

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(id=2, organization_id=test_config.organization_id)
        )

        # Create some valid rows and some error rows
        errors = FileParseErrorFactory.create_batch(
            n_errors,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
        )
        valid = FileParseResultFactory.create_batch(
            n_valid,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        parsed = ParsedFileRecords(errors=errors, valid=valid)

        # When
        processed: ProcessedRecords = await repo.persist(
            parsed_records=parsed, file=new_file
        )

        # Then
        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        assert n_valid == processed.valid == len(results)
        assert n_errors == processed.errors == len(errors)

    @staticmethod
    async def test_persist_and_flush(
        file_test_client: Files,
        configuration_test_client: Configurations,
        test_config: db_model.Configuration,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )

        n_errors = 2
        n_valid = 20
        n_missing = 5

        # Old e9y file that we will associate some member records to
        old_file = await file_test_client.persist(
            model=FileFactory.create(id=1, organization_id=test_config.organization_id)
        )

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(id=2, organization_id=test_config.organization_id)
        )

        # Create some valid rows and some error rows
        errors = FileParseErrorFactory.create_batch(
            n_errors,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
        )
        valid = FileParseResultFactory.create_batch(
            n_valid,
            file_id=new_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        # Create some members that won't be in the new file
        await member_versioned_test_client.bulk_persist(
            models=MemberVersionedFactory.create_batch(
                n_missing,
                organization_id=test_config.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )

        parsed = ParsedFileRecords(errors=errors, valid=valid)

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        # Check that
        #  - errors are cleared out for file
        #  - missing member records are expired
        #  - valid members are persisted in db

        members_from_old_file = await member_versioned_test_client.get_for_file(
            file_id=old_file.id
        )
        members_from_new_file = await member_versioned_test_client.get_for_file(
            file_id=new_file.id
        )

        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        # Check that the temp tables are flushed out
        assert not errors and not results
        # Check that all missing members are expired
        assert all([m.effective_range.upper for m in members_from_old_file])
        # Check that all our new members are not expired
        assert all([not m.effective_range.upper for m in members_from_new_file])
        # Check that all members from new file are persisted
        assert set(
            [
                (m.organization_id, m.unique_corp_id, m.dependent_id)
                for m in members_from_new_file
            ]
        ) == set(
            [
                (
                    m.organization_id,
                    m.unique_corp_id,
                    m.dependent_id,
                )
                for m in valid
            ]
        )

    @staticmethod
    async def test_persist_and_flush_org_affiliations(
        file_test_client: Files,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
        configuration_test_client: Configurations,
    ):
        # Given
        n_errors_a = 2
        n_errors_b = 1

        n_valid_a = 20
        n_valid_b = 30

        n_missing_a = 5
        n_missing_b = 3

        config_data_provider = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )

        config_a = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )

        config_b = await configuration_test_client.persist(
            model=ConfigurationFactory.create()
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_a.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="a",
            )
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_b.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="b",
            )
        )

        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )

        # Old e9y file attached to data provider that we will associate some member records to
        old_file = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )

        # New e9y file that we will ingest and persist
        new_file = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )

        # Create some valid rows and some error rows
        errors_a = FileParseErrorFactory.create_batch(
            n_errors_a,
            file_id=new_file.id,
            organization_id=config_a.organization_id,
        )
        errors_b = FileParseErrorFactory.create_batch(
            n_errors_b,
            file_id=new_file.id,
            organization_id=config_b.organization_id,
        )
        valid_a = FileParseResultFactory.create_batch(
            n_valid_a,
            file_id=new_file.id,
            organization_id=config_a.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )
        valid_b = FileParseResultFactory.create_batch(
            n_valid_b,
            file_id=new_file.id,
            organization_id=config_b.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        # Create some members that won't be in the new file
        await member_versioned_test_client.bulk_persist(
            models=MemberVersionedFactory.create_batch(
                n_missing_a,
                organization_id=config_a.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )
        await member_versioned_test_client.bulk_persist(
            models=MemberVersionedFactory.create_batch(
                n_missing_b,
                organization_id=config_b.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            ),
        )

        parsed = ParsedFileRecords(errors=errors_a + errors_b, valid=valid_a + valid_b)

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        # Check that
        #  - errors are cleared out for file
        #  - missing member records are expired
        #  - valid members are persisted in db

        members_from_old_file = await member_versioned_test_client.get_for_file(
            file_id=old_file.id
        )
        members_from_new_file = await member_versioned_test_client.get_for_file(
            file_id=new_file.id
        )

        errors = await file_parse_results_test_client.get_file_parse_errors_for_file(
            file_id=new_file.id
        )
        results = await file_parse_results_test_client.get_file_parse_results_for_file(
            file_id=new_file.id
        )

        # Check that the temp tables are flushed out
        assert not errors and not results
        # Check that all missing members are expired
        assert all([m.effective_range.upper for m in members_from_old_file])
        # Check that all our new members are not expired
        assert all([not m.effective_range.upper for m in members_from_new_file])
        # Check that all members from new file are persisted
        assert set(
            [
                (m.organization_id, m.unique_corp_id, m.dependent_id)
                for m in members_from_new_file
            ]
        ) == set(
            [
                (
                    m.organization_id,
                    m.unique_corp_id,
                    m.dependent_id,
                )
                for m in valid_a + valid_b
            ]
        )

    @staticmethod
    async def test_pre_verification_after_ingest(
        file_test_client: Files,
        test_config: db_model.Configuration,
        member_versioned_test_client: MembersVersioned,
        configuration_test_client: Configurations,
        file_parse_results_test_client: FileParseResults,
        verification_test_client: Verifications,
        member_verification_test_client: MemberVerifications,
    ):
        """Test that pre-verification is running immediately after file ingest"""
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        old_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        new_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        # Create a member
        member: db_model.MemberVersioned = await member_versioned_test_client.persist(
            model=MemberVersionedFactory.create(
                organization_id=test_config.organization_id,
                file_id=old_file.id,
                effective_range=DateRangeFactory(upper=None),
            )
        )
        # Create a verification
        verification: db_model.Verification = await verification_test_client.persist(
            model=VerificationFactory.create(
                organization_id=test_config.organization_id
            )
        )
        await member_verification_test_client.persist(
            model=MemberVerificationFactory.create(
                verification_id=verification.id, member_id=member.id
            )
        )
        # Create a new file_parse_result
        new_result: db_model.FileParseResult = FileParseResultFactory.create(
            file_id=new_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
            # # pre-verification params that need to match member
            first_name=member.first_name,
            last_name=member.last_name,
            date_of_birth=member.date_of_birth,
            email=member.email,
            unique_corp_id=member.unique_corp_id,
            work_state=member.work_state,
        )
        parsed = ParsedFileRecords(errors=[], valid=[new_result])

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        member_verifications: List[
            db_model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification.id
        )

        assert len(member_verifications) == 2

    @staticmethod
    async def test_pre_verification_after_ingest_org_affiliations(
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
        verification_test_client: Verifications,
        member_verification_test_client: MemberVerifications,
    ):
        """Test that pre-verification works on org-affiliations files"""
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        config_data_provider = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )

        config_a = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=False)
        )

        config_b = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=False)
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_a.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="a",
            )
        )

        await configuration_test_client.add_external_id(
            **ExternalIDFactory.create(
                organization_id=config_b.organization_id,
                data_provider_organization_id=config_data_provider.organization_id,
                external_id="b",
            )
        )
        old_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )
        new_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(
                organization_id=config_data_provider.organization_id
            )
        )
        # Create a member
        member_org_a: db_model.MemberVersioned = (
            await member_versioned_test_client.persist(
                model=MemberVersionedFactory.create(
                    organization_id=config_a.organization_id,
                    file_id=old_file.id,
                    effective_range=DateRangeFactory(upper=None),
                )
            )
        )
        member_org_b: db_model.MemberVersioned = (
            await member_versioned_test_client.persist(
                model=MemberVersionedFactory.create(
                    organization_id=config_b.organization_id,
                    file_id=old_file.id,
                    effective_range=DateRangeFactory(upper=None),
                )
            )
        )
        # Create a verification
        verification_a: db_model.Verification = await verification_test_client.persist(
            model=VerificationFactory.create(organization_id=config_a.organization_id)
        )
        verification_b: db_model.Verification = await verification_test_client.persist(
            model=VerificationFactory.create(organization_id=config_b.organization_id)
        )
        await member_verification_test_client.persist(
            model=MemberVerificationFactory.create(
                verification_id=verification_a.id, member_id=member_org_a.id
            )
        )
        await member_verification_test_client.persist(
            model=MemberVerificationFactory.create(
                verification_id=verification_b.id, member_id=member_org_b.id
            )
        )
        # Create a new file_parse_result
        new_result_a: db_model.FileParseResult = FileParseResultFactory.create(
            file_id=new_file.id,
            organization_id=config_a.organization_id,
            effective_range=DateRangeFactory(upper=None),
            # # pre-verification params that need to match member
            first_name=member_org_a.first_name,
            last_name=member_org_a.last_name,
            date_of_birth=member_org_a.date_of_birth,
            email=member_org_a.email,
            unique_corp_id=member_org_a.unique_corp_id,
            work_state=member_org_a.work_state,
        )
        new_result_b: db_model.FileParseResult = FileParseResultFactory.create(
            file_id=new_file.id,
            organization_id=config_b.organization_id,
            effective_range=DateRangeFactory(upper=None),
            # # pre-verification params that need to match member
            first_name=member_org_b.first_name,
            last_name=member_org_b.last_name,
            date_of_birth=member_org_b.date_of_birth,
            email=member_org_b.email,
            unique_corp_id=member_org_b.unique_corp_id,
            work_state=member_org_b.work_state,
        )
        parsed = ParsedFileRecords(errors=[], valid=[new_result_a, new_result_b])

        # When
        await repo.persist(parsed_records=parsed, file=new_file)
        await repo.flush(file=new_file)

        # Then
        member_verifications_a: List[
            db_model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification_a.id
        )
        member_verifications_b: List[
            db_model.MemberVerification
        ] = await member_verification_test_client.get_for_verification_id(
            verification_id=verification_a.id
        )

        assert len(member_verifications_a) == 2 and len(member_verifications_b) == 2


class TestGetOrganizationIdsForFile:
    @staticmethod
    async def test_get_organization_ids_for_file(
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
        verification_test_client: Verifications,
        member_verification_test_client: MemberVerifications,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        data_provider_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )
        sub_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=False)
        )
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id="external_id",
        )
        file = FileFactory.create(organization_id=data_provider_org.organization_id)
        # When
        org_ids = await repo.get_organization_ids_for_file(file=file)

        # Then
        assert len(org_ids) == 1

    @staticmethod
    async def test_get_organization_ids_for_file_multiple(
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
        verification_test_client: Verifications,
        member_verification_test_client: MemberVerifications,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        data_provider_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )
        sub_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=False)
        )
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id="external_id",
        )
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id="another_id",
        )
        file = FileFactory.create(organization_id=data_provider_org.organization_id)
        # When
        org_ids = await repo.get_organization_ids_for_file(file=file)

        # Then
        assert len(org_ids) == 1


class TestFileRecordHashing:
    @staticmethod
    async def test_reloading_previously_expired_records(
        test_config: db_model.Configuration,
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
    ):
        """Test that when a member is expired and valid again, it is valid"""
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        # Full file
        first_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        # Missing 1 member
        second_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        # Full file again
        third_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )

        # When
        # Process first file
        records = FileParseResultFactoryWithHash.create_batch(
            10,
            organization_id=test_config.organization_id,
            file_id=first_file.id,
            effective_range=None,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        await repo.flush(file=first_file)

        # update the file_id and process second file
        second_file_records = copy.deepcopy(records)
        second_file_records.pop()

        for r in second_file_records:
            r.file_id = second_file.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=second_file_records
        )

        await repo.flush(file=second_file)

        # update the file_id and process third file
        third_file_records = copy.deepcopy(records)

        for r in third_file_records:
            r.file_id = third_file.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=third_file_records
        )

        await repo.flush(file=third_file)

        # Then
        members = await member_versioned_test_client.get_for_file(file_id=third_file.id)

        assert len(members) == 10 and all(
            [m.effective_range.upper is None for m in members]
        )

    @staticmethod
    async def test_reloading_previously_expired_record_new_is_inserted(
        test_config: db_model.Configuration,
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
    ):
        """Test that we have 2 records, 1 expired and 1 valid when a member is removed and then added to a file"""
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        # Full file
        first_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        # Missing 1 member
        second_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )
        # Full file again
        third_file: db_model.File = await file_test_client.persist(
            model=FileFactory.create(organization_id=test_config.organization_id)
        )

        # When
        # Process first file
        records = FileParseResultFactoryWithHash.create_batch(
            10,
            organization_id=test_config.organization_id,
            file_id=first_file.id,
            effective_range=None,
        )
        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=records
        )

        await repo.flush(file=first_file)

        # update the file_id and process second file
        second_file_records = copy.deepcopy(records)
        expected = second_file_records.pop()

        for r in second_file_records:
            r.file_id = second_file.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=second_file_records
        )

        await repo.flush(file=second_file)

        # update the file_id and process third file
        third_file_records = copy.deepcopy(records)

        for r in third_file_records:
            r.file_id = third_file.id

        await file_parse_results_test_client.bulk_persist_file_parse_results(
            results=third_file_records
        )

        await repo.flush(file=third_file)

        # Then
        all_members = await member_versioned_test_client.all()

        versions = [
            m
            for m in all_members
            if (m.unique_corp_id, m.dependent_id)
            == (expected.unique_corp_id, expected.dependent_id)
        ]

        assert (
            len(versions) == 2
            and len([v for v in versions if v.effective_range.upper is None]) == 1
        )


class TestGetSuccessCountFromPreviousFile:
    @staticmethod
    async def test_get_success_count_from_previous_file_with_no_previous(
        file_test_client: Files,
        configuration_test_client: Configurations,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
        verification_test_client: Verifications,
        member_verification_test_client: MemberVerifications,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
            config_client=configuration_test_client,
        )
        data_provider_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=True)
        )
        sub_org = await configuration_test_client.persist(
            model=ConfigurationFactory.create(data_provider=False)
        )
        await configuration_test_client.add_external_id(
            data_provider_organization_id=data_provider_org.organization_id,
            organization_id=sub_org.organization_id,
            external_id="external_id",
        )
        FileFactory.create(organization_id=data_provider_org.organization_id)
        # When
        success_count_previous_file = await repo.get_success_count_from_previous_file(
            organization_id=data_provider_org.organization_id
        )

        # Then
        assert success_count_previous_file is not None
        assert success_count_previous_file == 0

    @staticmethod
    async def test_get_success_count_from_previous_file(
        file_test_client: Files,
        test_config: db_model.Configuration,
        member_versioned_test_client: MembersVersioned,
        file_parse_results_test_client: FileParseResults,
    ):
        # Given
        repo = ParsedRecordsDatabaseRepository(
            fpr_client=file_parse_results_test_client,
            member_versioned_client=member_versioned_test_client,
            file_client=file_test_client,
        )

        n_errors_previous = 2
        n_valid_previous = 20

        # previous file that is being processed
        previous_file = await file_test_client.persist(
            model=FileFactory.create(id=1, organization_id=test_config.organization_id)
        )
        await file_test_client.persist(
            model=FileFactory.create(id=2, organization_id=test_config.organization_id)
        )

        # Create some valid rows and some error rows
        errors_previous = FileParseErrorFactory.create_batch(
            n_errors_previous,
            file_id=previous_file.id,
            organization_id=test_config.organization_id,
        )
        valid_previous = FileParseResultFactory.create_batch(
            n_valid_previous,
            file_id=previous_file.id,
            organization_id=test_config.organization_id,
            effective_range=DateRangeFactory(upper=None),
        )

        parsed_previous = ParsedFileRecords(
            errors=errors_previous, valid=valid_previous
        )

        # When
        await repo.persist(parsed_records=parsed_previous, file=previous_file)
        await repo.persist_file_counts(
            file=previous_file,
            success_count=n_valid_previous,
            failure_count=n_errors_previous,
        )

        # Then
        success_previous = await repo.get_success_count_from_previous_file(
            organization_id=test_config.organization_id
        )

        assert success_previous == n_valid_previous
