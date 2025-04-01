from __future__ import annotations

import datetime
import uuid
from collections import namedtuple
from typing import List, Tuple

import asyncpg
import structlog

from app.utils import feature_flag
from db import model as db_model
from db.clients import member_verification_client as mv_client
from db.clients import verification_2_client as v_2_client
from db.clients import verification_attempt_client as v_attempt_client
from db.clients import verification_client as v_client

logger = structlog.getLogger(__name__)


class VerificationRepository:
    def __init__(
        self,
        verification_client: v_client.Verifications | None = None,
        verification_attempt_client: (
            v_attempt_client.VerificationAttempts | None
        ) = None,
        member_verification_client: mv_client.MemberVerifications | None = None,
        verification_2_client: v_2_client.Verification2Client | None = None,
    ):
        self._verification_client: v_client.Verifications = (
            verification_client or v_client.Verifications()
        )
        self._verification_attempt_client: v_attempt_client.VerificationAttempts = (
            verification_attempt_client or v_attempt_client.VerificationAttempts()
        )
        self._member_verification_client: mv_client.MemberVerifications = (
            member_verification_client or mv_client.MemberVerifications()
        )
        self._verification_2_client: v_2_client.Verification2Client = (
            verification_2_client or v_2_client.Verification2Client()
        )

    # region persist
    async def create_verification(
        self,
        user_id: int,
        verification_type: str,
        organization_id: int,
        unique_corp_id: int,
        dependent_id: int = "",
        first_name: str = "",
        last_name: str = "",
        email: str = "",
        work_state: str = "",
        session_id: str = "",
        date_of_birth: datetime.date = None,
        deactivated_at: datetime.datetime = None,
        verified_at: datetime.datetime = None,
        additional_fields: dict = {},
        verification_session: uuid | None = None,
        verification_2_id: int = None,
        connection: asyncpg.Connection = None,
    ) -> db_model.Verification:

        if verified_at is None:
            verified_at = datetime.datetime.now()

        """Create a verification record for a user"""
        return await self._verification_client.persist(
            user_id=user_id,
            verification_type=verification_type,
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            work_state=work_state,
            deactivated_at=deactivated_at,
            date_of_birth=date_of_birth,
            verified_at=verified_at,
            additional_fields=additional_fields,
            verification_session=verification_session,
            verification_2_id=verification_2_id,
            connection=connection,
        )

    async def create_verification_2(
        self,
        user_id: int,
        verification_type: str,
        organization_id: int,
        unique_corp_id: str,
        dependent_id: str,
        first_name: str,
        last_name: str,
        email: str,
        work_state: str,
        date_of_birth: datetime.date,
        additional_fields: dict,
        verification_session: uuid | None,
        *,
        deactivated_at: datetime.datetime | None = None,
        verified_at: datetime.datetime | None = None,
        eligibility_member_id: int | None = None,
        eligibility_member_version: int | None = None,
        connection: asyncpg.Connection = None,
    ) -> db_model.Verification2:

        if verified_at is None:
            verified_at = datetime.datetime.now()

        verification_2 = await self._verification_2_client.persist(
            user_id=user_id,
            organization_id=organization_id,
            verification_type=verification_type,  # convert?
            date_of_birth=date_of_birth,
            first_name=first_name,
            last_name=last_name,
            email=email,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
            work_state=work_state,
            deactivated_at=deactivated_at,
            verified_at=verified_at,
            additional_fields=additional_fields,
            verification_session=verification_session,
            member_id=eligibility_member_id,
            member_version=eligibility_member_version,
            connection=connection,
        )
        # TODO: https://mavenclinic.atlassian.net/browse/ELIG-2259
        # Once e9y 2 audit store is ready, need persist the success verification attempt to audit store
        logger.info(
            "Verfication2 created successfully",
            verification_id=verification_2.id,
            user_id=user_id,
            organization_id=organization_id,
            verification_type=verification_type,
            verified_at=verified_at,
            erification_session=verification_session,
            member_id=eligibility_member_id,
            member_version=eligibility_member_version,
        )

        return verification_2

    async def create_verification_dual_write(
        self,
        user_id: int,
        verification_type: str,
        organization_id: int,
        unique_corp_id: str,
        dependent_id: str,
        first_name: str,
        last_name: str,
        email: str,
        work_state: str,
        date_of_birth: datetime.date,
        additional_fields: dict,
        verification_session: uuid | None,
        *,
        deactivated_at: datetime.datetime | None = None,
        verified_at: datetime.datetime | None = None,
        eligibility_member_1_id: int | None = None,
        eligibility_member_2_id: int | None = None,
        eligibility_member_2_version: int | None = None,
    ) -> Tuple[db_model.Verification, db_model.Verification2]:
        if verified_at is None:
            verified_at = datetime.datetime.now()

        async with self._verification_2_client.client.connector.transaction() as c:
            verification_2 = await self.create_verification_2(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                date_of_birth=date_of_birth,
                additional_fields=additional_fields,
                verification_session=verification_session,
                deactivated_at=deactivated_at,
                verified_at=verified_at,
                eligibility_member_id=eligibility_member_2_id,
                eligibility_member_version=eligibility_member_2_version,
                connection=c,
            )

            verification_1 = await self.create_verification(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                date_of_birth=date_of_birth,
                deactivated_at=deactivated_at,
                verified_at=verified_at,
                additional_fields=additional_fields,
                verification_session=verification_session,
                verification_2_id=verification_2.id,
                connection=c,
            )

            verification_attempt = await self.create_verification_attempt(
                verification_type=verification_type,
                additional_fields=additional_fields,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                dependent_id=dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=email,
                work_state=work_state,
                # TODO - add this later when we begin logging which policy is used to create a verification
                policy_used="",
                verification_id=verification_1.id,
                user_id=user_id,
                date_of_birth=date_of_birth,
                verified_at=verified_at,
                connection=c,
            )

            if eligibility_member_1_id:
                await self.create_member_verification(
                    member_id=eligibility_member_1_id,
                    verification_id=verification_1.id,
                    verification_attempt_id=verification_attempt.id,
                    connection=c,
                )

        return verification_1, verification_2

    async def create_multiple_verification_dual_write(
        self,
        user_id: int,
        verification_type: db_model.VerificationTypes,
        verification_data_list: list[db_model.VerificationData],
        first_name: str = "",
        last_name: str = "",
        date_of_birth: datetime.date = None,
        deactivated_at: datetime.datetime = None,
        verified_at: datetime.datetime = None,
        verification_session: uuid | None = None,
    ):
        if verified_at is None:
            verified_at = datetime.datetime.now()

        async with self._verification_2_client.client.connector.transaction() as c:
            # Create verification of 2.0, and 1.0
            verification_2_models = []
            for verification_data in verification_data_list:
                verification_2_model = db_model.Verification2(
                    user_id=user_id,
                    verification_type=verification_type,
                    organization_id=verification_data.organization_id,
                    unique_corp_id=verification_data.unique_corp_id,
                    dependent_id=verification_data.dependent_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=verification_data.email,
                    work_state=verification_data.work_state,
                    deactivated_at=deactivated_at,
                    date_of_birth=date_of_birth,
                    verified_at=verified_at,
                    additional_fields=verification_data.additional_fields,
                    verification_session=verification_session,
                    member_id=verification_data.member_2_id,
                    member_version=verification_data.member_2_version,
                )
                verification_2_models.append(verification_2_model)

            verifications_2_list = await self._verification_2_client.bulk_persist(
                models=verification_2_models, connection=c
            )
            # Link verification records of 1.0 and 2.0
            verification_models = []
            for v_2 in verifications_2_list:
                verification_model = db_model.Verification(
                    user_id=user_id,
                    verification_type=verification_type,
                    organization_id=verification_data.organization_id,
                    unique_corp_id=verification_data.unique_corp_id,
                    dependent_id=verification_data.dependent_id,
                    first_name=first_name,
                    last_name=last_name,
                    email=verification_data.email,
                    work_state=verification_data.work_state,
                    deactivated_at=deactivated_at,
                    date_of_birth=date_of_birth,
                    verified_at=verified_at,
                    additional_fields=verification_data.additional_fields,
                    verification_session=verification_session,
                    verification_2_id=v_2.id,
                )
                verification_models.append(verification_model)
            verification_1_list = await self._verification_client.bulk_persist(
                models=verification_models, connection=c
            )
            verification_1_organization_map = {
                v.organization_id: v for v in verification_1_list
            }
            for data in verification_data_list:
                verification = verification_1_organization_map.get(data.organization_id)
                if verification is not None:
                    data.verification_id = verification.id

            verification_attempts = await self.create_multiple_verification_attempts(
                verification_type=verification_type,
                date_of_birth=date_of_birth,
                verification_data_list=verification_data_list,
                first_name=first_name,
                last_name=last_name,
                policy_used={},  # TODO: Add policy when logging is implemented
                verified_at=verified_at,
                user_id=user_id,
                connection=c,
            )
            verification_attempt_organization_map = {
                va.organization_id: va for va in verification_attempts
            }
            # Map verification attempts back to verification_data_list
            for data in verification_data_list:
                attempt = verification_attempt_organization_map.get(
                    data.organization_id
                )
                if attempt is not None:
                    data.verification_attempt_id = attempt.id

            # Tie member to verification
            mv_records = []
            for data in verification_data_list:
                if data.member_1_id in (None, 0) or data.verification_id in (
                    None,
                    0,
                ):
                    logger.info(
                        "Skip creating member_verification record due to missing verification_id/member_id of v2",
                        member_id=data.member_1_id,
                        verification_id=data.verification_id,
                        verification_attempt_id=data.verification_attempt_id,
                    )
                    continue
                record = db_model.MemberVerification(
                    member_id=data.member_1_id,
                    verification_id=data.verification_id,
                    verification_attempt_id=data.verification_attempt_id,
                )
                mv_records.append(record)
            if mv_records:
                await self.create_multiple_member_verifications(
                    records_to_save=mv_records, connection=c
                )

    VerificationRecord = namedtuple(
        "VerificationRecord",
        [
            "verification_id",
            "user_id",
            "verification_type",
            "organization_id",
            "unique_corp_id",
            "dependent_id",
            "first_name",
            "last_name",
            "email",
            "work_state",
            "deactivated_at",
            "date_of_birth",
            "successful_verification",
            "verified_at",
            "additional_fields",
            "verification_session",
            "policy_used",
        ],
    )

    async def create_multiple_verifications(
        self,
        user_id: int,
        verification_type: str,
        verification_data_list: list[db_model.VerificationData],
        first_name: str = "",
        last_name: str = "",
        session_id: str = "",
        date_of_birth: datetime.date = None,
        deactivated_at: datetime.datetime = None,
        verified_at: datetime.datetime = None,
        verification_session: uuid | None = None,
        connection: asyncpg.Connection = None,
    ) -> List[db_model.Verification]:
        """Create multiple verification records for a user"""

        if verified_at is None:
            verified_at = datetime.datetime.now()

        records_to_create = self._get_verification_records_to_be_created(
            user_id=user_id,
            verification_type=db_model.VerificationTypes(verification_type),
            verification_data_list=verification_data_list,
            first_name=first_name,
            last_name=last_name,
            date_of_birth=date_of_birth,
            deactivated_at=deactivated_at,
            verified_at=verified_at,
            verification_session=verification_session,
        )

        return await self._verification_client.bulk_persist(
            models=records_to_create, connection=connection
        )

    def _get_verification_records_to_be_created(
        self,
        user_id: int,
        verification_type: db_model.VerificationTypes,
        verification_data_list: list[db_model.VerificationData],
        first_name: str = "",
        last_name: str = "",
        date_of_birth: datetime.date = None,
        deactivated_at: datetime.datetime = None,
        verified_at: datetime.datetime = None,
        verification_session: uuid | None = None,
    ) -> List[db_model.Verification]:
        """convert to a list of VerificationRecord objects"""

        verification_records = []
        for verification_data in verification_data_list:
            record = db_model.Verification(
                user_id=user_id,
                verification_type=verification_type,
                organization_id=verification_data.organization_id,
                unique_corp_id=verification_data.unique_corp_id,
                dependent_id=verification_data.dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=verification_data.email,
                work_state=verification_data.work_state,
                deactivated_at=deactivated_at,
                date_of_birth=date_of_birth,
                verified_at=verified_at,
                additional_fields=verification_data.additional_fields,
                verification_session=verification_session,
            )
            verification_records.append(record)
        return verification_records

    async def create_verification_attempt(
        self,
        verification_type: str,
        additional_fields: dict | None = {},
        organization_id: int = None,
        unique_corp_id: int = None,
        dependent_id: int = "",
        first_name: str = "",
        last_name: str = "",
        email: str = "",
        work_state: str = "",
        policy_used: str = "",
        verification_id: int | None = None,
        user_id: int | None = None,
        date_of_birth: datetime.date | None = None,
        verified_at: datetime.datetime | None = None,
        connection: asyncpg.Connection = None,
    ) -> db_model.VerificationAttempt:
        """Create a verification attempt record for a user - may be successful/unsuccessful"""

        successful_verification = False
        if verification_id:
            successful_verification = True

        if verified_at is None:
            verified_at = datetime.datetime.now()

        return await self._verification_attempt_client.persist(
            verification_type=verification_type,
            organization_id=organization_id,
            unique_corp_id=unique_corp_id,
            dependent_id=dependent_id,
            first_name=first_name,
            last_name=last_name,
            email=email,
            work_state=work_state,
            policy_used=policy_used,
            successful_verification=successful_verification,
            date_of_birth=date_of_birth,
            verification_id=verification_id,
            verified_at=verified_at,
            additional_fields=additional_fields,
            user_id=user_id,
            connection=connection,
        )

    async def create_multiple_verification_attempts(
        self,
        verification_data_list: list[db_model.VerificationData],
        verification_type: str,
        first_name: str = "",
        last_name: str = "",
        policy_used: dict = "",
        user_id: int | None = None,
        date_of_birth: datetime.date | None = None,
        verified_at: datetime.datetime | None = None,
        connection: asyncpg.Connection = None,
    ) -> List[db_model.VerificationAttempt]:
        """Create multiple verification attempt records for a user - may be successful/unsuccessful"""

        records_to_create = self._get_verification_attempt_records_to_be_created(
            verification_data_list=verification_data_list,
            verification_type=db_model.VerificationTypes(verification_type),
            first_name=first_name,
            last_name=last_name,
            policy_used=policy_used,
            verified_at=verified_at,
            user_id=user_id,
            date_of_birth=date_of_birth,
        )
        return await self._verification_attempt_client.bulk_persist(
            models=records_to_create, connection=connection
        )

    def _get_verification_attempt_records_to_be_created(
        self,
        verification_data_list: List[db_model.VerificationData],
        verification_type: db_model.VerificationTypes,
        first_name: str = "",
        last_name: str = "",
        policy_used: dict = "",
        user_id: int | None = None,
        date_of_birth: datetime.date | None = None,
        verified_at: datetime.datetime | None = None,
    ) -> List[db_model.VerificationAttempt]:
        """convert to a list of VerificationAttemptRecord objects"""

        verification_attempt_records = []
        for verification_data in verification_data_list:
            successful_verification = False
            verification_id = verification_data.verification_id

            if verification_id:
                successful_verification = True

            if verified_at is None:
                verified_at = datetime.datetime.now()

            record = db_model.VerificationAttempt(
                verification_type=verification_type,
                organization_id=verification_data.organization_id,
                unique_corp_id=verification_data.unique_corp_id,
                dependent_id=verification_data.dependent_id,
                first_name=first_name,
                last_name=last_name,
                email=verification_data.email,
                work_state=verification_data.work_state,
                policy_used=policy_used,
                successful_verification=successful_verification,
                date_of_birth=date_of_birth,
                verification_id=verification_id,
                verified_at=verified_at,
                additional_fields=verification_data.additional_fields,
                user_id=user_id,
            )
            verification_attempt_records.append(record)
        return verification_attempt_records

    async def create_member_verification(
        self,
        member_id: int,
        verification_id: int = None,
        verification_attempt_id: int = None,
        connection: asyncpg.Connection = None,
    ) -> db_model.MemberVerification:
        """Create a tie between a member record and a verification_attempt/verification record"""

        if not verification_id and not verification_attempt_id:
            raise ValueError(
                "Call to create member_verification with no verification/verification_attempt ID provided"
            )

        return await self._member_verification_client.persist(
            member_id=member_id,
            verification_id=verification_id,
            verification_attempt_id=verification_attempt_id,
            connection=connection,
        )

    async def create_multiple_member_verifications(
        self,
        records_to_save: List[db_model.MemberVerification],
        connection: asyncpg.Connection = None,
    ) -> List[db_model.MemberVerification]:
        """Create ties between member records and verification_attempts/verification records"""
        return await self._member_verification_client.bulk_persist(
            models=records_to_save, connection=connection
        )

    # endregion

    # region fetch

    async def get_verification_for_member(self, member_id) -> db_model.Verification:
        """Grab the most recent successful verification for a member"""

        return await self._verification_client.get_for_member_id(member_id=member_id)

    async def get_verification_2_for_member(self, member_id) -> db_model.Verification2:
        return await self._verification_2_client.get_for_member_id(member_id=member_id)

    async def get_verification_key_1_for_verification_2_id(
        self, verification_2_id
    ) -> db_model.VerificationKey:
        return (
            await self._verification_client.get_verification_key_for_verification_2_id(
                verification_2_id=verification_2_id
            )
        )

    async def get_all_verifications_for_member(
        self, member_id
    ) -> List[db_model.Verification]:
        """Get *all* successful verification records for a member"""
        return await self._verification_client.get_all_for_member_id(
            member_id=member_id
        )

    async def get_verification_attempts_for_member(self, member_id) -> dict:
        """Get a list of verification attempts for a member - both successful and unsuccessful"""
        member_verification_records = (
            await self._member_verification_client.get_all_for_member_id(
                member_id=member_id
            )
        )

        if not member_verification_records:
            return {}

        successful = []
        failed = []

        for mv in member_verification_records:
            # Grab the verification attempt corresponding to this record
            verification_attempt = await self._verification_attempt_client.get(
                mv.verification_attempt_id
            )

            if verification_attempt.successful_verification is True:
                successful.append(verification_attempt)
            else:
                failed.append(verification_attempt)

        return {"successful": successful, "failed": failed}

    async def get_user_ids_for_eligibility_member_id(self, member_id) -> List:
        """Return the list of users associated with an eligibility member ID"""
        return await self._verification_client.get_user_ids_for_eligibility_member_id(
            member_id=member_id
        )

    async def get_eligibility_member_id_for_user(self, user_id) -> int | None:
        """Return the most recent eligibility member ID associated with a user record"""
        return await self._verification_client.get_member_id_for_user_id(
            user_id=user_id
        )

    async def get_eligibility_member_id_for_user_and_org(
        self, user_id, organization_id
    ) -> int | None:
        """Return the most recent eligibility member ID associated with a user record"""
        return await self._verification_client.get_member_id_for_user_and_org(
            user_id=user_id, organization_id=organization_id
        )

    async def get_eligibility_member_ids_for_user(self):
        # TODO
        """Return all eligibility member IDs associated with a user record"""
        pass

    async def get_eligibility_verification_record_for_user(
        self, user_id
    ) -> db_model.EligibilityVerificationForUser:
        """Return the most recent eligibility record associated with a user ID"""
        verification_record: db_model.EligibilityVerificationForUser | None = await self._verification_client.get_eligibility_verification_record_for_user(
            user_id=user_id
        )

        if verification_record and feature_flag.organization_enabled_for_e9y_2_read(
            verification_record.organization_id
        ):
            verification_record.is_v2 = True

        return verification_record

    async def get_all_eligibility_verification_records_for_user(
        self, user_id
    ) -> List[db_model.EligibilityVerificationForUser]:
        """Return all eligibility verification records associated with a user across all organizations"""
        verification_records = await self._verification_client.get_all_eligibility_verification_record_for_user(
            user_id=user_id
        )
        results = []
        # Only returns verifications if the orgs are enabled/disabled accordingly
        if verification_records:
            for v in verification_records:
                if feature_flag.organization_enabled_for_e9y_2_read(v.organization_id):
                    v.is_v2 = True
                results.append(v)

        return results

    async def get_verification_key_for_user(self, user_id):
        """Return the latest verification record associated with a user across all organizations"""
        verification_record = (
            await self._verification_client.get_verification_key_for_user(
                user_id=user_id
            )
        )
        if not verification_record:
            return None

        if verification_record.verification_2_id:
            verification_2_record = (
                await self._verification_2_client.get_verification_key_for_id(
                    id=verification_record.verification_2_id
                )
            )
            # if 2.0 record found, link it
            if verification_2_record:
                verification_record.member_2_id = verification_2_record.member_2_id
                verification_record.member_2_version = (
                    verification_2_record.member_2_version
                )
                verification_record.is_v2 = True
                verification_record.created_at = verification_2_record.created_at
            else:
                verification_record.verification_2_id = None

        return verification_record

    async def get_verification_key_2_for_user_and_org(self, user_id, organization_id):
        """Return the latest verification record 2.0 associated with a user across all organizations"""
        return await self._verification_2_client.get_verification_key_for_user_and_org(
            user_id=user_id,
            organization_id=organization_id,
        )

    async def deactivate_verification_for_user(
        self, verification_id: int, user_id: int
    ) -> db_model.Verification:
        """Deactivate a verification record for a user"""
        deactivated_verification_1 = (
            await self._verification_client.deactivate_verification_record_for_user(
                verification_id=verification_id, user_id=user_id
            )
        )
        if deactivated_verification_1:
            if feature_flag.organization_enabled_for_e9y_2_write(
                deactivated_verification_1.organization_id
            ):
                verification_2_id = deactivated_verification_1.verification_2_id
                if not verification_2_id:
                    raise ValueError(
                        f"verification_2_id is not set in 2.0 for verification {verification_id}; user {user_id}"
                    )
                await self._verification_2_client.deactivate_verification_record_for_user(
                    verification_id=verification_2_id, user_id=user_id
                )

        return deactivated_verification_1

    # endregion
