from __future__ import annotations

from typing import Iterable, Iterator, List, Mapping, Optional, Tuple

import asyncpg
from verification.repository.utils import (
    convert_record_to_eligibility_verification_for_user,
)

from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import (
    BackfillMemberTrackEligibilityData,
    EligibilityVerificationForUser,
    Verification,
    VerificationKey,
)

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class Verifications(ServiceProtocol[Verification]):
    """A service for querying & mutating the `eligibility.verification` table.

    Usage:
        >>> verification = Verifications()
        >>> verification = Verification(organization_id=1, user_id=123, verification_type='standard')
        >>> verification = await verifications.persist(model=verification)
        >>> await verifications.get(verification.id)
    """

    model = Verification

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("verification", connector=connector)

    # region fetch operations

    @retry
    async def get_for_ids(
        self,
        *verification_ids: int,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_ids(
                c, verification_ids=verification_ids
            )

    @_coerceable(bulk=False)
    @retry
    async def get_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Verification:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_member_id(c, member_id=member_id)

    @_coerceable(bulk=True)
    @retry
    async def get_all_for_member_id(
        self,
        member_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[Verification]:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_all_for_member_id(
                c, member_id=member_id
            )

    @retry
    async def get_user_ids_for_eligibility_member_id(
        self,
        member_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> List:
        async with self.client.connector.connection(c=connection) as c:
            id_list = await self.client.queries.get_user_ids_for_eligibility_member_id(
                c, member_id=member_id
            )
            # Convert return value to an array of userIDs
            return [r["user_id"] for r in id_list]

    @_coerceable(bulk=True)
    @retry
    async def get_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Verification:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_verification_key_for_user(
        self,
        user_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> VerificationKey | None:
        async with self.client.connector.connection(c=connection) as c:
            v1_record = await self.client.queries.get_verification_key_for_user(
                c, user_id=user_id
            )
        if not v1_record:
            return None

        return VerificationKey(
            member_id=v1_record["member_id"],
            organization_id=v1_record["organization_id"],
            is_v2=False,
            created_at=v1_record["created_at"],
            verification_1_id=v1_record["verification_1_id"],
            verification_2_id=v1_record["verification_2_id"],
            member_2_id=None,
            member_2_version=None,
        )

    @retry
    async def get_verification_key_for_verification_2_id(
        self,
        verification_2_id: int,
        *,
        connection: asyncpg.Connection = None,
    ) -> VerificationKey | None:
        async with self.client.connector.connection(c=connection) as c:
            v1_record = (
                await self.client.queries.get_verification_key_for_verification_2_id(
                    c, verification_2_id=verification_2_id
                )
            )
        if not v1_record:
            return None

        return VerificationKey(
            member_id=v1_record["member_id"],
            organization_id=v1_record["organization_id"],
            is_v2=False,
            created_at=v1_record["created_at"],
            verification_1_id=v1_record["verification_1_id"],
            verification_2_id=v1_record["verification_2_id"],
            member_2_id=None,
            member_2_version=None,
        )

    @retry
    async def get_eligibility_verification_record_for_user(
        self,
        user_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> EligibilityVerificationForUser | None:
        async with self.client.connector.connection(c=connection) as c:
            records = (
                await self.client.queries.get_eligibility_verification_record_for_user(
                    c, user_id=user_id
                )
            )

            if records is None or records == []:
                return None

            # SQL limits records length to 1 so we pick the first
            return convert_record_to_eligibility_verification_for_user(
                record=records[0],
            )

    @retry
    async def get_all_eligibility_verification_record_for_user(
        self,
        user_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[List[EligibilityVerificationForUser]]:
        """Query all eligibility verification records for a user.
        Only the latest active verification record per organization is considered.
        return value will have multiple records if a member is overeligible
        """
        async with self.client.connector.connection(c=connection) as c:
            records = await self.client.queries.get_all_eligibility_verification_records_for_user(
                c, user_id=user_id
            )

            if records is None or records == []:
                return None

            result = []
            for record in records:
                converted_record = convert_record_to_eligibility_verification_for_user(
                    record=record,
                )
                result.append(converted_record)
            return result

    @retry
    async def get_member_id_for_user_id(
        self, *, user_id: int, connection: asyncpg.Connection = None
    ) -> int | None:
        async with self.client.connector.connection(c=connection) as c:
            result = await self.client.queries.get_member_id_for_user_id(
                c, user_id=user_id
            )
            if result:
                return result["member_id"]
            return result

    @retry
    async def get_member_id_for_user_and_org(
        self,
        *,
        user_id: int,
        organization_id: int,
        connection: asyncpg.Connection = None,
    ) -> int | None:
        async with self.client.connector.connection(c=connection) as c:
            result = await self.client.queries.get_member_id_for_user_and_org(
                c, user_id=user_id, organization_id=organization_id
            )
            if result:
                return result["member_id"]
            return result

    @retry
    async def get_count_for_org(
        self, organization_id: int, *, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_count_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_counts_for_orgs(
        self,
        *organization_ids: int,
        connection: asyncpg.Connection = None,
    ) -> []:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_counts_for_orgs(
                c, organization_ids=organization_ids
            )

    # endregion

    # region mutate operations
    @_coerceable(bulk=True)
    @retry
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> [Verification]:
        if models:
            data = [d for d in self._iterdump(models)]
        if connection is not None:
            return await self.client.queries.bulk_persist(connection, records=data)
        else:
            async with self.client.connector.transaction(connection=connection) as c:
                return await self.client.queries.bulk_persist(c, records=data)

    @retry
    async def delete_all_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> None:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_all_for_org(
                c,
                organization_id=organization_id,
            )

    @retry
    async def batch_pre_verify_records_by_org(
        self,
        *,
        organization_id: int,
        batch_size: int,
        file_id: int | None = None,
        connection: asyncpg.Connection = None,
    ) -> int:
        """
        Batch pre-verify member records by ID, returns number of records pre-verified
        """
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.set_work_mem(c)

            if file_id:
                return (
                    await self.client.queries.batch_pre_verify_records_by_org_and_file(
                        c,
                        organization_id=organization_id,
                        file_id=file_id,
                        batch_size=batch_size,
                    )
                )

            return await self.client.queries.batch_pre_verify_records_by_org(
                c, organization_id=organization_id, batch_size=batch_size
            )

    @_coerceable(bulk=False)
    @retry
    async def deactivate_verification_record_for_user(
        self,
        *,
        verification_id: int,
        user_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Verification:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.deactivate_verification_record_for_user(
                c,
                verification_id=verification_id,
                user_id=user_id,
            )

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion

    # region for backfill
    async def get_e9y_data_for_member_track_backfill(
        self, user_id: int
    ) -> List[BackfillMemberTrackEligibilityData]:
        async with self.client.connector.connection() as c:
            records = await self.client.queries.get_e9y_data_for_member_track_backfill(
                c, user_id=user_id
            )
            return [BackfillMemberTrackEligibilityData(**record) for record in records]

    # endregion
