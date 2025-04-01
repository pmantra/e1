from __future__ import annotations

import contextlib
import datetime
from datetime import date
from typing import Iterable, Iterator, List, Mapping, Optional, Tuple

import aiosql
import asyncpg
import typic
from mmlib.ops import stats

import constants
from db.clients import verification_client
from db.clients.client import (
    BoundClient,
    CoercingCursor,
    ServiceProtocol,
    T,
    _coerceable,
)
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import (
    Address,
    ExternalRecordAndAddress,
    Member2,
    MemberAddressVersioned,
    MemberVersioned,
    OrgIdentity,
    WalletEnablement,
)

MemberIDtoRangeT = Tuple[int, asyncpg.Range]


class MembersVersioned(ServiceProtocol[MemberVersioned]):
    """A service for querying & mutating the `eligibility.member_versioned` table.

    Usage:
        >>> members_versioned = MembersVersioned()
        >>> member = MemberVersioned(organization_id=1, first_name="foo", last_name="bar", email="foo@bar.net")
        >>> member = await members_versioned.persist(model=member_versioned)
        >>> await members_versioned.get(member.id)
    """

    model = MemberVersioned

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("member_versioned", connector=connector)
        self.verification_client = verification_client.Verifications(
            connector=connector
        )

    # region fetch operations

    @retry
    async def get_values_to_hash_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_values_to_hash_for_org(
                c, organization_id=organization_id
            )

    @_coerceable(bulk=True)
    @retry
    async def get_all_historical(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.all_historical(c)

    @retry
    async def get_all_historical_addresses(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.all_historical_addresses(c)

    @_coerceable(bulk=True)
    @retry
    async def get_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_count_for_org(
        self, organization_id: int, *, connection: asyncpg.Connection = None
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_count_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_counts_for_orgs(
        self,
        *organization_ids: int,
        connection: asyncpg.Connection = None,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_counts_for_orgs(
                c, organization_ids=organization_ids
            )

    @retry
    async def get_count_for_sub_population_criteria(
        self, criteria: str, *, connection: asyncpg.Connection = None
    ) -> int:
        """
        Gets the count of members who fit the specified criteria. This is used to determine the
        sub-population sizes for their given criteria.
        """
        # Construct the SQL
        sql_str = (
            "-- name: get_count_for_sub_population_criteria$\n"
            "-- Get the current count of member records for a given set of criteria\n"
            f"SELECT count(1) FROM eligibility.member_versioned WHERE {criteria}"
        )
        # Create the query from the SQL
        queries = aiosql.from_str(sql_str, "asyncpg")

        async with self.client.connector.connection(c=connection) as c:
            return await queries.get_count_for_sub_population_criteria(c)

    @retry
    async def get_ids_for_sub_population_criteria(
        self, criteria: str, *, connection: asyncpg.Connection = None
    ) -> List[int]:
        """
        Gets the IDs of members who fit the specified criteria. This is used to allow
        auditing of sub-populations based on their given criteria.
        """
        # Construct the SQL
        sql_str = (
            "-- name: get_ids_for_sub_population_criteria\n"
            "-- Get the current count of member records for a given set of criteria\n"
            f"SELECT id FROM eligibility.member_versioned WHERE {criteria}"
        )
        # Create the query from the SQL
        queries = aiosql.from_str(sql_str, "asyncpg")

        async with self.client.connector.connection(c=connection) as c:
            return [
                record["id"]
                for record in await queries.get_ids_for_sub_population_criteria(c)
            ]

    @_coerceable(bulk=True)
    @retry
    async def get_for_file(
        self,
        file_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_file(c, file_id=file_id)

    @_coerceable(bulk=True)
    @retry
    async def get_for_files(
        self,
        *file_ids: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_for_files(c, file_ids=file_ids)

    @retry
    async def get_count_for_file(
        self, file_id: int, *, connection: asyncpg.Connection = None
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_count_for_file(c, file_id=file_id)

    @_coerceable(bulk=True)
    @retry
    async def get_by_dob_and_email(
        self,
        date_of_birth: date,
        email: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_dob_and_email(
                c,
                date_of_birth=date_of_birth,
                email=email,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_secondary_verification(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        work_state: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_secondary_verification(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_dob_name_and_work_state(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        work_state: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_dob_name_and_work_state(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_tertiary_verification(
        self,
        date_of_birth: date,
        unique_corp_id: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_tertiary_verification(
                c,
                date_of_birth=date_of_birth,
                unique_corp_id=unique_corp_id,
            )

    @_coerceable
    @retry
    async def get_by_any_verification(
        self,
        date_of_birth: date,
        first_name: str = None,
        last_name: str = None,
        work_state: str = None,
        email: str = None,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        assert email or all((first_name, last_name, work_state)), (
            "Either primary, secondary, or all values must be provided. "
            f"Got: {email=}, ({first_name=}, {last_name=}, {work_state=})"
        )

        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_any_verification(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
                email=email,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_overeligibility(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_overeligibility(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_all_by_name_and_date_of_birth(
        self,
        date_of_birth: date,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_all_by_name_and_date_of_birth(
                c,
                date_of_birth=date_of_birth,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable
    @retry
    async def get_by_name_and_unique_corp_id(
        self,
        unique_corp_id: str,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> MemberVersioned:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name_and_unique_corp_id(
                c,
                unique_corp_id=unique_corp_id,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable
    @retry
    async def get_by_date_of_birth_and_unique_corp_id(
        self,
        date_of_birth: date,
        unique_corp_id: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_date_of_birth_and_unique_corp_id(
                c,
                date_of_birth=date_of_birth,
                unique_corp_id=unique_corp_id,
            )

    @_coerceable
    @retry
    async def get_by_client_specific_verification(
        self,
        organization_id: int,
        unique_corp_id: str,
        date_of_birth: date,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_client_specific_verification(
                c,
                organization_id=organization_id,
                unique_corp_id=unique_corp_id,
                date_of_birth=date_of_birth,
            )

    @_coerceable
    @retry
    async def get_by_org_identity(
        self,
        identity: OrgIdentity,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_org_identity(
                c,
                organization_id=identity.organization_id,
                unique_corp_id=identity.unique_corp_id,
                dependent_id=identity.dependent_id,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_org_email(
        self,
        organization_id: int,
        email: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_org_email(
                c,
                organization_id=organization_id,
                email=email,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_difference_by_org_corp_id(
        self,
        organization_id: int,
        corp_ids: Iterable[str],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_difference_by_org_corp_id(
                c,
                organization_id=organization_id,
                corp_ids=corp_ids,
            )

    @_coerceable(bulk=True)
    @retry
    async def get_by_name_and_date_of_birth(
        self,
        first_name,
        last_name,
        date_of_birth: date,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name_and_date_of_birth(
                c,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )

    @contextlib.asynccontextmanager
    async def get_members_for_pre_verification_by_organization_cursor(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            async with self.client.queries.get_members_for_pre_verification_cursor(
                c, organization_id=organization_id
            ) as cursor:
                yield CoercingCursor(self, await cursor) if coerce else await cursor

    @retry
    async def get_wallet_enablement(
        self,
        *,
        member_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[WalletEnablement]:
        async with self.client.read_connector.connection(c=connection) as c:
            wallet = await self.client.queries.get_wallet_enablement(
                c, member_id=member_id
            )
            if wallet and coerce:
                return typic.transmute(WalletEnablement, wallet)
            return wallet

    @retry
    async def get_wallet_enablement_by_identity(
        self,
        *,
        identity: OrgIdentity,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[WalletEnablement]:
        async with self.client.read_connector.connection(c=connection) as c:
            wallet = await self.client.queries.get_wallet_enablement_by_identity(
                c,
                organization_id=identity.organization_id,
                unique_corp_id=identity.unique_corp_id,
                dependent_id=identity.dependent_id,
            )
            if wallet and coerce:
                return typic.transmute(WalletEnablement, wallet)
            return wallet

    @retry
    async def get_address_by_member_id(
        self,
        *,
        member_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[MemberAddressVersioned]:

        async with self.client.read_connector.connection(c=connection) as c:
            address = await self.client.queries.get_address_for_member(
                c, member_id=member_id
            )
            if address and coerce:
                return typic.transmute(MemberAddressVersioned, address)
            return address

    @retry
    async def get_kafka_record_count_for_org(
        self, *, organization_id: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_kafka_record_count_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_file_record_count_for_org(
        self, *, organization_id: int, connection: asyncpg.Connection = None
    ) -> int:
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_file_record_count_for_org(
                c, organization_id=organization_id
            )

    @retry
    async def get_members_for_unique_corp_id(
        self, *, unique_corp_id: str, connection: asyncpg.Connection = None
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_members_for_unique_corp_id(
                c, unique_corp_id=unique_corp_id
            )

    # endregion
    @retry
    async def get_unique_corp_id_for_member(
        self,
        *,
        member_id: int,
        connection: asyncpg.Connection = None,
    ) -> Optional[str]:
        async with self.client.read_connector.connection(c=connection) as c:
            result = await self.client.queries.get_unique_corp_id_for_member(
                c, member_id=member_id
            )
            if result:
                return result["unique_corp_id"]
            return result

    @retry
    async def get_other_user_ids_in_family(
        self, *, user_id: int, connection: asyncpg.Connection = None
    ) -> List[int]:
        """Given a user_id, return all user_id's for that family, as grouped by unique_corp_id and organization_id"""
        async with self.client.read_connector.connection(c=connection) as c:
            records: List[
                asyncpg.Record
            ] = await self.client.queries.get_other_user_ids_in_family(
                c, user_id=user_id
            )
            return [r["user_id"] for r in records]

    @_coerceable(bulk=True)
    @retry
    async def get_by_email_and_name(
        self,
        email: str,
        first_name: str,
        last_name: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> List[MemberVersioned]:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_email_and_name(
                c,
                email=email,
                first_name=first_name,
                last_name=last_name,
            )

    # region mutate operations
    @retry
    @_coerceable(bulk=True)
    async def bulk_persist(
        self,
        *,
        models: Iterable[T] = (),
        data: Iterable[Mapping] = (),
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        if models:
            data = [d for d in self._iterdump(models)]
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_persist(c, records=data)

    @retry
    async def delete_all_for_org(
        self,
        organization_id: int,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.delete_all_for_org(
                c,
                organization_id=organization_id,
            )

    @retry
    async def purge_expired_records(
        self,
        *,
        organization_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            result = await self.client.queries.purge_expired_records(
                c, organization_id=organization_id
            )
            if result:
                return result[0]["purged_count"]
            else:
                return 0

    @retry
    async def purge_duplicate_non_hash_optum(
        self,
        *,
        member_ids: int,
        connection: asyncpg.Connection = None,
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            purged_count = await self.client.queries.purge_duplicate_non_hash_optum(
                c, member_ids=member_ids
            )
        return purged_count[0]["count"]

    async def update_hash_values_for_optum(
        self,
        organization_id: int,
        records: list(str, str),
        *,
        connection: asyncpg.Connection = None,
    ) -> (int, int):
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.disable_timestamp_trigger(c)
            updated_records = await self.client.queries.update_optum_rows_with_hash(
                c, records=records, organization_id=organization_id
            )
            await self.client.queries.reenable_timestamp_trigger(c)

        updated_ids = [r["id"] for r in updated_records]

        async with self.client.connector.transaction(connection=connection) as c:
            # We should remove records that already had a hash set
            removed_duplicates = await self.client.queries.remove_optum_hash_duplicates(
                c, ids=[r[0] for r in records if r[0] not in updated_ids]
            )

            return len(updated_records), len(removed_duplicates)

    @retry
    async def set_address_for_member(
        self,
        *,
        address: Address,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[MemberAddressVersioned]:
        async with self.client.connector.transaction(connection=connection) as c:
            addresses = await self.client.queries.bulk_persist_member_address_versioned(
                c, addresses=[address]
            )
            if addresses and coerce:
                return typic.transmute(MemberAddressVersioned, addresses[0])
            return addresses[0] if addresses else None

    @retry
    async def set_effective_range(
        self,
        id: int,
        range: asyncpg.Range,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> asyncpg.Range:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_effective_range(
                c,
                id=id,
                range=range,
            )

    @retry
    async def set_do_not_contact(
        self,
        id: int,
        do_not_contact: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_do_not_contact(
                c, id=id, value=do_not_contact
            )

    @retry
    async def set_dependent_id_for_member(
        self,
        id: int,
        dependent_id: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_dependent_id_for_member(
                c, id=id, dependent_id=dependent_id
            )

    @retry
    async def set_updated_at(
        self,
        id: int,
        updated_at: datetime.datetime,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        """
        Do not use this outside of tests- this should be used to help us mock up records that represent 'updated' records
        Our test fixtures result in all records having the same created_at/updated_at date, which prevent us from sorting
        records by the date/time they were created.
        """
        async with self.client.connector.transaction(connection=connection) as c:
            await self.client.queries.disable_timestamp_trigger(c)
            updated = await self.client.queries.set_updated_at(
                c, id=id, updated_at=updated_at
            )
            await self.client.queries.reenable_timestamp_trigger(c)
        return updated

    @retry
    async def set_created_at(
        self,
        id: int,
        created_at: datetime.datetime,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        """
        Do not use this outside of tests- this should be used to help us mock up records that represent 'created' records
        Our test fixtures result in all records having the same created_at/updated_at date, which prevent us from sorting
        records by the date/time they were created.
        """
        async with self.client.connector.transaction(connection=connection) as c:
            created_at = await self.client.queries.set_created_at(
                c, id=id, created_at=created_at
            )
        return created_at

    @retry
    @_coerceable
    async def set_pre_verified(
        self,
        id: int,
        pre_verified: bool,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> MemberVersioned:
        """
        Set the pre_verified boolean column to True or False
        """
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.set_pre_verified(
                c, id=id, pre_verified=pre_verified
            )

    @retry
    async def bulk_set_effective_range(
        self,
        ranges: Iterable[MemberIDtoRangeT],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_set_effective_range(c, ranges=ranges)

    @retry
    async def bulk_set_do_not_contact(
        self,
        records: Iterable[Tuple[int, str]],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.bulk_set_do_not_contact(c, records=records)

    async def get_count_of_member_address(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> int:
        async with self.client.connector.transaction(connection=connection) as c:
            return await self.client.queries.get_count_of_member_address(c)

    @retry
    async def bulk_persist_external_records(
        self,
        external_records: List[ExternalRecordAndAddress],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        # Upsert our external records and their corresponding addresses
        async with self.client.connector.transaction(connection=connection) as c:
            # Assign a unique identifier for records within batch
            for i, record in enumerate(external_records):
                record["external_record"]["record"]["mvn_batch_record_id"] = i

            # Bulk insert our external records -
            saved_external_records = (
                await self.client.queries.bulk_persist_external_records(
                    c, records=[er["external_record"] for er in external_records]
                )
            )

            addresses_to_upsert = []

            for ser in saved_external_records:
                er = external_records[ser["record"]["mvn_batch_record_id"]]
                address = er.get("record_address")
                # If there is an address to upsert
                if address:
                    address["member_id"] = ser["id"]
                    addresses_to_upsert.append(address)

            saved_addresses = (
                await self.client.queries.bulk_persist_member_address_versioned(
                    c, addresses=addresses_to_upsert
                )
            )

            stats.increment(
                metric_name="eligibility.process.persist_member_versioned_records",
                pod_name=constants.POD,
                metric_value=len(saved_external_records),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            stats.increment(
                metric_name="eligibility.process.persist_member_versioned_addresses",
                pod_name=constants.POD,
                metric_value=len(saved_addresses),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            return saved_external_records, saved_addresses

    @retry
    async def bulk_persist_external_records_hash(
        self,
        external_records: List[ExternalRecordAndAddress],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        # Upsert our external records and their corresponding addresses
        async with self.client.connector.transaction(connection=connection) as c:

            # Ensure we don't have any duplicate records in our batch- that will cause issues with insert
            encountered_hashes = {}
            i = 0
            external_record_dedupe = []

            for record in external_records:
                # Don't attempt to de-dupe records without hashes in them
                if record["external_record"]["hash_value"] is not None:

                    # If we've seen this hash before, exclude the item
                    if encountered_hashes.get(record["external_record"]["hash_value"]):
                        continue
                    else:
                        encountered_hashes[record["external_record"]["hash_value"]] = 1

                record["external_record"]["record"]["mvn_batch_record_id"] = i
                external_record_dedupe.append(record)
                i += 1

            external_records = external_record_dedupe

            # Bulk insert our external records -
            saved_external_records = (
                await self.client.queries.bulk_persist_external_records(
                    c, records=[er["external_record"] for er in external_records]
                )
            )

            addresses_to_upsert = []

            for ser in saved_external_records:
                er = external_records[ser["record"]["mvn_batch_record_id"]]
                address = er.get("record_address")
                # If there is an address to upsert
                if address:
                    address["member_id"] = ser["id"]
                    addresses_to_upsert.append(address)

            saved_addresses = (
                await self.client.queries.bulk_persist_member_address_versioned(
                    c, addresses=addresses_to_upsert
                )
            )

            stats.increment(
                metric_name="eligibility.process.persist_member_versioned_records",
                pod_name=constants.POD,
                metric_value=len(saved_external_records),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            stats.increment(
                metric_name="eligibility.process.persist_member_versioned_addresses",
                pod_name=constants.POD,
                metric_value=len(saved_addresses),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            return saved_external_records, saved_addresses

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    @_coerceable
    @retry
    async def get_by_member_2(
        self,
        member_2: Member2,
        *,
        connection: asyncpg.Connection = None,
    ) -> MemberVersioned:
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_member_2(
                c,
                organization_id=member_2.organization_id,
                first_name=member_2.first_name,
                last_name=member_2.last_name,
                email=member_2.email,
                date_of_birth=member_2.date_of_birth,
                work_state=member_2.work_state,
                unique_corp_id=member_2.unique_corp_id,
            )

    # endregion
