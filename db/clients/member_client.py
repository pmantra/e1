from __future__ import annotations

from datetime import date, datetime
from timeit import default_timer
from typing import Iterable, Iterator, Mapping, Optional, Tuple

import asyncpg
import structlog
import typic
from mmlib.ops import stats

import constants
from db.clients.client import BoundClient, ServiceProtocol, T, _coerceable
from db.clients.postgres_connector import PostgresConnector, retry
from db.model import (
    Address,
    ExternalRecordAndAddress,
    Member,
    MemberAddress,
    OrgIdentity,
    WalletEnablement,
)

MemberIDtoRangeT = Tuple[int, asyncpg.Range]

logger = structlog.getLogger(__name__)


class Members(ServiceProtocol[Member]):
    """A service for querying & mutating the `eligibility.member` table.

    Usage:
        >>> members = Members()
        >>> member = Member(organization_id=1, first_name="foo", last_name="bar", email="foo@bar.net")
        >>> member = await members.persist(model=member)
        >>> await members.get(member.id)
    """

    model = Member

    def __init__(self, *, connector: PostgresConnector = None):
        super().__init__()
        self.client = BoundClient("member", connector=connector)

    # region fetch operations

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

    @_coerceable
    @retry
    async def get_by_dob_and_email(
        self,
        date_of_birth: date,
        email: str,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        async with self.client.connector.connection(c=connection) as c:
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
    ):
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_secondary_verification(
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
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_tertiary_verification(
                c,
                date_of_birth=date_of_birth,
                unique_corp_id=unique_corp_id,
            )

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
    ):
        async with self.client.read_connector.connection(c=connection) as c:
            return await self.client.queries.get_by_email_and_name(
                c,
                email=email,
                first_name=first_name,
                last_name=last_name,
            )

    @_coerceable
    @retry
    async def get_by_any_verification(
        self,
        dob: date,
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

        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_any_verification(
                c,
                date_of_birth=dob,
                first_name=first_name,
                last_name=last_name,
                work_state=work_state,
                email=email,
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
        async with self.client.connector.connection(c=connection) as c:
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
        async with self.client.connector.connection(c=connection) as c:
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
        async with self.client.connector.connection(c=connection) as c:
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
        async with self.client.connector.connection(c=connection) as c:
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
        async with self.client.connector.connection(c=connection) as c:
            return await self.client.queries.get_by_name_and_date_of_birth(
                c,
                first_name=first_name,
                last_name=last_name,
                date_of_birth=date_of_birth,
            )

    @retry
    async def get_wallet_enablement(
        self,
        *,
        member_id: int,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[WalletEnablement]:
        async with self.client.connector.connection(c=connection) as c:
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
        async with self.client.connector.connection(c=connection) as c:
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
    ) -> Optional[MemberAddress]:

        async with self.client.connector.connection(c=connection) as c:
            address = await self.client.queries.get_address_for_member(
                c, member_id=member_id
            )
            if address and coerce:
                return typic.transmute(MemberAddress, address)
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

    # endregion

    # region mutate operations
    @retry
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
    async def set_address_for_member(
        self,
        *,
        address: Address,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ) -> Optional[MemberAddress]:
        async with self.client.connector.transaction(connection=connection) as c:
            addresses = await self.client.queries.bulk_persist_member_address(
                c, addresses=[address]
            )
            if addresses and coerce:
                return typic.transmute(MemberAddress, addresses[0])
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

    @retry
    async def get_id_range_for_member(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> Tuple[int, int]:
        """Return the min and max id's for the member table as a tuple"""
        async with self.client.connector.transaction(connection=connection) as c:
            id_record: asyncpg.Record = (
                await self.client.queries.get_id_range_for_member(c)
            )
            return id_record["min_id"], id_record["max_id"]

    @retry
    async def get_id_range_for_member_address(
        self,
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ) -> Tuple[int, int]:
        """Return the min and max id's for the member_address table as a tuple"""
        async with self.client.connector.transaction(connection=connection) as c:
            id_record: asyncpg.Record = (
                await self.client.queries.get_id_range_for_member_address(c)
            )
            return id_record["min_id"], id_record["max_id"]

    @retry
    async def migrate_member(
        self,
        *,
        batch_size: int = 1_000,
        stmt_per_commit: int = 100,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        """
        Migrate member to member_versioned in batches of batch_size
        commit every stmt_per_commit inserts
        """
        async with self.client.connector.transaction(connection=connection) as c:
            start_time = default_timer()
            min_id, max_id = await self.get_id_range_for_member(connection=c)
            logger.info(f"member min_id={min_id} max_id={max_id}")

            curr_min_id = min_id
            curr_max_id = min_id + batch_size

            while curr_min_id <= max_id:
                stmt_count = 0
                # Open a nested transaction
                async with c.transaction():
                    # Commit after every stmt_per_commit statements of batch_size
                    while stmt_count < stmt_per_commit and curr_min_id <= max_id:
                        logger.info(f"Executing {curr_min_id} -> {curr_max_id}")

                        await self.client.queries.migrate_member_for_range(
                            c, min_id=curr_min_id, max_id=curr_max_id
                        )
                        curr_min_id = curr_min_id + batch_size
                        curr_max_id = curr_max_id + batch_size
                        stmt_count += 1

                logger.info(f"committing after statement: {stmt_count}")
            end_time = default_timer()

            logger.info(f"Total execution time: {end_time - start_time} seconds")

    @retry
    async def migrate_member_address(
        self,
        *,
        batch_size: int = 1_000,
        stmt_per_commit: int = 100,
        connection: asyncpg.Connection = None,
        coerce: bool = False,
    ):
        """
        Migrate member_address to member_address_versioned in batches of batch_size
        commit every stmt_per_commit inserts
        """
        async with self.client.connector.transaction(connection=connection) as c:
            start_time = default_timer()
            min_id, max_id = await self.get_id_range_for_member_address(connection=c)
            logger.info(f"member_address min_id={min_id} max_id={max_id}")

            curr_min_id = min_id
            curr_max_id = min_id + batch_size

            while curr_min_id <= max_id:
                stmt_count = 0
                # Open a nested transaction
                async with c.transaction():
                    # Commit after every stmt_per_commit statements of batch_size
                    while stmt_count < stmt_per_commit and curr_min_id <= max_id:
                        logger.info(f"Executing {curr_min_id} -> {curr_max_id}")

                        await self.client.queries.migrate_member_address_for_range(
                            c, min_id=curr_min_id, max_id=curr_max_id
                        )
                        curr_min_id = curr_min_id + batch_size
                        curr_max_id = curr_max_id + batch_size
                        stmt_count += 1

                logger.info(f"committing after statement: {stmt_count}")
            end_time = default_timer()

            logger.info(f"Total execution time: {end_time - start_time} seconds")

    @retry
    async def tmp_bulk_persist_external_records(
        self,
        external_records: Iterable[ExternalRecordAndAddress],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        # Upsert our external records and their corresponding addresses
        async with self.client.connector.transaction(connection=connection) as c:
            # Bulk insert our external records -
            #   logic below would be simpler with individual inserts, but *much* worse in performance
            saved_external_records = (
                await self.client.queries.tmp_bulk_persist_external_records(
                    c, records=[er["external_record"] for er in external_records]
                )
            )

            # To avoid the painful sql required to bulk save both external records and addresses together,
            #   we save our external records and their addresses separately.
            # In order to do this though, we need to find a way to map the resultant externalRecord IDs in the DB
            #   to their corresponding address record
            unique_identifier_to_external_record_id = {
                (r["record"]["external_id"], r["unique_corp_id"], r["dependent_id"]): r[
                    "id"
                ]
                for r in saved_external_records
            }

            addresses_to_upsert = []
            addresses_to_delete = []
            for er in external_records:
                unique_identifier = (
                    er["external_record"]["external_id"],
                    er["external_record"]["unique_corp_id"],
                    er["external_record"]["dependent_id"],
                )

                # We want to make sure we are looking at the most up-to-date record-
                #   we could have an edge case where we updated two of the same member records in one batch
                # If that was the case, only the most up-to-date record would be in 'saved_records'
                #   i.e. in the map of unique_identifier_to_external_record_id
                #   use this most up-to-date record for the address value
                if unique_identifier in unique_identifier_to_external_record_id.keys():
                    member_id = unique_identifier_to_external_record_id[
                        unique_identifier
                    ]

                    address = er.get("record_address")
                    # If there is an address to upsert
                    if address:
                        address["member_id"] = member_id
                        addresses_to_upsert.append(address)
                    # There either isn't an address to update (i.e. member record provided without an address),
                    #   or we need to delete the existing address for a member
                    else:
                        addresses_to_delete.append(member_id)

            saved_addresses = await self.client.queries.tmp_bulk_persist_member_address(
                c, addresses=addresses_to_upsert
            )

            # Delete any addresses no longer needed
            await self.client.queries.tmp_bulk_delete_member_address_by_member_id(
                c, member_ids=addresses_to_delete
            )

            return saved_external_records, saved_addresses

    @retry
    async def bulk_persist_external_records(
        self,
        external_records: Iterable[ExternalRecordAndAddress],
        *,
        connection: asyncpg.Connection = None,
        coerce: bool = True,
    ):
        # Upsert our external records and their corresponding addresses
        async with self.client.connector.transaction(connection=connection) as c:
            # Bulk insert our external records -
            #   logic below would be simpler with individual inserts, but *much* worse in performance
            saved_external_records = (
                await self.client.queries.bulk_persist_external_records(
                    c, records=[er["external_record"] for er in external_records]
                )
            )

            # To avoid the painful sql required to bulk save both external records and addresses together,
            #   we save our external records and their addresses separately.
            # In order to do this though, we need to find a way to map the resultant externalRecord IDs in the DB
            #   to their corresponding address record
            unique_identifier_to_external_record_id = {
                (r["record"]["external_id"], r["unique_corp_id"], r["dependent_id"]): r[
                    "id"
                ]
                for r in saved_external_records
            }

            addresses_to_upsert = []
            addresses_to_delete = []
            for er in external_records:
                unique_identifier = (
                    er["external_record"]["external_id"],
                    er["external_record"]["unique_corp_id"],
                    er["external_record"]["dependent_id"],
                )

                # We want to make sure we are looking at the most up-to-date record-
                #   we could have an edge case where we updated two of the same member records in one batch
                # If that was the case, only the most up-to-date record would be in 'saved_records'
                #   i.e. in the map of unique_identifier_to_external_record_id
                #   use this most up-to-date record for the address value
                if unique_identifier in unique_identifier_to_external_record_id.keys():
                    member_id = unique_identifier_to_external_record_id[
                        unique_identifier
                    ]

                    address = er.get("record_address")
                    # If there is an address to upsert
                    if address:
                        address["member_id"] = member_id
                        addresses_to_upsert.append(address)
                    # There either isn't an address to update (i.e. member record provided without an address),
                    #   or we need to delete the existing address for a member
                    else:
                        addresses_to_delete.append(member_id)

            saved_addresses = await self.client.queries.bulk_persist_member_address(
                c, addresses=addresses_to_upsert
            )

            # Delete any addresses no longer needed
            await self.client.queries.bulk_delete_member_address_by_member_id(
                c, member_ids=addresses_to_delete
            )

            stats.increment(
                metric_name="eligibility.process.persist_member_records",
                pod_name=constants.POD,
                metric_value=len(saved_external_records),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            stats.increment(
                metric_name="eligibility.process.persist_member_addresses",
                pod_name=constants.POD,
                metric_value=len(saved_addresses),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            stats.increment(
                metric_name="eligibility.process.delete_member_address",
                pod_name=constants.POD,
                metric_value=len(addresses_to_delete),
                tags=["eligibility:info", "db:psql", "source:external_record"],
            )
            return saved_external_records, saved_addresses

    @retry
    async def set_created_at(
        self,
        id: int,
        created_at: datetime,
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

    def _iterdump(self, models: Iterable[T]) -> Iterator[T]:
        kvs = self._get_kvs
        yield from (kvs(m) for m in models)

    # endregion
