from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import enum
import pathlib
from datetime import datetime
from typing import Dict, List, Optional

import aiomysql
import aiosql
import typic
from aiomysql import DictCursor
from mmlib.config import apply_app_environment_namespace

from config import settings

from .adapter import AsyncMySQLAdapter
from .model import CreditBackfillRequest, MemberTrackBackfillRequest

QUERY_PATH = pathlib.Path(__file__).parent
# Event-loop-local state for connection
CONNECTOR: contextvars.ContextVar[Optional[MySQLConnector]] = contextvars.ContextVar(
    "mysql_connector", default=None
)


class MySQLConnector:
    """A simple connector for aiomysql."""

    __slots__ = ("pool", "__dict__")

    def __init__(self, *, pool: aiomysql.Pool = None):
        self.pool: aiomysql.Pool = pool

    def __repr__(self):
        open = self.open
        return f"<{self.__class__.__name__} {open=}>"

    async def initialize(self):
        if self.pool is None:
            self.pool = await create_pool()

    @contextlib.asynccontextmanager
    async def connection(self, *, c: aiomysql.Connection = None) -> aiomysql.Connection:
        if c:
            yield c
        else:
            if not self.open:
                await self.initialize()
            conn: aiomysql.Connection
            async with self.pool.acquire() as conn:
                try:
                    yield conn
                finally:
                    await conn.rollback()

    async def close(self, timeout: int = 10):
        if self.open:
            self.pool.close()
            await asyncio.wait_for(self.pool.wait_closed(), timeout=timeout)
            self.pool = None

    @property
    def open(self) -> bool:
        return self.pool and not self.pool._closed


def get_mono_db(db: str) -> str:
    if not db:
        return db

    return apply_app_environment_namespace(db)


async def create_pool(maxsize: int = 200):
    mono_db_settings = settings.MonoDB()
    return await aiomysql.create_pool(
        user=mono_db_settings.user,
        password=mono_db_settings.password,
        host=mono_db_settings.host,
        db=get_mono_db(mono_db_settings.db),
        cursorclass=DictCursor,
        maxsize=maxsize,
        autocommit=False,
    )


def cached_connector() -> MySQLConnector:
    if (connector := CONNECTOR.get()) is None:
        connector = MySQLConnector()
        CONNECTOR.set(connector)
    return connector


async def initialize(*, sighandlers: bool = False):
    connector = cached_connector()
    await connector.initialize()


async def teardown(*, signal: int = 0):
    if (connector := CONNECTOR.get()) is not None:
        await connector.close()
        CONNECTOR.set(None)


@typic.slotted(dict=False, weakref=True)
@dataclasses.dataclass
class MavenOrganization:
    id: int
    name: Optional[str]
    directory_name: str
    json: dict
    data_provider: int = 0
    eligibility_type: str | None = None
    email_domains: Optional[set[str]] = None
    activated_at: Optional[datetime] = None
    terminated_at: Optional[datetime] = None
    employee_only: int = 0
    medical_plan_only: int = 0


@typic.slotted(dict=False)
@dataclasses.dataclass
class MavenOrgExternalID:
    external_id: str
    organization_id: int
    source: str | None = None
    data_provider_organization_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicClientTrack:
    id: int
    track: str
    organization_id: int
    active: bool = True
    launch_date: Optional[datetime] = None
    length_in_days: int = 365
    ended_at: Optional[datetime] = None


class FertilityProgramType(str, enum.Enum):
    CARVE_OUT = "CARVE_OUT"
    WRAP_AROUND = "WRAP_AROUND"


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicReimbursementOrganizationSettings:
    id: int
    organization_id: int
    name: str
    benefit_faq_resource_id: int = 0
    survey_url: str = ""
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    debit_card_enabled: bool = False
    cycles_enabled: bool = False
    direct_payment_enabled: bool = False
    rx_direct_payment_enabled: bool = False
    deductible_accumulation_enabled: bool = False
    closed_network: bool = True
    fertility_program_type: str = FertilityProgramType.CARVE_OUT
    fertility_requires_diagnosis: bool = True
    fertility_allows_taxable: bool = False


@typic.slotted(dict=False)
@dataclasses.dataclass
class FeatureInformation:
    id: int
    descriptor: str


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicCredit:
    id: int
    user_id: int
    organization_employee_id: int | None
    eligibility_verification_id: int | None
    eligibility_member_id: int | None
    created_at: datetime | None


@typic.slotted(dict=False)
@dataclasses.dataclass
class OrganizationEmployee:
    id: int
    organization_id: int
    unique_corp_id: str | None = None
    email: str | None = None
    date_of_birth: datetime = datetime(2000, 1, 1)
    first_name: str | None = None
    last_name: str | None = None
    work_state: str | None = None
    modified_at: datetime | None = None
    deleted_at: datetime | None = None
    retention_start_date: datetime | None = None
    created_at: datetime | None = (None,)
    json: str = ""
    dependent_id: str = ""
    eligibility_member_id_deleted: int | None = None
    alegeus_id: str | None = None
    eligibility_member_id: int | None = None
    eligibility_member_2_id: int | None = None
    eligibility_member_2_version: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicMemberTrack:
    id: int
    client_track_id: int
    user_id: int
    organization_employee_id: int
    auto_transitioned: bool = False
    created_at: datetime | None = None
    name: str = "Mocked"
    bucket_id: str = "Mocked"
    start_date: datetime.date = datetime.today()
    activated_at: datetime = datetime.now()
    eligibility_verification_id: int | None = None
    eligibility_member_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicOrganizationEmployeeDependent:
    id: int
    organization_employee_id: int | None = None
    reimbursement_wallet_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class BasicReimbursementWallet:
    id: int
    reimbursement_organization_settings_id: int
    organization_employee_id: int | None = None


class MavenMonoClient:
    """
    This class acts as the DB client to Mono's MySQL database. It provides a
    wrapper to call the queries defined in fetch.sql.

    This class should be used by passing in an initialized MySQLConnector or
    by using the mono_db_context asynccontextmanager.
    """

    __slots__ = "connector", "queries"

    def __init__(self, *, c: MySQLConnector = None):
        self.connector = c or cached_connector()
        self.queries = aiosql.from_path(
            QUERY_PATH / "fetch.sql", driver_adapter=AsyncMySQLAdapter
        )

    async def get_org_from_directory(
        self, name: str, *, c: aiomysql.Connection = None
    ) -> MavenOrganization:
        proto = typic.protocol(MavenOrganization)
        async with self.connector.connection(c=c) as c:
            org = await self.queries.get_org_from_directory(c, directory_name=name)

        return org and proto.transmute(org)

    async def get_org_from_id(
        self, id: int, *, c: aiomysql.Connection = None
    ) -> MavenOrganization:
        proto = typic.protocol(MavenOrganization)
        async with self.connector.connection(c=c) as c:
            org = await self.queries.get_org_from_id(c, id=id)

        return org and proto.transmute(org)

    async def get_org_external_ids_for_org(
        self, org_id: int, *, c: aiomysql.Connection = None
    ) -> List[MavenOrgExternalID]:
        """
        Get all org external ID's for an organization based on ID

        Args:
            org_id: int
            c: aiomysql.Connection

        Returns: List[MavenOrgExternalID]

        """
        async with self.connector.connection(c=c) as c:
            # We are passing in the same value twice because the current implementation
            # doesn't allow the same variable to be used twice in the SQL query
            external_ids = await self.queries.get_org_external_ids_for_org(
                c, org_id=org_id, data_provider_org_id=org_id
            )
        return typic.transmute(List[MavenOrgExternalID], external_ids)

    async def get_all_records_with_optum_idp(
        self, optum_idp: str = "OPTUM", *, c: aiomysql.Connection = None
    ) -> List[Dict]:
        """
        Get all records that have Optum as their IDP.

        Args:
            optum_idp: The Optum IDP value
            c: Optional database connection

        Returns:
            List of dictionaries with external ID information
        """
        async with self.connector.connection(c=c) as c:
            records = await self.queries.get_all_records_with_optum_idp(
                c, optum_idp=optum_idp
            )
            return records

    async def update_org_provider(
        self, record_id: int, new_provider_org_id: int, *, c: aiomysql.Connection = None
    ) -> int:
        """
        Update organization external ID provider.

        Args:
            record_id: ID of the organization_external_id record to update
            new_provider_org_id: New provider organization ID
            c: Optional database connection

        Returns:
            Number of records updated
        """
        async with self.connector.connection(c=c) as c:
            # Update provider
            result = await self.queries.update_org_provider(
                c, record_id=record_id, new_provider_org_id=new_provider_org_id
            )
            await c.commit()
            return result.rowcount

    @contextlib.asynccontextmanager
    async def get_orgs_for_sync_cursor(self, c: aiomysql.Connection = None):
        async with self.connector.connection(c=c) as c:
            async with self.queries.get_orgs_for_sync_cursor(c) as cur:
                yield cur

    @contextlib.asynccontextmanager
    async def get_external_ids_for_sync_cursor(self, c: aiomysql.Connection = None):
        async with self.connector.connection(c=c) as c:
            async with self.queries.get_external_ids_for_sync_cursor(c) as cur:
                yield cur

    async def get_all_external_ids_for_sync(
        self, c: aiomysql.Connection = None
    ) -> List[MavenOrgExternalID]:
        async with self.connector.connection(c=c) as c:
            external_ids = await self.queries.get_external_ids_for_sync(c)
            return typic.transmute(List[MavenOrgExternalID], external_ids)

    async def get_non_ended_track_information_for_organization_id(
        self,
        organization_id: int,
        c: aiomysql.Connection = None,
    ) -> List[FeatureInformation]:
        """
        Gets FeatureInformation for the client tracks of an organization. This would
        include the IDs and descriptors of each track available to the organization.
        """
        async with self.connector.connection(c=c) as c:
            org_tracks_info = (
                await self.queries.get_non_ended_track_information_for_organization_id(
                    c,
                    organization_id=organization_id,
                )
            )
            return typic.transmute(List[FeatureInformation], org_tracks_info)

    async def get_non_ended_reimbursement_organization_settings_information_for_organization_id(
        self,
        organization_id: int,
        c: aiomysql.Connection = None,
    ) -> List[FeatureInformation]:
        """
        Gets FeatureInformation for the reimbursement organization settings of an
        organization. This would include the IDs and descriptors of each one
        available to the organization.
        """
        async with self.connector.connection(c=c) as c:
            org_reimbursement_setting_info = await self.queries.get_non_ended_reimbursement_organization_settings_information_for_organization_id(
                c,
                organization_id=organization_id,
            )
            return typic.transmute(
                List[FeatureInformation], org_reimbursement_setting_info
            )

    async def get_credit_back_fill_requests(
        self,
        batch_size: int,
        last_id: int,
        c: aiomysql.Connection = None,
    ) -> List[CreditBackfillRequest]:
        async with self.connector.connection(c=c) as c:
            credit_data = await self.queries.get_credit_back_fill_requests(
                c, batch_size=batch_size, last_id=last_id
            )
            return typic.transmute(List[CreditBackfillRequest], credit_data)

    async def backfill_credit_record(
        self,
        id: int,
        e9y_verification_id: int,
        e9y_member_id: int,
        c: aiomysql.Connection = None,
    ) -> None:
        async with self.connector.connection(c=c) as c:
            await self.queries.backfill_credit_record(
                c,
                id=id,
                e9y_verification_id=e9y_verification_id,
                e9y_member_id=e9y_member_id,
            )
            await c.commit()

    async def get_member_track_back_fill_requests(
        self,
        batch_size: int,
        last_id: int,
        c: aiomysql.Connection = None,
    ) -> List[MemberTrackBackfillRequest]:
        async with self.connector.connection(c=c) as c:
            data = await self.queries.get_member_track_back_fill_requests(
                c, batch_size=batch_size, last_id=last_id
            )
            return typic.transmute(List[MemberTrackBackfillRequest], data)

    async def backfill_member_track_record(
        self,
        id: int,
        e9y_verification_id: int,
        e9y_member_id: int,
        c: aiomysql.Connection = None,
    ) -> None:
        async with self.connector.connection(c=c) as c:
            await self.queries.backfill_member_track_record(
                c,
                id=id,
                e9y_verification_id=e9y_verification_id,
                e9y_member_id=e9y_member_id,
            )
            await c.commit()

    async def get_member_track_back_fill_requests_for_v2(
        self,
        organization_id: int,
        batch_size: int,
        last_id: int,
        c: aiomysql.Connection = None,
    ) -> List[MemberTrackBackfillRequest]:
        async with self.connector.connection(c=c) as c:
            data = await self.queries.get_member_track_back_fill_requests_for_v2(
                c,
                organization_id=organization_id,
                batch_size=batch_size,
                last_id=last_id,
            )
            return typic.transmute(List[MemberTrackBackfillRequest], data)

    async def get_member_track_back_fill_requests_for_billing(
        self,
        member_track_ids: List[int],
        c: aiomysql.Connection = None,
    ) -> List[MemberTrackBackfillRequest]:
        async with self.connector.connection(c=c) as c:
            data = await self.queries.get_member_track_back_fill_requests_for_billing(
                c,
                member_track_ids=member_track_ids,
            )
            return typic.transmute(List[MemberTrackBackfillRequest], data)

    async def get_oed_back_fill_requests(
        self,
        batch_size: int,
        last_id: int,
        c: aiomysql.Connection = None,
    ) -> List[int]:
        async with self.connector.connection(c=c) as c:
            oed_data = await self.queries.get_oed_back_fill_requests(
                c, batch_size=batch_size, last_id=last_id
            )
            return [record["id"] for record in oed_data]

    async def backfill_oed_record(
        self,
        id: int,
        reimbursement_wallet_id: int,
        c: aiomysql.Connection = None,
    ) -> None:
        async with self.connector.connection(c=c) as c:
            await self.queries.backfill_oed_record(
                c, id=id, reimbursement_wallet_id=reimbursement_wallet_id
            )
            await c.commit()

    async def get_rw_id_for_oed(
        self, id: int, c: aiomysql.Connection = None
    ) -> int | None:
        async with self.connector.connection(c=c) as c:
            rw_id = await self.queries.get_rw_id_for_oed(c, id=id)
            return rw_id and rw_id["reimbursement_wallet_id"]


@contextlib.asynccontextmanager
async def mono_db_context():
    """
    A context-manager for managing global state. This was modeled after how the sync job also
    provides its own context-manager.
    """
    await initialize()
    try:
        yield contextvars.copy_context()
    finally:
        await teardown()
