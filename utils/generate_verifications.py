import asyncio
import contextlib
import contextvars

from mmlib.ops import log

from app.eligibility import service
from db import model
from db.clients import member_versioned_client, postgres_connector

logger = log.getLogger(__name__)


def main():
    organization_id = 881
    amount = 13_000
    asyncio.run(
        generate_mock_verifications(organization_id=organization_id, amount=amount)
    )


# region backfill credit


async def generate_mock_verifications(organization_id: int, amount: int):
    """
    generate mock verification for an organization
    Can be run in devshell

    from utils import generate_verifications
    await generate_verifications.generate_mock_verifications(organization_id=881, amount = 1)
    """
    logger.info(
        f"Beginning create mock verifications for organization_id={organization_id}, amount={amount}"
    )
    verification_type = "LOOKUP"

    e9y_service = service.service()
    async with utils_context():
        members_versioned = member_versioned_client.MembersVersioned()
        member_versioned_ids = set()
        async with members_versioned.client.connector.connection() as con:
            rows = await con.fetch(
                "select id, first_name, last_name, unique_corp_id, dependent_id, email, work_state  from eligibility.member_versioned mv where mv.organization_id=$1 and mv.effective_range @> CURRENT_DATE limit $2",
                organization_id,
                amount,
            )
            for r in rows:
                member_versioned_ids.add(
                    (
                        r["id"],
                        r["first_name"],
                        r["last_name"],
                        r["unique_corp_id"],
                        r["dependent_id"],
                        r["email"],
                        r["work_state"],
                    )
                )

            for (
                eligibility_member_id,
                first_name,
                last_name,
                unique_corp_id,
                dependent_id,
                email,
                work_state,
            ) in member_versioned_ids:
                found = await con.fetch(
                    "select id from eligibility.member_verification mvv where mvv.member_id = $1",
                    eligibility_member_id,
                )
                if found:
                    logger.info(
                        f"eligibility_member_id={eligibility_member_id} already has verification, skip it"
                    )
                else:
                    data = model.VerificationData(
                        verification_id=None,
                        verification_attempt_id=None,
                        eligibility_member_id=eligibility_member_id,
                        organization_id=organization_id,
                        unique_corp_id=unique_corp_id,
                        dependent_id=dependent_id,
                        email=email,
                        work_state=work_state,
                        additional_fields={},
                        member_1_id=None,
                        member_2_id=None,
                        member_2_version=None,
                    )

                    await e9y_service.create_multiple_verifications_for_user(
                        verification_data_list=[data],
                        verification_type=verification_type,
                        first_name=first_name,
                        last_name=last_name,
                        user_id=-1 * eligibility_member_id,
                    )


@contextlib.asynccontextmanager
async def utils_context():
    """A context-manager for managing global state."""

    pg_connections = postgres_connector.cached_connectors()
    pg_main_connection = pg_connections["main"]

    await pg_main_connection.initialize()
    try:
        yield contextvars.copy_context()
    finally:
        await pg_main_connection.close()
