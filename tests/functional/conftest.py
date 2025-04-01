import asyncio
import contextlib
import json
import os
import pathlib
import random
from datetime import datetime, time, timedelta, timezone
from typing import List, Tuple
from unittest import mock

import aiomysql
import aioredis
import asyncpg
import pytest
from maven import feature_flags
from mmstream import redis
from tests.factories import data_models
from tests.factories import data_models as factory

import db.model
from app.eligibility import constants as e9y_constants
from app.eligibility import translate
from app.eligibility.constants import ORGANIZATIONS_NOT_SENDING_DOB
from app.eligibility.populations import model as pop_model
from app.mono import repository as mono_repository
from app.worker import pubsub
from db import model as db_model
from db import redis as redisc
from db.clients import (
    configuration_client,
    file_client,
    file_parse_results_client,
    header_aliases_client,
    member_2_client,
    member_client,
    member_sub_population_client,
    member_verification_client,
    member_versioned_client,
    population_client,
    postgres_connector,
    sub_population_client,
    verification_2_client,
    verification_attempt_client,
    verification_client,
)
from db.mono import client as mono_client

THIS_DIR = pathlib.Path(__file__).resolve().parent
TESTS_DIR = THIS_DIR.parent
DB_DIR = TESTS_DIR.parent / "db"

NUMBER_TEST_OBJECTS = 10


@pytest.fixture
def reset_external_id_cache():
    pubsub.get_cached_external_org_infos_by_value.reset()


async def _reset_maven(maven: mono_client.MavenMonoClient):
    schema = (DB_DIR / "mono" / "dump.sql").read_text()
    async with maven.connector.connection() as c:
        try:
            await maven.queries.driver_adapter.execute_script(
                c, "DROP SCHEMA IF EXISTS `maven`;"
            )
            await maven.queries.driver_adapter.execute_script(c, schema)
        except:  # noqa: E722
            c.rollback()
            raise
        else:
            await c.commit()


async def _reset_redis(store: redisc.RedisKeyStore):
    c: aioredis.Redis
    async with store.redis.connection() as c:
        await c.flushall()


async def _reset_e9y(e9y: postgres_connector.PostgresConnector):
    tables = (
        "file_parse_errors",
        "file_parse_results",
        "organization_external_id",
        "member_versioned",
        "member",
        "verification",
        "member_verification",
        "verification_attempt",
        "file",
        "configuration",
        "header_alias",
    )
    c: asyncpg.Connection
    async with e9y.transaction() as c:
        for table in tables:
            await c.execute(f"DELETE FROM {table} CASCADE;")


async def _create_mono_connector():
    pool: aiomysql.Pool = await mono_client.create_pool(maxsize=1)
    return mono_client.MySQLConnector(pool=pool)


@pytest.fixture
async def keystore() -> redisc.RedisKeyStore:
    store = redisc.RedisKeyStore()
    await store.redis.initialize()
    yield store
    await _reset_redis(store=store)
    await store.redis.teardown()


@pytest.fixture(scope="package", autouse=True)
async def init_storage(event_loop, e9y_session):
    os.environ["MONO_DB_DB"] = ""

    mono_connection = mono_client.MySQLConnector()
    redis_connection = redisc.RedisKeyStore()
    await asyncio.gather(
        mono_connection.initialize(),
        redisc.initialize(),
        loop=event_loop,
    )
    maven = mono_client.MavenMonoClient(c=mono_connection)
    await asyncio.gather(
        _reset_maven(maven),
        _reset_redis(redis_connection),
        _reset_e9y(e9y_session),
        loop=event_loop,
    )
    os.environ["MONO_DB_DB"] = "maven"


# region Mono DB client


@pytest.fixture(scope="package")
async def maven() -> mono_client.MavenMonoClient:
    connector = await _create_mono_connector()
    await connector.initialize()
    mono_client.CONNECTOR.set(connector)
    connection: aiomysql.Connection
    async with connector.pool.acquire() as connection:

        @contextlib.asynccontextmanager
        async def connect(*, c=None):
            yield connection

        connector.connection = connect
        yield mono_client.MavenMonoClient(c=connector)
        await connection.rollback()
    mono_client.CONNECTOR.set(None)


@pytest.fixture
async def maven_connection(maven) -> aiomysql.Connection:
    async with maven.connector.connection() as c:
        await c.begin()
        yield c
        await c.rollback()


@pytest.fixture(scope="package")
async def maven_repo(maven) -> mono_repository.MavenMonoRepository:
    yield mono_repository.MavenMonoRepository(
        context={"mysql_connector": maven.connector}
    )


@pytest.fixture
async def maven_org(
    maven_connection,
) -> Tuple[mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]]:
    return await create_maven_org(maven_connection)


@pytest.fixture
async def maven_org_with_features(
    maven_connection,
) -> Tuple[
    mono_client.MavenOrganization,
    List[mono_client.MavenOrgExternalID],
    List[mono_client.BasicReimbursementOrganizationSettings],
]:
    return await create_org_with_features(maven_connection)


@pytest.fixture
async def maven_org_with_activated_terminated_at(
    maven_connection: aiomysql.Connection,
) -> mono_client.MavenOrganization:
    org: mono_client.MavenOrganization = data_models.MavenOrganizationFactory.create(
        activated_at=datetime.combine(
            datetime.strptime("04-01-2020", "%m-%d-%Y"), time()
        ),
        terminated_at=datetime.combine(
            datetime.strptime("04-01-2023", "%m-%d-%Y"), time()
        ),
    )
    await persist_maven_orgs(orgs=[org], conn=maven_connection)
    return org


@pytest.fixture
async def multiple_maven_org_with_external_ids(
    maven_connection: aiomysql.Connection,
) -> Tuple[List[mono_client.MavenOrganization], List[mono_client.MavenOrgExternalID]]:
    orgs: List[
        mono_client.MavenOrganization
    ] = data_models.MavenOrganizationFactory.create_batch(
        size=10,
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        },
    )

    # factory.Faker("domain_word") in MavenOrganizationFactory may generate duplicate directory_name
    # the following code remove those org with duplicate directory_names, put all good orgs to valid_orgs
    # store seen directory_name
    seen_directories = set()
    # store org id which has duplicate directory with previous org
    invalid_org_ids = set()
    for org in orgs:
        if org.directory_name in seen_directories:
            invalid_org_ids.add(org.id)
        else:
            seen_directories.add(org.directory_name)
    valid_orgs = [org for org in orgs if org.id not in invalid_org_ids]

    org_external_ids: List[mono_client.MavenOrgExternalID] = []
    for org in valid_orgs:
        org_external_ids.extend(
            data_models.MavenOrgExternalIDFactory.create_batch(
                size=10, organization_id=org.id, data_provider_organization_id=None
            )
        )

    await persist_maven_orgs(orgs=valid_orgs, conn=maven_connection)
    await persist_external_ids(org_external_ids=org_external_ids, conn=maven_connection)
    return valid_orgs, org_external_ids


@pytest.fixture
async def duplicate_directory_maven_org_with_external_ids(
    maven_connection: aiomysql.Connection,
) -> Tuple[List[mono_client.MavenOrganization], List[mono_client.MavenOrgExternalID]]:
    orgs: List[
        mono_client.MavenOrganization
    ] = data_models.MavenOrganizationFactory.create_batch(
        size=2,
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        },
        directory_name="same-directory",
    )
    org_external_ids: List[mono_client.MavenOrgExternalID] = []
    for org in orgs:
        org_external_ids.extend(
            data_models.MavenOrgExternalIDFactory.create_batch(
                size=3, organization_id=org.id, data_provider_organization_id=None
            )
        )

    await persist_maven_orgs(orgs=orgs, conn=maven_connection)
    await persist_external_ids(org_external_ids=org_external_ids, conn=maven_connection)
    return orgs, org_external_ids


@pytest.fixture
async def maven_org_with_empty_external_id(
    maven_connection: aiomysql.Connection,
) -> Tuple[List[mono_client.MavenOrganization], List[mono_client.MavenOrgExternalID]]:
    orgs: List[
        mono_client.MavenOrganization
    ] = data_models.MavenOrganizationFactory.create_batch(
        size=2,
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        },
        directory_name="same-directory",
    )
    org_external_ids: List[mono_client.MavenOrgExternalID] = []
    for org in orgs:
        org_external_ids.extend(
            data_models.MavenOrgExternalIDFactory.create_batch(
                size=3, organization_id=org.id, data_provider_organization_id=None
            )
        )
    org_external_ids.append(
        data_models.MavenOrgExternalIDFactory.create(
            organization_id=orgs[0].id,
            data_provider_organization_id=None,
            external_id="   ",
        )
    )

    await persist_maven_orgs(orgs=orgs, conn=maven_connection)
    await persist_external_ids(org_external_ids=org_external_ids, conn=maven_connection)
    return orgs, org_external_ids


@pytest.fixture
async def maven_org_with_external_ids(
    maven_connection: aiomysql.Connection,
) -> Tuple[mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]]:
    org: mono_client.MavenOrganization = data_models.MavenOrganizationFactory.create(
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        }
    )
    org_external_ids: List[
        mono_client.MavenOrgExternalID
    ] = data_models.MavenOrgExternalIDFactory.create_batch(
        size=10, organization_id=org.id
    )
    await persist_maven_orgs(orgs=[org], conn=maven_connection)
    await persist_external_ids(org_external_ids=org_external_ids, conn=maven_connection)
    return org, org_external_ids


@pytest.fixture
async def maven_data_provider_org_with_external_ids(
    maven_connection: aiomysql.Connection,
) -> Tuple[
    mono_client.MavenOrganization,
    List[Tuple[mono_client.MavenOrganization, mono_client.MavenOrgExternalID]],
]:
    n_sub_orgs = 10
    data_provider_org: mono_client.MavenOrganization = (
        data_models.MavenOrganizationFactory.create(
            json={
                "PK_COLUMN": "unique_corp_id",
                translate.FIELD_MAP_KEY: {
                    "unique_corp_id": "employee_id",
                    "gender": "sex",
                },
            },
            data_provider=True,
        )
    )
    sub_orgs: List[
        mono_client.MavenOrganization
    ] = data_models.MavenOrganizationFactory.create_batch(
        size=n_sub_orgs,
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        },
        data_provider=False,
    )
    sub_org_eids: List[mono_client.MavenOrgExternalID] = [
        data_models.MavenOrgExternalIDFactory.create(
            organization_id=sub_org.id,
            data_provider_organization_id=data_provider_org.id,
        )
        for sub_org in sub_orgs
    ]
    await persist_maven_orgs(orgs=[data_provider_org] + sub_orgs, conn=maven_connection)
    await persist_external_ids(org_external_ids=sub_org_eids, conn=maven_connection)
    return data_provider_org, [
        (sub_orgs[i], sub_org_eids[i]) for i in range(n_sub_orgs)
    ]


# endregion Mono DB client
# region clients and DB access


@pytest.fixture(scope="package")
async def e9y_session():
    connectors = postgres_connector.cached_connectors(
        min_size=1,
        max_size=1,
    )
    connector = connectors.get("main")
    await connector.initialize()
    connection = await connector.pool.acquire()
    transaction = connection.transaction()
    await transaction.start()

    @contextlib.asynccontextmanager
    async def connect(*, c=None):
        yield connection

    connector.connection = connect
    yield connector
    await transaction.rollback()
    await connector.pool.release(connection)


@pytest.fixture
async def e9y_connector(e9y_session):
    async with e9y_session.connection() as connection:
        transaction = connection.transaction()
        await transaction.start()
        yield e9y_session
        await transaction.rollback()


@pytest.fixture
def member_test_client(e9y_connector) -> member_client.Members:
    return member_client.Members(connector=e9y_connector)


@pytest.fixture
def member_versioned_test_client(
    e9y_connector,
) -> member_versioned_client.MembersVersioned:
    return member_versioned_client.MembersVersioned(connector=e9y_connector)


@pytest.fixture
def file_test_client(e9y_connector) -> file_client.Files:
    return file_client.Files(connector=e9y_connector)


@pytest.fixture
def configuration_test_client(e9y_connector) -> configuration_client.Configurations:
    return configuration_client.Configurations(connector=e9y_connector)


@pytest.fixture
def header_test_client(e9y_connector) -> header_aliases_client.HeaderAliases:
    return header_aliases_client.HeaderAliases(connector=e9y_connector)


@pytest.fixture
def file_parse_results_test_client(
    e9y_connector,
) -> file_parse_results_client.FileParseResults:
    return file_parse_results_client.FileParseResults(connector=e9y_connector)


@pytest.fixture
def verification_test_client(
    e9y_connector,
) -> verification_client.Verifications:
    return verification_client.Verifications(connector=e9y_connector)


@pytest.fixture
def verification_attempt_test_client(
    e9y_connector,
) -> verification_attempt_client.VerificationAttempts:
    return verification_attempt_client.VerificationAttempts(connector=e9y_connector)


@pytest.fixture
def member_verification_test_client(
    e9y_connector,
) -> member_verification_client.MemberVerifications:
    return member_verification_client.MemberVerifications(connector=e9y_connector)


@pytest.fixture
def population_test_client(
    e9y_connector,
) -> population_client.Populations:
    return population_client.Populations(connector=e9y_connector)


@pytest.fixture
def sub_population_test_client(
    e9y_connector,
) -> sub_population_client.SubPopulations:
    return sub_population_client.SubPopulations(connector=e9y_connector)


@pytest.fixture
def member_sub_population_test_client(
    e9y_connector,
) -> member_sub_population_client.MemberSubPopulations:
    return member_sub_population_client.MemberSubPopulations(connector=e9y_connector)


@pytest.fixture
def member_2_test_client(e9y_connector) -> member_2_client.Member2Client:
    return member_2_client.Member2Client(connector=e9y_connector)


@pytest.fixture
def verification_2_test_client(
    e9y_connector,
) -> verification_2_client.Verification2Client:
    return verification_2_client.Verification2Client(connector=e9y_connector)


# endregion clients


@pytest.fixture
async def test_config(configuration_test_client) -> configuration_client.Configuration:
    return await configuration_test_client.persist(
        model=data_models.ConfigurationFactory.create()
    )


@pytest.fixture
async def test_config_for_orgs_that_dont_send_dob(
    configuration_test_client,
) -> configuration_client.Configuration:
    config = data_models.ConfigurationFactory.create()
    config.organization_id = next(iter(ORGANIZATIONS_NOT_SENDING_DOB))
    return await configuration_test_client.persist(model=config)


@pytest.fixture
async def test_inactive_config(
    configuration_test_client,
) -> configuration_client.Configuration:
    inactive_config = data_models.ConfigurationFactory.create()
    inactive_config.activated_at = None
    return await configuration_test_client.persist(model=inactive_config)


@pytest.fixture
def sample_file_row() -> dict:
    return {
        "unique_corp_id": "1",
        "email": "email@blah.net",
        "date_of_birth": "1990-01-1",
        "first_name": "first_name",
        "last_name": "last_name",
    }


@pytest.fixture()
async def multiple_test_config(
    configuration_test_client,
) -> List[configuration_client.Configuration]:
    await configuration_test_client.bulk_persist(
        models=[
            data_models.ConfigurationFactory.create()
            for _ in range(NUMBER_TEST_OBJECTS)
        ]
    )
    # Bulk persist does not return the values we created like `persist` does, so we must return them manually
    return await configuration_test_client.all()


@pytest.fixture()
async def multiple_test_config_with_data_provider(
    configuration_test_client,
) -> configuration_client.Configuration:
    test_config = await configuration_test_client.persist(
        model=data_models.ConfigurationFactory.create(data_provider=False)
    )
    data_provider_config = await configuration_test_client.persist(
        model=data_models.ConfigurationFactory.create(data_provider=True)
    )
    return {
        "organization": test_config,
        "data_provider_organization": data_provider_config,
    }


@pytest.fixture()
async def test_organization_external_id(configuration_test_client, test_config):
    model = data_models.ExternalIDFactory.create()
    return await configuration_test_client.add_external_id(
        organization_id=test_config.organization_id,
        source=model["source"],
        external_id=model["external_id"],
    )


@pytest.fixture()
async def test_multiple_organization_external_id(
    configuration_test_client, test_config
):
    external_ids = []
    for i in range(2):
        model = data_models.ExternalIDFactory.create()
        external_ids.append(
            await configuration_test_client.add_external_id(
                organization_id=test_config.organization_id,
                source=model["source"],
                external_id=model["external_id"],
            )
        )
    return external_ids


@pytest.fixture()
async def test_organization_external_id_with_data_provider(
    configuration_test_client, multiple_test_config_with_data_provider
):
    model = data_models.ExternalIDFactory.create()
    return await configuration_test_client.add_external_id(
        organization_id=multiple_test_config_with_data_provider[
            "organization"
        ].organization_id,
        data_provider_organization_id=multiple_test_config_with_data_provider[
            "data_provider_organization"
        ].organization_id,
        external_id=model["external_id"],
    )


@pytest.fixture()
async def test_multiple_organization_external_id_with_data_provider(
    configuration_test_client, multiple_test_config_with_data_provider
):
    external_ids = []
    for i in range(2):
        model = data_models.ExternalIDFactory.create()
        external_ids.append(
            await configuration_test_client.add_external_id(
                organization_id=multiple_test_config_with_data_provider[
                    "organization"
                ].organization_id,
                data_provider_organization_id=multiple_test_config_with_data_provider[
                    "data_provider_organization"
                ].organization_id,
                external_id=model["external_id"],
            )
        )
    return external_ids


@pytest.fixture
async def test_file(file_test_client, test_config) -> file_client.File:
    return await file_test_client.persist(
        model=data_models.FileFactory.create(
            organization_id=test_config.organization_id, name="primary/clean.csv"
        )
    )


@pytest.fixture()
async def multiple_test_file(
    file_test_client, multiple_test_config
) -> file_client.File:
    # Because we must have an organization to match our files to, use the orgIDs we know exist
    org_ids = [config.organization_id for config in multiple_test_config]
    for org_id in org_ids:
        await file_test_client.bulk_persist(
            models=[
                data_models.FileFactory.create(organization_id=org_id)
                for _ in range(NUMBER_TEST_OBJECTS)
            ]
        )
    return await file_test_client.all()


@pytest.fixture
async def test_header_alias(
    header_test_client, test_config
) -> header_aliases_client.HeaderAliases:
    return await header_test_client.persist(
        model=data_models.HeaderAliasFactory.create(
            organization_id=test_config.organization_id,
            header="cognomen",
            alias="first_name",
        )
    )


@pytest.fixture
async def multiple_test_header_alias(
    header_test_client, multiple_test_config
) -> header_aliases_client.HeaderAliases:
    return await header_test_client.bulk_persist(
        models=[
            data_models.HeaderAliasFactory.create(
                organization_id=c.organization_id, header="cognomen", alias="first_name"
            )
            for c in multiple_test_config
        ]
    )


# region test member(versioned) fixtures


@pytest.fixture
async def test_member(test_file, member_test_client) -> member_client.Member:
    return await member_test_client.persist(
        model=data_models.MemberFactory.create(
            organization_id=test_file.organization_id, file_id=test_file.id
        )
    )


@pytest.fixture
async def test_member_versioned(
    test_file, member_versioned_test_client
) -> member_versioned_client.MemberVersioned:
    return await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id, file_id=test_file.id
        )
    )


@pytest.fixture
async def test_member_2(
    test_file,
    member_2_test_client,
) -> member_versioned_client.MemberVersioned:
    return await member_2_test_client.persist(
        model=data_models.Member2Factory.create(
            id=1001234,
            organization_id=test_file.organization_id,
        )
    )


@pytest.fixture
async def multiple_test_members(
    multiple_test_file, member_test_client
) -> member_client.Members:
    await member_test_client.bulk_persist(
        models=[
            data_models.MemberFactory.create(
                organization_id=f.organization_id, file_id=f.id
            )
            for f in multiple_test_file
        ]
    )

    return await member_test_client.all()


@pytest.fixture
async def multiple_test_members_versioned(
    multiple_test_file, member_versioned_test_client
):
    result = await member_versioned_test_client.bulk_persist(
        models=[
            data_models.MemberVersionedFactory.create(
                organization_id=f.organization_id, file_id=f.id
            )
            for f in multiple_test_file
        ]
    )

    return result


@pytest.fixture
async def multiple_test_members_versioned_from_test_config(
    multiple_test_config,
    member_versioned_test_client,
    first_name=None,
    last_name=None,
    date_of_birth=None,
):
    models = []
    for f in multiple_test_config:
        params = {"organization_id": f.organization_id}

        # Add optional parameters if they are not None
        if first_name is not None:
            params["first_name"] = first_name
        if last_name is not None:
            params["last_name"] = last_name
        if date_of_birth is not None:
            params["date_of_birth"] = date_of_birth

        member = data_models.MemberVersionedFactory.create(**params)
        models.append(member)

    result = await member_versioned_test_client.bulk_persist(models=models)

    return result


@pytest.fixture
async def multiple_test_members_2_from_test_config(
    multiple_test_config,
    member_2_test_client,
    first_name=None,
    last_name=None,
    date_of_birth=None,
):
    member_id = 1000
    results = []
    for f in multiple_test_config:
        member_id += 1
        params = {"organization_id": f.organization_id, "id": member_id}

        # Add optional parameters if they are not None
        if first_name is not None:
            params["first_name"] = first_name
        if last_name is not None:
            params["last_name"] = last_name
        if date_of_birth is not None:
            params["date_of_birth"] = date_of_birth

        member = data_models.Member2Factory.create(**params)
        record = await member_2_test_client.persist(model=member)
        results.append(record)

    return results


@pytest.fixture(params=["True", "False", ""])
async def test_member_do_not_contact(
    request, test_file, member_test_client
) -> member_client.Member:
    return await member_test_client.persist(
        model=data_models.MemberFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            do_not_contact=request.param,
        )
    )


@pytest.fixture(params=["True", "False", ""])
async def test_member_versioned_do_not_contact(
    request, test_file, member_versioned_test_client
) -> member_versioned_client.MembersVersioned:
    return await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            do_not_contact=request.param,
        )
    )


@pytest.fixture(params=["True", "False", ""])
async def test_member_versioned_dependent_id(
    request, test_file, member_versioned_test_client
) -> member_versioned_client.MembersVersioned:
    return await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            dependent_id=request.param,
        )
    )


# endregion


# region verification and verification attempt fixtures


@pytest.fixture
async def test_verification(test_config, verification_test_client):
    return await verification_test_client.persist(
        model=data_models.VerificationFactory.create(
            organization_id=test_config.organization_id,
            verification_session=None,
            verification_2_id=1001,
        )
    )


@pytest.fixture
async def test_eligibility_verification_record(
    test_verification,
    test_member_versioned,
    member_verification_test_client,
) -> db.model.EligibilityVerificationForUser:
    await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned.id, verification_id=test_verification.id
        )
    )

    return db.model.EligibilityVerificationForUser(
        verification_id=test_verification.id,
        user_id=test_verification.user_id,
        organization_id=test_verification.organization_id,
        eligibility_member_id=test_member_versioned.id,
        first_name=test_member_versioned.first_name,
        last_name=test_member_versioned.last_name,
        date_of_birth=test_member_versioned.date_of_birth,
        unique_corp_id=test_member_versioned.unique_corp_id,
        dependent_id=test_member_versioned.dependent_id,
        work_state=test_member_versioned.work_state,
        email=test_member_versioned.email,
        record=test_member_versioned.record,
        verification_type=test_verification.verification_type,
        employer_assigned_id=test_member_versioned.employer_assigned_id,
        effective_range=test_member_versioned.effective_range,
        verification_created_at=test_verification.created_at,
        verification_updated_at=test_verification.updated_at,
        verification_deactivated_at=test_verification.deactivated_at,
        gender_code=test_member_versioned.gender_code,
        do_not_contact=test_member_versioned.do_not_contact,
        verified_at=test_verification.verified_at,
        additional_fields=test_verification.additional_fields,
        is_v2=False,
        verification_1_id=test_verification.id,
        verification_2_id=test_verification.verification_2_id,
    )


@pytest.fixture
async def multiple_test_verifications(
    multiple_test_config, verification_test_client
) -> verification_client.Verifications:
    await verification_test_client.bulk_persist(
        models=[
            data_models.VerificationFactory.create(organization_id=c.organization_id)
            for c in multiple_test_config
        ]
    )

    return await verification_test_client.all()


@pytest.fixture
async def multiple_test_verifications_for_user(
    multiple_test_config, verification_test_client
):
    user_id = 999
    result = await verification_test_client.bulk_persist(
        models=[
            data_models.VerificationFactory.create(
                user_id=user_id,
                organization_id=c.organization_id,
                verified_at=datetime(year=2020, month=10, day=12),
            )
            for c in multiple_test_config
        ]
    )
    return result


@pytest.fixture
async def same_user_multiple_test_verifications(
    multiple_test_config, verification_test_client
) -> List[verification_client.Verifications]:
    user_id = random.randint(10000, 20000)
    await verification_test_client.bulk_persist(
        models=[
            data_models.VerificationFactory.create(
                organization_id=c.organization_id, user_id=user_id
            )
            for c in multiple_test_config
        ]
    )
    return await verification_test_client.all()


@pytest.fixture
async def same_user_multiple_test_member_verification(
    multiple_test_members_versioned,
    same_user_multiple_test_verifications,
    member_verification_test_client,
) -> member_verification_client.MemberVerification:
    models = []
    for i in range(NUMBER_TEST_OBJECTS - 1):
        models.append(
            data_models.MemberVerificationFactory.create(
                member_id=multiple_test_members_versioned[i].id,
                verification_id=same_user_multiple_test_verifications[i].id,
            )
        )
    await member_verification_test_client.bulk_persist(models=models)
    return await member_verification_test_client.all()


@pytest.fixture
async def test_verification_attempt(
    test_config, verification_attempt_test_client
) -> verification_attempt_client.VerificationAttempt:
    return await verification_attempt_test_client.persist(
        model=data_models.VerificationAttemptFactory.create(
            organization_id=test_config.organization_id
        )
    )


@pytest.fixture
async def multiple_test_verification_attempts(
    multiple_test_config, verification_attempt_test_client
) -> verification_attempt_client.VerificationAttempt:
    await verification_attempt_test_client.bulk_persist(
        models=[
            data_models.VerificationAttemptFactory.create(
                organization_id=c.organization_id
            )
            for c in multiple_test_config
        ]
    )

    return await verification_attempt_test_client.all()


@pytest.fixture
async def test_member_verification(
    test_member_versioned, test_verification, member_verification_test_client
) -> member_verification_client.MemberVerification:
    return await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned.id, verification_id=test_verification.id
        )
    )


@pytest.fixture
async def test_member_verification_attempt(
    test_member_versioned, test_verification_attempt, member_verification_test_client
) -> member_verification_client.MemberVerification:
    return await member_verification_test_client.persist(
        model=data_models.MemberVerificationFactory.create(
            member_id=test_member_versioned.id,
            verification_attempt_id=test_verification_attempt.id,
            verification_id=None,
        )
    )


@pytest.fixture
async def multiple_test_member_verification(
    test_member_versioned, multiple_test_verifications, member_verification_test_client
) -> member_verification_client.MemberVerification:
    await member_verification_test_client.bulk_persist(
        models=[
            data_models.MemberVerificationFactory.create(
                member_id=test_member_versioned.id, verification_id=v.id
            )
            for v in multiple_test_verifications
        ]
    )
    return await member_verification_test_client.all()


@pytest.fixture
async def multiple_test_member_verification_multiple_members(
    multiple_test_members_versioned,
    multiple_test_verifications,
    member_verification_test_client,
) -> member_verification_client.MemberVerification:
    models = []
    for i in range(NUMBER_TEST_OBJECTS - 1):
        models.append(
            data_models.MemberVerificationFactory.create(
                member_id=multiple_test_members_versioned[i].id,
                verification_id=multiple_test_verifications[i].id,
            )
        )
    await member_verification_test_client.bulk_persist(models=models)
    return await member_verification_test_client.all()


# endregion
# region population and sub_population fixtures


@pytest.fixture
async def test_population(
    test_config,
    population_test_client,
) -> pop_model.Population:
    return await population_test_client.persist(
        model=data_models.PopulationFactory.create(
            organization_id=test_config.organization_id
        )
    )


@pytest.fixture
async def active_test_population(
    test_config,
    population_test_client,
) -> pop_model.Population:
    return await population_test_client.persist(
        model=data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
    )


@pytest.fixture
async def multiple_test_populations(
    multiple_test_config,
    population_test_client,
) -> List[pop_model.Population]:
    await population_test_client.bulk_persist(
        models=[
            data_models.PopulationFactory.create(organization_id=c.organization_id)
            for c in multiple_test_config
        ]
    )

    return await population_test_client.all()


@pytest.fixture
async def multiple_active_test_populations(
    multiple_test_config,
    population_test_client,
) -> List[pop_model.Population]:
    await population_test_client.bulk_persist(
        models=[
            data_models.PopulationFactory.create(
                organization_id=c.organization_id,
                activated_at=datetime.now(timezone.utc) - timedelta(days=1),
            )
            for c in multiple_test_config
        ]
    )

    return await population_test_client.all()


@pytest.fixture
async def test_sub_population(
    test_population,
    sub_population_test_client,
) -> pop_model.SubPopulation:
    return await sub_population_test_client.persist(
        model=data_models.SubPopulationFactory.create(population_id=test_population.id)
    )


@pytest.fixture
async def multiple_test_sub_populations(
    multiple_test_populations,
    sub_population_test_client,
) -> List[pop_model.SubPopulation]:
    await sub_population_test_client.bulk_persist(
        models=[
            data_models.SubPopulationFactory.create(population_id=p.id)
            for p in multiple_test_populations
        ]
    )

    return await sub_population_test_client.all()


@pytest.fixture
async def mapped_population(
    test_config: db_model.Configuration,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
) -> pop_model.Population:
    return await population_test_client.persist(
        model=data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            sub_pop_lookup_keys_csv="work_state,custom_attributes.employment_status,custom_attributes.group_number",
            sub_pop_lookup_map_json={
                "NY": {
                    "Full": {"1": 101, "2": 102, "3": 103},
                    "Part": {"1": 201, "2": 202, "3": 203},
                },
                "ZZ": {
                    "Full": {"1": 201, "2": 202, "3": 203},
                    "Part": {"1": 301, "2": 302, "3": 303},
                },
            },
            activated_at=datetime.now(timezone.utc) - timedelta(days=1),
        )
    )


@pytest.fixture
async def mapped_sub_populations(
    mapped_population: pop_model.Population,
    sub_population_test_client: sub_population_client.SubPopulations,
    population_test_client: population_client.Populations,
) -> List[pop_model.SubPopulation]:
    ret_list = await create_mapped_sub_populations(
        mapped_population_id=mapped_population.id,
        sub_population_test_client=sub_population_test_client,
        population_test_client=population_test_client,
    )

    sub_pop_id_dict = {sub_pop.feature_set_name: sub_pop.id for sub_pop in ret_list}
    await population_test_client.set_sub_pop_lookup_info(
        population_id=mapped_population.id,
        sub_pop_lookup_keys_csv="work_state,custom_attributes.employment_status,custom_attributes.group_number",
        sub_pop_lookup_map={
            "NY": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_01", None),
                    "2": sub_pop_id_dict.get("fs_02", None),
                    "3": sub_pop_id_dict.get("fs_03", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
            },
            "ZZ": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_07", None),
                    "2": sub_pop_id_dict.get("fs_08", None),
                    "3": sub_pop_id_dict.get("fs_09", None),
                },
            },
        },
    )

    return ret_list


async def create_mapped_sub_populations(
    mapped_population_id: int,
    sub_population_test_client: sub_population_client.SubPopulations,
    population_test_client: population_client.Populations,
) -> List[pop_model.SubPopulation]:
    ret_list = await sub_population_test_client.bulk_persist(
        models=[
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_01",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1101,1102,1103,1104,1105,1106,1107,1108,1109,1110,1111",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2101",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_02",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1101,1103,1105,1107,1109,1111",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2101",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_03",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1101,1102,1103",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2101",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_04",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1101,1202,1103,1204,1105,1206,1107,1208,1109,1210,1111",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2102",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_05",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1202,1204,1206,1208,1210",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2102",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_06",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1101,1102,1103",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2102",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_07",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1301,1302,1303",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2103",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_08",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "1301",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "2104",
                },
            ),
            data_models.SubPopulationFactory.create(
                population_id=mapped_population_id,
                feature_set_name="fs_09",
                feature_set_details_json={
                    f"{pop_model.FeatureTypes.TRACK_FEATURE}": "",
                    f"{pop_model.FeatureTypes.WALLET_FEATURE}": "",
                },
            ),
        ]
    )
    sub_pop_id_dict = {sub_pop.feature_set_name: sub_pop.id for sub_pop in ret_list}
    await population_test_client.set_sub_pop_lookup_info(
        population_id=mapped_population_id,
        sub_pop_lookup_keys_csv="work_state,custom_attributes.employment_status,custom_attributes.group_number",
        sub_pop_lookup_map={
            "NY": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_01", None),
                    "2": sub_pop_id_dict.get("fs_02", None),
                    "3": sub_pop_id_dict.get("fs_03", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
            },
            "ZZ": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_07", None),
                    "2": sub_pop_id_dict.get("fs_08", None),
                    "3": sub_pop_id_dict.get("fs_09", None),
                },
            },
        },
    )

    return ret_list


@pytest.fixture
async def mapped_advanced_population(
    test_config: db_model.Configuration,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
) -> pop_model.Population:
    return await population_test_client.persist(
        model=data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            advanced=True,
            sub_pop_lookup_keys_csv="work_state,custom_attributes.employment_status,custom_attributes.group_number",
            sub_pop_lookup_map_json={
                "NY": {
                    "Full": {"1": 101, "2": 102, "3": 103, "ATTRIBUTE_IS_NULL": 104},
                    "Part": {"1": 201, "2": 202, "3": 203, "ATTRIBUTE_IS_NULL": 204},
                    "ATTRIBUTE_IS_NULL": {
                        "1": 301,
                        "2": 302,
                        "3": 303,
                        "ATTRIBUTE_IS_NULL": 304,
                    },
                },
                "ATTRIBUTE_DEFAULT_CASE": {
                    "Full": {"1": 401, "2": 402, "3": 403, "ATTRIBUTE_IS_NULL": 404},
                    "Part": {"1": 501, "2": 502, "3": 503, "ATTRIBUTE_IS_NULL": 504},
                    "ATTRIBUTE_IS_NULL": {"ATTRIBUTE_DEFAULT_CASE": 600},
                },
            },
        )
    )


@pytest.fixture
async def mapped_populations_with_bool(
    test_config: db_model.Configuration,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
) -> pop_model.Population:
    return await population_test_client.persist(
        model=data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            advanced=True,
            sub_pop_lookup_keys_csv="record.wallet_enabled",
            sub_pop_lookup_map_json={
                "true": 101,
                "false": 102,
                "ATTRIBUTE_DEFAULT_CASE": 103,
            },
        )
    )


@pytest.fixture
async def mapped_advanced_sub_populations(
    mapped_advanced_population: pop_model.Population,
    sub_population_test_client: sub_population_client.SubPopulations,
    population_test_client: population_client.Populations,
) -> List[pop_model.SubPopulation]:
    ret_list = await create_mapped_sub_populations(
        mapped_population_id=mapped_advanced_population.id,
        sub_population_test_client=sub_population_test_client,
        population_test_client=population_test_client,
    )

    sub_pop_id_dict = {sub_pop.feature_set_name: sub_pop.id for sub_pop in ret_list}
    await population_test_client.set_sub_pop_lookup_info(
        population_id=mapped_advanced_population.id,
        sub_pop_lookup_keys_csv="work_state,custom_attributes.employment_status,custom_attributes.group_number",
        sub_pop_lookup_map={
            "NY": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_01", None),
                    "2": sub_pop_id_dict.get("fs_02", None),
                    "3": sub_pop_id_dict.get("fs_03", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
                "ATTRIBUTE_IS_NULL": {
                    "1": sub_pop_id_dict.get("fs_07", None),
                    "2": sub_pop_id_dict.get("fs_08", None),
                    "3": sub_pop_id_dict.get("fs_09", None),
                },
            },
            "ATTRIBUTE_DEFAULT_CASE": {
                "Full": {
                    "1": sub_pop_id_dict.get("fs_04", None),
                    "2": sub_pop_id_dict.get("fs_05", None),
                    "3": sub_pop_id_dict.get("fs_06", None),
                },
                "Part": {
                    "1": sub_pop_id_dict.get("fs_07", None),
                    "2": sub_pop_id_dict.get("fs_08", None),
                    "3": sub_pop_id_dict.get("fs_09", None),
                },
                "ATTRIBUTE_IS_NULL": {
                    "ATTRIBUTE_DEFAULT_CASE": sub_pop_id_dict.get("fs_09", None)
                },
            },
        },
    )

    return ret_list


# endregion population and sub_population fixtures


@pytest.fixture(scope="package")
async def _streams(_records):
    from app.worker import redis as worker

    worker.stream_supervisor.project = worker.settings.project
    yield worker.stream_supervisor
    for publisher in worker.stream_supervisor.publishers.values():
        await publisher.redis.close()
    worker.stream_supervisor.publishers.clear()


@pytest.fixture
async def streams(_streams, records):
    await _streams.startup()
    for stream in redis.iterstreams(_streams):
        stream.should_stop = stream.stopped = stream.forever = False
        stream.redis = records.store.redis.redis
    yield _streams


# region data creation in Mono test DB


async def persist_maven_orgs(
    orgs: List[mono_client.MavenOrganization], conn: aiomysql.Connection
) -> mono_client.MavenOrganization:
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        await cursor.executemany(
            ORG_CREATE_SQL,
            [
                (
                    org.id,
                    org.name,
                    org.directory_name,
                    org.data_provider,
                    json.dumps(org.json),
                    org.activated_at,
                    org.terminated_at,
                )
                for org in orgs
            ],
        )
        values = [(emd, org.id) for org in orgs for emd in org.email_domains]
        await cursor.executemany(EMAIL_CREATE_SQL, values)
    return orgs


async def persist_external_ids(
    org_external_ids: List[mono_client.MavenOrgExternalID], conn: aiomysql.Connection
) -> List[mono_client.MavenOrgExternalID]:
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        await cursor.executemany(
            EXT_ID_CREATE_SQL,
            [
                (
                    "OPTUM",
                    oei.external_id,
                    oei.data_provider_organization_id,
                    oei.organization_id,
                )
                for oei in org_external_ids
            ],
        )
    return org_external_ids


async def delete_external_ids_for_org(organization_id: int, conn: aiomysql.Connection):
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        await cursor.execute(EXT_ID_DELETE_SQL, organization_id)


async def delete_all_external_ids(conn: aiomysql.Connection):
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        await cursor.execute(EXT_ID_DELETE_ALL_SQL, ())


async def create_maven_org(
    conn: aiomysql.Connection,
) -> Tuple[mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]]:
    org: mono_client.MavenOrganization = data_models.MavenOrganizationFactory.create(
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        }
    )
    org_external_ids: List[
        mono_client.MavenOrgExternalID
    ] = data_models.MavenOrgExternalIDFactory.create_batch(
        size=10, organization_id=org.id
    )
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        await cursor.execute(
            ORG_CREATE_SQL,
            (
                org.id,
                org.name,
                org.directory_name,
                org.data_provider,
                json.dumps(org.json),
                org.activated_at,
                org.terminated_at,
            ),
        )
        values = [
            (
                "OPTUM",
                oei.external_id,
                oei.data_provider_organization_id,
                oei.organization_id,
            )
            for oei in org_external_ids
        ]
        await cursor.executemany(EXT_ID_CREATE_SQL, values)
        values = [(emd, org.id) for emd in org.email_domains]
        await cursor.executemany(EMAIL_CREATE_SQL, values)

    return org, org_external_ids


async def create_org_with_features(
    conn: aiomysql.Connection,
) -> Tuple[
    mono_client.MavenOrganization,
    List[mono_client.BasicClientTrack],
    List[mono_client.BasicReimbursementOrganizationSettings],
]:
    # Create the org
    org: mono_client.MavenOrganization = data_models.MavenOrganizationFactory.create(
        json={
            "PK_COLUMN": "unique_corp_id",
            translate.FIELD_MAP_KEY: {"unique_corp_id": "employee_id", "gender": "sex"},
        }
    )

    # Establish datetimes for testing tracks and reimbursement organization settings in a
    # variety of activation states. By passing in these established relative dates, we can
    # avoid using freezegun, which has caused problems in the past.
    yesterday = datetime.now(tz=timezone.utc) - timedelta(days=1)
    day_before_yesterday = yesterday - timedelta(days=1)
    tomorrow = datetime.now(tz=timezone.utc) + timedelta(days=1)
    day_after_tomorrow = tomorrow + timedelta(days=1)

    # Create the tracks
    # Note that all the tracks are added to client_tracks so that they can be added to the
    # DB, the ended tracks are not added to non_ended_client_tracks, which is part of the
    # tuple that is returned by this function. This is because when querying for the
    # organization's features, we do not expect the ended ones to be returned.
    client_tracks: List[mono_client.BasicClientTrack] = []
    non_ended_client_tracks: List[mono_client.BasicClientTrack] = []
    temp_tracks = _get_pre_launch_tracks(
        org_id=org.id,
        tomorrow=tomorrow,
        day_after_tomorrow=day_after_tomorrow,
    )
    client_tracks.extend(temp_tracks)
    non_ended_client_tracks.extend(temp_tracks)
    temp_tracks = _get_launched_tracks(
        org_id=org.id,
        yesterday=yesterday,
        tomorrow=tomorrow,
    )
    client_tracks.extend(temp_tracks)
    non_ended_client_tracks.extend(temp_tracks)
    temp_tracks = _get_ended_tracks(
        org_id=org.id,
        yesterday=yesterday,
        day_before_yesterday=day_before_yesterday,
    )
    client_tracks.extend(temp_tracks)

    # Create the ROSs
    # Note that all the tracks are added to reimbursement_organization_settings so
    # that they can be added to the DB, the ended tracks are not added to
    # non_ended_reimbursement_organization_settings, which is part of the tuple
    # that is returned by this function. This is because when querying for the
    # organization's features, we do not expect the ended ones to be returned.
    reimbursement_organization_settings: List[
        mono_client.BasicReimbursementOrganizationSettings
    ] = []
    non_ended_reimbursement_organization_settings: List[
        mono_client.BasicReimbursementOrganizationSettings
    ] = []
    temp_ros = _get_pre_launch_ros(
        org_id=org.id,
        tomorrow=tomorrow,
        day_after_tomorrow=day_after_tomorrow,
    )
    reimbursement_organization_settings.extend(temp_ros)
    non_ended_reimbursement_organization_settings.extend(temp_ros)
    temp_ros = _get_launched_ros(
        org_id=org.id,
        yesterday=yesterday,
        tomorrow=tomorrow,
    )
    reimbursement_organization_settings.extend(temp_ros)
    non_ended_reimbursement_organization_settings.extend(temp_ros)
    temp_ros = _get_ended_ros(
        org_id=org.id,
        yesterday=yesterday,
        day_before_yesterday=day_before_yesterday,
    )
    reimbursement_organization_settings.extend(temp_ros)

    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        # Add the organization to the DB
        await cursor.execute(
            ORG_CREATE_SQL,
            (
                org.id,
                org.name,
                org.directory_name,
                org.data_provider,
                json.dumps(org.json),
                org.activated_at,
                org.terminated_at,
            ),
        )
        # Add the tracks to the DB
        track_values = [
            (
                track.id,
                track.track,
                track.organization_id,
                track.active,
                track.launch_date,
                track.length_in_days,
                track.ended_at,
            )
            for track in client_tracks
        ]
        await cursor.executemany(CLIENT_TRACK_CREATE_SQL, track_values)
        # Add the ROSs to the DB
        ros_values = [
            (
                ros.id,
                ros.organization_id,
                ros.name,
                ros.benefit_faq_resource_id,
                ros.survey_url,
                ros.started_at,
                ros.ended_at,
                ros.debit_card_enabled,
                ros.cycles_enabled,
                ros.direct_payment_enabled,
                ros.rx_direct_payment_enabled,
                ros.deductible_accumulation_enabled,
                ros.closed_network,
                ros.fertility_program_type,
                ros.fertility_requires_diagnosis,
                ros.fertility_allows_taxable,
            )
            for ros in reimbursement_organization_settings
        ]
        await cursor.executemany(
            REIMBURSEMENT_ORGANIZATION_SETTINGS_CREATE_SQL, ros_values
        )

    return org, non_ended_client_tracks, non_ended_reimbursement_organization_settings


def _get_pre_launch_tracks(
    org_id: int,
    tomorrow: datetime,
    day_after_tomorrow: datetime,
) -> List[mono_client.BasicClientTrack]:
    return_tracks = [
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=0,
            launch_date=None,
            ended_at=None,
        ),
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=0,
            launch_date=None,
            ended_at=tomorrow,
        ),
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=0,
            launch_date=tomorrow,
            ended_at=None,
        ),
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=0,
            launch_date=tomorrow,
            ended_at=day_after_tomorrow,
        ),
    ]
    return return_tracks


def _get_launched_tracks(
    org_id: int,
    yesterday: datetime,
    tomorrow: datetime,
) -> List[mono_client.BasicClientTrack]:
    return_tracks = [
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=1,
            launch_date=yesterday,
            ended_at=None,
        ),
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=1,
            launch_date=yesterday,
            ended_at=tomorrow,
        ),
    ]
    return return_tracks


def _get_ended_tracks(
    org_id: int,
    yesterday: datetime,
    day_before_yesterday: datetime,
) -> List[mono_client.BasicClientTrack]:
    return_tracks = [
        data_models.MavenClientTrackFactory.create(
            organization_id=org_id,
            active=0,
            launch_date=day_before_yesterday,
            ended_at=yesterday,
        ),
    ]
    return return_tracks


def _get_pre_launch_ros(
    org_id: int,
    tomorrow: datetime,
    day_after_tomorrow: datetime,
) -> List[mono_client.BasicReimbursementOrganizationSettings]:
    return_ros = [
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=None,
            ended_at=None,
        ),
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=None,
            ended_at=tomorrow,
        ),
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=tomorrow,
            ended_at=None,
        ),
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=tomorrow,
            ended_at=day_after_tomorrow,
        ),
    ]
    return return_ros


def _get_launched_ros(
    org_id: int,
    yesterday: datetime,
    tomorrow: datetime,
) -> List[mono_client.BasicReimbursementOrganizationSettings]:
    return_ros = [
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=yesterday,
            ended_at=None,
        ),
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=yesterday,
            ended_at=tomorrow,
        ),
    ]
    return return_ros


def _get_ended_ros(
    org_id: int,
    yesterday: datetime,
    day_before_yesterday: datetime,
) -> List[mono_client.BasicReimbursementOrganizationSettings]:
    return_ros = [
        data_models.MavenReimbursementOrganizationSettingsFactory.create(
            organization_id=org_id,
            started_at=day_before_yesterday,
            ended_at=yesterday,
        ),
    ]
    return return_ros


ORG_CREATE_SQL = """
INSERT INTO maven.organization (
    id,
    name, 
    directory_name, 
    data_provider,
    json,
    multitrack_enabled,
    internal_type,
    education_only,
    activated_at,
    terminated_at
    ) 
VALUES (
    %s,
    %s,
    %s,
    %s,
    %s,
    0,
    'TEST',
    0,
    %s, 
    %s
)
"""
EXT_ID_CREATE_SQL = """
INSERT INTO maven.organization_external_id (idp, external_id, data_provider_organization_id, organization_id) 
VALUES (%s, %s, %s, %s);
"""
EXT_ID_DELETE_SQL = """
DELETE FROM maven.organization_external_id
WHERE organization_id=(%s);
"""
EXT_ID_DELETE_ALL_SQL = """
DELETE FROM maven.organization_external_id;
"""
EMAIL_CREATE_SQL = """
INSERT INTO maven.organization_email_domain (domain, organization_id, eligibility_logic) 
VALUES (%s, %s, 'CLIENT_SPECIFIC');
"""

CLIENT_TRACK_CREATE_SQL = """
INSERT INTO maven.client_track (id, track, organization_id, active, launch_date, length_in_days, ended_at)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

CLIENT_TRACK_DELETE_SQL = """
DELETE FROM maven.client_track
WHERE id=(%s)
"""

CLIENT_TRACK_DELETE_ALL_SQL = """
DELETE FROM maven.client_track
"""

REIMBURSEMENT_ORGANIZATION_SETTINGS_CREATE_SQL = """
INSERT INTO maven.reimbursement_organization_settings (
    id,
    organization_id, 
    name,
    benefit_faq_resource_id,
    survey_url,
    started_at,
    ended_at,
    debit_card_enabled,
    cycles_enabled,
    direct_payment_enabled,
    rx_direct_payment_enabled,
    deductible_accumulation_enabled,
    closed_network,
    fertility_program_type,
    fertility_requires_diagnosis,
    fertility_allows_taxable
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

REIMBURSEMENT_ORGANIZATION_SETTINGS_DELETE_SQL = """
DELETE FROM maven.reimbursement_organization_settings
WHERE id=(%s)
"""

REIMBURSEMENT_ORGANIZATION_SETTINGS_DELETE_ALL_SQL = """
DELETE FROM maven.reimbursement_organization_settings
"""

CREDIT_CREATE_SQL = """
INSERT INTO maven.credit
(id, user_id, organization_employee_id, eligibility_verification_id, eligibility_member_id, created_at)
VALUES (%s, %s, %s, %s, %s, %s);
"""

OE_CREATE_SQL = """
INSERT INTO maven.organization_employee
(id, organization_id, unique_corp_id, email, date_of_birth, first_name, 
    last_name, work_state, modified_at, deleted_at, retention_start_date, created_at, 
    json, dependent_id, eligibility_member_id_deleted, alegeus_id, eligibility_member_id, 
    eligibility_member_2_id, eligibility_member_2_version)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

MT_CREATE_SQL = """
INSERT INTO maven.member_track
(id, client_track_id, user_id, organization_employee_id, auto_transitioned, created_at, name, bucket_id, start_date, activated_at, eligibility_verification_id, eligibility_member_id)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

OED_CREATE_SQL = """
INSERT INTO maven.organization_employee_dependent
(id, organization_employee_id, reimbursement_wallet_id)
VALUES (%s, %s, %s);
"""

RW_CREATE_SQL = """
INSERT INTO maven.reimbursement_wallet
(id, organization_employee_id, reimbursement_organization_settings_id)
VALUES (%s, %s, %s);
"""

# endregion data creation in Mono test DB


@pytest.fixture
def external_record(test_config):
    member = factory.MemberFactory.create(organization_id=test_config.organization_id)
    return {
        "first_name": member.first_name,
        "last_name": member.last_name,
        "date_of_birth": member.date_of_birth,
        "work_state": member.work_state,
        "email": member.email,
        "unique_corp_id": member.unique_corp_id,
        "effective_range": member.effective_range,
        "do_not_contact": member.do_not_contact,
        "gender_code": member.gender_code,
        "organization_id": member.organization_id,
        "record": member.record,
        "dependent_id": member.dependent_id,
        "employer_assigned_id": member.unique_corp_id,
        "external_id": member.unique_corp_id,
        "external_name": "foobar",
        "custom_attributes": {"foo": "bar"},
    }


@pytest.fixture
def ff_test_data():
    with feature_flags.test_data() as td:
        yield td


@pytest.fixture
def mock_is_overeligibility_enabled(ff_test_data):
    def _mock(is_enabled: bool = True):
        ff_test_data.update(
            ff_test_data.flag(
                e9y_constants.E9yFeatureFlag.RELEASE_OVER_ELIGIBILITY,
            ).variation_for_all(is_enabled)
        )

    return _mock


@pytest.fixture
def mock_all_orgs_enabled_for_overeligibility():
    with mock.patch(
        "app.utils.feature_flag.are_all_organizations_enabled_for_overeligibility",
        return_value=True,
    ):
        yield


@pytest.fixture
async def credit_records(
    maven_connection,
) -> List[mono_client.BasicCredit]:
    return await create_credit_records(maven_connection)


async def create_credit_records(
    conn: aiomysql.Connection,
) -> List[mono_client.BasicCredit]:

    records: List[mono_client.BasicCredit] = [
        # no back fill, e9y data exists
        mono_client.BasicCredit(
            id=1,
            user_id=21,
            organization_employee_id=31,
            eligibility_verification_id=41,
            eligibility_member_id=51,
            created_at=datetime(2024, 1, 1),
        ),
        # back fill
        mono_client.BasicCredit(
            id=2,
            user_id=22,
            organization_employee_id=32,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2024, 1, 1),
        ),
        # no back fill, created early
        mono_client.BasicCredit(
            id=3,
            user_id=23,
            organization_employee_id=33,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2022, 1, 1),
        ),
        # no back fill, oe id is None
        mono_client.BasicCredit(
            id=4,
            user_id=24,
            organization_employee_id=None,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2024, 1, 1),
        ),
        # backfill, with oe_e9y_member_id
        mono_client.BasicCredit(
            id=5,
            user_id=25,
            organization_employee_id=35,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2024, 1, 1),
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        credit_values = [
            (
                credit.id,
                credit.user_id,
                credit.organization_employee_id,
                credit.eligibility_verification_id,
                credit.eligibility_member_id,
                credit.created_at,
            )
            for credit in records
        ]
        await cursor.executemany(CREDIT_CREATE_SQL, credit_values)

    return records


@pytest.fixture
async def oe_records(
    maven_connection,
) -> List[mono_client.OrganizationEmployee]:
    return await create_oe_records(maven_connection)


async def create_oe_records(
    conn: aiomysql.Connection,
) -> List[mono_client.OrganizationEmployee]:
    records = [
        mono_client.OrganizationEmployee(id=31, organization_id=100),
        mono_client.OrganizationEmployee(id=32, organization_id=200),
        mono_client.OrganizationEmployee(id=33, organization_id=300),
        mono_client.OrganizationEmployee(
            id=35, organization_id=500, eligibility_member_id=1001
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        oe_values = [
            (
                oe.id,
                oe.organization_id,
                oe.unique_corp_id,
                oe.email,
                oe.date_of_birth,
                oe.first_name,
                oe.last_name,
                oe.work_state,
                oe.modified_at,
                oe.deleted_at,
                oe.retention_start_date,
                oe.created_at,
                oe.json,
                oe.dependent_id,
                oe.eligibility_member_id_deleted,
                oe.alegeus_id,
                oe.eligibility_member_id,
                oe.eligibility_member_2_id,
                oe.eligibility_member_2_version,
            )
            for oe in records
        ]
        await cursor.executemany(OE_CREATE_SQL, oe_values)
    return records


@pytest.fixture
async def member_track_records(
    maven_connection,
) -> List[mono_client.BasicMemberTrack]:
    return await create_member_track_records(maven_connection)


async def create_member_track_records(
    conn: aiomysql.Connection,
) -> List[mono_client.BasicMemberTrack]:

    records: List[mono_client.BasicMemberTrack] = [
        # no back fill, e9y data exists, both
        mono_client.BasicMemberTrack(
            id=1,
            user_id=21,
            client_track_id=1001,
            organization_employee_id=31,
            eligibility_verification_id=41,
            eligibility_member_id=51,
            created_at=datetime(2024, 1, 1),
        ),
        # back fill
        mono_client.BasicMemberTrack(
            id=2,
            user_id=22,
            client_track_id=1002,
            organization_employee_id=32,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2024, 1, 1),
        ),
        # backfill for v2
        mono_client.BasicMemberTrack(
            id=3,
            user_id=23,
            client_track_id=1003,
            organization_employee_id=33,
            eligibility_verification_id=3,
            eligibility_member_id=None,
            created_at=datetime(2022, 1, 1),
        ),
        # no back fill, oe id is None
        mono_client.BasicMemberTrack(
            id=4,
            user_id=24,
            client_track_id=1004,
            organization_employee_id=45,
            eligibility_verification_id=None,
            eligibility_member_id=4,
            created_at=datetime(2024, 1, 1),
        ),
        # back fill
        mono_client.BasicMemberTrack(
            id=5,
            user_id=25,
            client_track_id=1005,
            organization_employee_id=35,
            eligibility_verification_id=None,
            eligibility_member_id=None,
            created_at=datetime(2025, 1, 1),
        ),
        # backfill for v2
        mono_client.BasicMemberTrack(
            id=6,
            user_id=26,
            client_track_id=1003,
            organization_employee_id=36,
            eligibility_verification_id=6,
            eligibility_member_id=None,
            created_at=datetime(2022, 1, 1),
        ),
        # no backfill for v2, organization_id not match
        mono_client.BasicMemberTrack(
            id=7,
            user_id=27,
            client_track_id=1004,
            organization_employee_id=37,
            eligibility_verification_id=7,
            eligibility_member_id=None,
            created_at=datetime(2022, 1, 1),
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        values = [
            (
                mt.id,
                mt.client_track_id,
                mt.user_id,
                mt.organization_employee_id,
                mt.auto_transitioned,
                mt.created_at,
                mt.name,
                mt.bucket_id,
                mt.start_date,
                mt.activated_at,
                mt.eligibility_verification_id,
                mt.eligibility_member_id,
            )
            for mt in records
        ]
        await cursor.executemany(MT_CREATE_SQL, values)

    return records


@pytest.fixture
async def client_track_records(
    maven_connection,
) -> List[mono_client.BasicClientTrack]:
    return await create_client_track_records(maven_connection)


async def create_client_track_records(
    conn: aiomysql.Connection,
) -> List[mono_client.BasicClientTrack]:

    records: List[mono_client.BasicClientTrack] = [
        mono_client.BasicClientTrack(
            id=1001,
            track="Mocked",
            organization_id=101,
        ),
        mono_client.BasicClientTrack(
            id=1002,
            track="Mocked",
            organization_id=102,
        ),
        mono_client.BasicClientTrack(
            id=1003,
            track="Mocked",
            organization_id=103,
        ),
        mono_client.BasicClientTrack(
            id=1004,
            track="Mocked",
            organization_id=104,
        ),
        mono_client.BasicClientTrack(
            id=1005,
            track="Mocked",
            organization_id=105,
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        values = [
            (
                ct.id,
                ct.track,
                ct.organization_id,
                ct.active,
                ct.launch_date,
                ct.length_in_days,
                ct.ended_at,
            )
            for ct in records
        ]
        await cursor.executemany(CLIENT_TRACK_CREATE_SQL, values)

    return records


@pytest.fixture
async def oed_records(
    maven_connection,
) -> List[mono_client.BasicOrganizationEmployeeDependent]:
    return await create_oed_records(maven_connection)


async def create_oed_records(
    conn: aiomysql.Connection,
) -> List[mono_client.BasicOrganizationEmployeeDependent]:

    records: List[mono_client.BasicOrganizationEmployeeDependent] = [
        # no back fill, rw_id exists
        mono_client.BasicOrganizationEmployeeDependent(
            id=1,
            organization_employee_id=31,
            reimbursement_wallet_id=41,
        ),
        # back fill
        mono_client.BasicOrganizationEmployeeDependent(
            id=2,
            organization_employee_id=32,
            reimbursement_wallet_id=None,
        ),
        mono_client.BasicOrganizationEmployeeDependent(
            id=3,
            organization_employee_id=33,
            reimbursement_wallet_id=None,
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        values = [
            (
                oed.id,
                oed.organization_employee_id,
                oed.reimbursement_wallet_id,
            )
            for oed in records
        ]
        await cursor.executemany(OED_CREATE_SQL, values)

    return records


@pytest.fixture
async def rw_records(
    maven_connection,
) -> List[mono_client.BasicReimbursementWallet]:
    return await create_rw_records(maven_connection)


async def create_rw_records(
    conn: aiomysql.Connection,
) -> List[mono_client.BasicReimbursementWallet]:

    records: List[mono_client.BasicReimbursementWallet] = [
        # no back fill, rw_id exists
        mono_client.BasicReimbursementWallet(
            id=41, organization_employee_id=31, reimbursement_organization_settings_id=1
        ),
        # back fill
        mono_client.BasicReimbursementWallet(
            id=10002,
            organization_employee_id=32,
            reimbursement_organization_settings_id=2,
        ),
        mono_client.BasicReimbursementWallet(
            id=10003,
            organization_employee_id=33,
            reimbursement_organization_settings_id=3,
        ),
    ]
    cursor: aiomysql.Cursor
    async with conn.cursor() as cursor:
        values = [
            (
                rw.id,
                rw.organization_employee_id,
                rw.reimbursement_organization_settings_id,
            )
            for rw in records
        ]
        await cursor.executemany(RW_CREATE_SQL, values)

    return records
