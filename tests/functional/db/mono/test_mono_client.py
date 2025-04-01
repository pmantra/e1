from __future__ import annotations

import datetime
import json
from typing import List, Tuple

import pytest

from db.mono import client as mono_client
from db.mono.model import CreditBackfillRequest, MemberTrackBackfillRequest

pytestmark = pytest.mark.asyncio


async def test_get_org_from_directory(
    maven,
    maven_connection,
    maven_org: Tuple[
        mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]
    ],
):
    # Given
    org = maven_org[0]
    # When
    located = await maven.get_org_from_directory(
        c=maven_connection, name=org.directory_name
    )
    # Then
    assert located == org


async def test_get_org_external_ids_for_org(
    maven,
    maven_connection,
    maven_org: Tuple[
        mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]
    ],
):
    # Given
    org, external_ids = maven_org[0], maven_org[1]
    # When
    external_ids: List[
        mono_client.MavenOrgExternalID
    ] = await maven.get_org_external_ids_for_org(c=maven_connection, org_id=org.id)
    # Then
    assert len(external_ids) == len(external_ids)


async def test_get_all_external_ids_for_sync(
    maven,
    maven_connection,
    maven_org: Tuple[
        mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]
    ],
):
    # Given
    expected_eids: List[mono_client.MavenOrgExternalID]
    expected_eids = maven_org[1]
    # When
    external_ids: List[
        mono_client.MavenOrgExternalID
    ] = await maven.get_all_external_ids_for_sync(c=maven_connection)
    # Then
    assert [
        (eid.organization_id, eid.external_id, eid.data_provider_organization_id)
        for eid in external_ids
    ] == [
        (eid.organization_id, eid.external_id, eid.data_provider_organization_id)
        for eid in expected_eids
    ]


async def test_get_org_for_sync_cursor(
    maven,
    maven_org: Tuple[
        mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]
    ],
):
    # Given
    expected_org: mono_client.MavenOrganization
    expected_org = maven_org[0]
    # When
    async with maven.get_orgs_for_sync_cursor() as cur:
        orgs = await cur.fetchall()
    # Then
    assert len(orgs) == 1
    located = orgs[0]
    assert set(json.loads(located["email_domains"])) == expected_org.email_domains


async def test_get_external_ids_for_sync_cursor(
    maven,
    maven_org: Tuple[
        mono_client.MavenOrganization, List[mono_client.MavenOrgExternalID]
    ],
):
    # Given
    expected_eids: List[mono_client.MavenOrgExternalID]
    expected_eids = maven_org[1]
    # When
    fetched_eids: List[
        mono_client.MavenOrgExternalID
    ] = await maven.get_all_external_ids_for_sync()
    # Then
    assert [
        (eid.organization_id, eid.external_id, eid.data_provider_organization_id)
        for eid in fetched_eids
    ] == [
        (eid.organization_id, eid.external_id, eid.data_provider_organization_id)
        for eid in expected_eids
    ]


async def test_get_non_ended_track_information_for_organization_id(
    maven,
    maven_org_with_features: Tuple[
        mono_client.MavenOrganization,
        List[mono_client.BasicClientTrack],
        List[mono_client.BasicReimbursementOrganizationSettings],
    ],
):
    # Given
    org, track_list, _ = maven_org_with_features
    # When
    fetched_client_track_info = (
        await maven.get_non_ended_track_information_for_organization_id(org.id)
    )
    # Then
    assert len(track_list) == len(fetched_client_track_info)
    track_set = {track.id for track in track_list}
    fetched_set = {fetched.id for fetched in fetched_client_track_info}
    assert track_set == fetched_set


async def test_get_non_ended_reimbursement_organization_settings_information_for_organization_id(
    maven,
    maven_org_with_features: Tuple[
        mono_client.MavenOrganization,
        List[mono_client.BasicClientTrack],
        List[mono_client.BasicReimbursementOrganizationSettings],
    ],
):
    # Given
    org, _, reimbursement_organization_settings_list = maven_org_with_features
    # When
    fetched_ros_info = await maven.get_non_ended_reimbursement_organization_settings_information_for_organization_id(
        organization_id=org.id
    )
    # Then
    assert len(reimbursement_organization_settings_list) == len(fetched_ros_info)
    ros_set = {ros.id for ros in reimbursement_organization_settings_list}
    fetched_set = {fetched.id for fetched in fetched_ros_info}
    assert ros_set == fetched_set


async def test_get_credit_back_fill_requests(
    maven,
    credit_records: List[mono_client.BasicCredit],
    oe_records: List[mono_client.OrganizationEmployee],
):
    # when
    data = await maven.get_credit_back_fill_requests(batch_size=1000, last_id=0)

    # then
    assert data == [
        CreditBackfillRequest(
            id=2,
            user_id=22,
            organization_id=200,
            organization_employee_id=32,
            oe_e9y_member_id=None,
        ),
        CreditBackfillRequest(
            id=5,
            user_id=25,
            organization_id=500,
            organization_employee_id=35,
            oe_e9y_member_id=1001,
        ),
    ]

    # small batch
    # when
    data = await maven.get_credit_back_fill_requests(batch_size=1, last_id=0)

    # then
    assert data == [
        CreditBackfillRequest(
            id=2,
            user_id=22,
            organization_id=200,
            organization_employee_id=32,
            oe_e9y_member_id=None,
        )
    ]

    data = await maven.get_credit_back_fill_requests(batch_size=1, last_id=2)
    assert data == [
        CreditBackfillRequest(
            id=5,
            user_id=25,
            organization_id=500,
            organization_employee_id=35,
            oe_e9y_member_id=1001,
        )
    ]


async def test_backfill_credit_record(
    maven,
    credit_records: List[mono_client.BasicCredit],
):
    # given
    credit_id = credit_records[0].id
    e9y_verification_id = 10001
    e9y_member_id = 10002

    # when
    await maven.backfill_credit_record(
        id=credit_id,
        e9y_verification_id=e9y_verification_id,
        e9y_member_id=e9y_member_id,
    )

    # then
    async with maven.connector.connection() as conn:
        async with conn.cursor() as cur:  # Get a cursor from the connection
            await cur.execute(f"SELECT * FROM maven.credit WHERE id={credit_id}")
            res = await cur.fetchall()
            # Execute your SQ
            assert len(res) == 1
            assert res[0]["eligibility_verification_id"] == e9y_verification_id
            assert res[0]["eligibility_member_id"] == e9y_member_id


async def test_get_member_track_back_fill_requests(
    maven,
    member_track_records: List[mono_client.BasicMemberTrack],
    client_track_records: List[mono_client.BasicClientTrack],
):
    # when
    r1 = MemberTrackBackfillRequest(
        member_track_id=2,
        user_id=22,
        organization_employee_id=32,
        created_at=datetime.datetime(2024, 1, 1, 0, 0),
        organization_id=102,
    )
    r2 = MemberTrackBackfillRequest(
        member_track_id=5,
        user_id=25,
        organization_employee_id=35,
        created_at=datetime.datetime(2025, 1, 1, 0, 0),
        organization_id=105,
    )
    data = await maven.get_member_track_back_fill_requests(batch_size=1000, last_id=0)

    # then
    assert data == [r1, r2]

    # small batch
    # when
    data = await maven.get_member_track_back_fill_requests(batch_size=1, last_id=0)

    # then
    assert data == [r1]

    data = await maven.get_member_track_back_fill_requests(batch_size=1, last_id=4)
    assert data == [r2]


async def test_get_member_track_back_fill_requests_for_v2(
    maven,
    member_track_records: List[mono_client.BasicMemberTrack],
    client_track_records: List[mono_client.BasicClientTrack],
):
    # when
    r1 = MemberTrackBackfillRequest(
        member_track_id=3,
        user_id=23,
        organization_employee_id=33,
        created_at=datetime.datetime(2022, 1, 1, 0, 0),
        organization_id=103,
        existing_e9y_verification_id=3,
        existing_e9y_member_id=None,
    )
    r2 = MemberTrackBackfillRequest(
        member_track_id=6,
        user_id=26,
        organization_employee_id=36,
        created_at=datetime.datetime(2022, 1, 1, 0, 0),
        organization_id=103,
        existing_e9y_verification_id=6,
        existing_e9y_member_id=None,
    )
    organization_id = 103
    data = await maven.get_member_track_back_fill_requests_for_v2(
        organization_id=organization_id, batch_size=1000, last_id=0
    )

    # then
    assert data == [r1, r2]

    # small batch
    # when
    data = await maven.get_member_track_back_fill_requests_for_v2(
        organization_id=organization_id, batch_size=1, last_id=0
    )

    # then
    assert data == [r1]

    data = await maven.get_member_track_back_fill_requests_for_v2(
        organization_id=organization_id, batch_size=1, last_id=4
    )
    assert data == [r2]


async def test_get_member_track_back_fill_requests_for_billing(
    maven,
    member_track_records: List[mono_client.BasicMemberTrack],
    client_track_records: List[mono_client.BasicClientTrack],
):
    member_track_ids = [2, 4, 6]

    data = await maven.get_member_track_back_fill_requests_for_billing(
        member_track_ids=member_track_ids
    )

    assert data == [
        MemberTrackBackfillRequest(
            member_track_id=2,
            user_id=22,
            organization_employee_id=32,
            created_at=datetime.datetime(2024, 1, 1, 0, 0),
            organization_id=102,
            existing_e9y_verification_id=None,
            existing_e9y_member_id=None,
        ),
        MemberTrackBackfillRequest(
            member_track_id=4,
            user_id=24,
            organization_employee_id=45,
            created_at=datetime.datetime(2024, 1, 1, 0, 0),
            organization_id=104,
            existing_e9y_verification_id=None,
            existing_e9y_member_id=4,
        ),
        MemberTrackBackfillRequest(
            member_track_id=6,
            user_id=26,
            organization_employee_id=36,
            created_at=datetime.datetime(2022, 1, 1, 0, 0),
            organization_id=103,
            existing_e9y_verification_id=6,
            existing_e9y_member_id=None,
        ),
    ]


async def test_backfill_member_track_record(
    maven,
    member_track_records: List[mono_client.BasicMemberTrack],
):
    # given
    member_track_id = member_track_records[0].id
    e9y_verification_id = 10001
    e9y_member_id = 10002

    # when
    await maven.backfill_member_track_record(
        id=member_track_id,
        e9y_verification_id=e9y_verification_id,
        e9y_member_id=e9y_member_id,
    )

    # then
    async with maven.connector.connection() as conn:
        async with conn.cursor() as cur:  # Get a cursor from the connection
            await cur.execute(
                f"SELECT * FROM maven.member_track WHERE id={member_track_id}"
            )
            res = await cur.fetchall()
            # Execute your SQ
            assert len(res) == 1
            assert res[0]["eligibility_verification_id"] == e9y_verification_id
            assert res[0]["eligibility_member_id"] == e9y_member_id


async def test_get_oed_back_fill_requests(
    maven,
    oed_records: List[mono_client.BasicOrganizationEmployeeDependent],
):
    # when
    data = await maven.get_oed_back_fill_requests(batch_size=1000, last_id=0)

    # then
    assert set(data) == {2, 3}

    # small batch
    # when
    data = await maven.get_oed_back_fill_requests(batch_size=1, last_id=0)

    # then
    assert data == [2]

    data = await maven.get_oed_back_fill_requests(batch_size=1, last_id=2)
    assert data == [3]


async def test_get_rw_id_for_oed(
    maven,
    rw_records: List[mono_client.BasicReimbursementWallet],
    oed_records: List[mono_client.BasicOrganizationEmployeeDependent],
):
    # given
    id = 2
    reimbursement_wallet_id = 10002

    # when
    rw_id = await maven.get_rw_id_for_oed(
        id=id,
    )

    # then
    assert rw_id == reimbursement_wallet_id

    # when
    rw_id = await maven.get_rw_id_for_oed(
        id=1000_003,
    )

    # then
    assert rw_id is None


async def test_backfill_oed_record(
    maven,
    oed_records: List[mono_client.BasicOrganizationEmployeeDependent],
):
    # given
    id = oed_records[0].id
    reimbursement_wallet_id = 10001

    # when
    await maven.backfill_oed_record(
        id=id,
        reimbursement_wallet_id=reimbursement_wallet_id,
    )

    # then
    async with maven.connector.connection() as conn:
        async with conn.cursor() as cur:  # Get a cursor from the connection
            await cur.execute(
                f"SELECT * FROM maven.organization_employee_dependent WHERE id={id}"
            )
            res = await cur.fetchall()
            # Execute your SQ
            assert len(res) == 1
            assert res[0]["reimbursement_wallet_id"] == reimbursement_wallet_id
