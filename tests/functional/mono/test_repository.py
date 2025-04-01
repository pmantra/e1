from __future__ import annotations

from typing import List, Tuple

import pytest

from db.mono.client import (
    BasicClientTrack,
    BasicReimbursementOrganizationSettings,
    MavenOrganization,
)

pytestmark = pytest.mark.asyncio


async def test_get_non_ended_track_information_for_organization_id(
    maven_repo,
    maven_org_with_features: Tuple[
        MavenOrganization,
        List[BasicClientTrack],
        List[BasicReimbursementOrganizationSettings],
    ],
):
    # Given
    org, track_list, _ = maven_org_with_features
    # When
    fetched_client_track_info = (
        await maven_repo.get_non_ended_track_information_for_organization_id(
            organization_id=org.id
        )
    )
    # Then
    assert len(track_list) == len(fetched_client_track_info)
    track_set = {track.id for track in track_list}
    fetched_set = {fetched.id for fetched in fetched_client_track_info}
    assert track_set == fetched_set


async def test_get_non_ended_reimbursement_organization_settings_information_for_organization_id(
    maven_repo,
    maven_org_with_features: Tuple[
        MavenOrganization,
        List[BasicClientTrack],
        List[BasicReimbursementOrganizationSettings],
    ],
):
    # Given
    org, _, reimbursement_organization_settings_list = maven_org_with_features
    # When
    fetched_ros_info = await maven_repo.get_non_ended_reimbursement_organization_settings_information_for_organization_id(
        organization_id=org.id
    )
    # Then
    assert len(reimbursement_organization_settings_list) == len(fetched_ros_info)
    ros_set = {ros.id for ros in reimbursement_organization_settings_list}
    fetched_set = {fetched.id for fetched in fetched_ros_info}
    assert ros_set == fetched_set
