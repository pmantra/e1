from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import List, Optional

from mmlib.ops import log

from app.eligibility import errors
from app.utils import async_ttl_cache
from db import model
from db.clients import configuration_client
from db.model import MemberVersioned

logger = log.getLogger(__name__)


def is_effective_range_activated(
    activated_at: datetime, effective_range: Optional[model.DateRange]
) -> bool:
    """
    check if end of effective_range is before activated_at or not

    Parameter:
      activated_at: activate datetime
      effective_range: effective date range

    Returns:
      true - effective_range is None (no terminate date)
        or effective_range upper/ends is None (no terminate date)
        or activated before effective range ends
      false - activated after effective range ends
    """
    if effective_range is None or effective_range.upper is None:
        return True
    return activated_at.date() < effective_range.upper


@async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=1024)
async def is_cached_organization_active(
    organization_id: int, configs: configuration_client.Configurations
) -> bool:
    organization = await configs.get(organization_id)
    return is_organization_activated(organization)


@async_ttl_cache.AsyncTTLCache(time_to_live=30 * 60, max_size=1024)
async def cached_organization_eligibility_type(
    organization_id: int, configs: configuration_client.Configurations
) -> str | None:
    organization = await configs.get(organization_id)
    return organization.eligibility_type


def is_organization_activated(configuration: model.Configuration) -> bool:
    """Determine whether or not an organization is active
    This means that the org's activation date has passed, and the organization is not yet terminated

    Parameter:
        organization: Organization

    Returns:
        true - (organization has been activated AND has not been terminated) OR the termination date is in the future
        false - organization has NOT been activated OR was activated but has since been terminated
    """
    current_date = datetime.now()

    # Ensure that the organization has been activated
    if configuration.activated_at is None or configuration.activated_at > current_date:
        logger.info(
            "User attempted to register using an inactive organization",
            organization_id=configuration.organization_id,
        )
        return False

    # Ensure that the org has not yet been terminated
    if configuration.terminated_at and configuration.terminated_at <= current_date:
        logger.info(
            "User attempted to register using a terminated organization",
            organization_id=configuration.organization_id,
        )
        return False

    return True


async def check_member_org_active(
    configuration_client: configuration_client.Configurations(), member: MemberVersioned
) -> MemberVersioned:
    """
    check a single member if the its orgnization is active or not
    if active: return member
    if not active: raise error
    """
    if not await is_cached_organization_active(
        member.organization_id, configuration_client
    ):
        raise errors.MatchError("No active records found for user.")
    return member


async def check_member_org_active_and_single_org(
    configuration_client: configuration_client.Configurations(),
    member_list: List[MemberVersioned],
) -> model.MemberVersioned:
    """If we have received eligibility records that match a user's input, ensure the following:
    1) The records being considered are from 'active' organizations and
    2) If multiple records were found for a user, that they all are from a single org (i.e. not overeligible)

    """

    # Loop through our returned results - we only should consider those that are from an active organization
    encountered_orgs = defaultdict(list)
    for m in member_list:
        if await is_cached_organization_active(m.organization_id, configuration_client):
            encountered_orgs[m.organization_id].append(m)

    # Handle cases with more than one organization or no active records returned
    if len(encountered_orgs) == 0:
        raise errors.MatchError("No active records found for user.")
    elif len(encountered_orgs) > 1:
        organization_ids = list(encountered_orgs.keys())
        member_ids = list(m.id for m in member_list)
        # log to keep count on failures due to overeligibility
        logger.error(
            "Multiple organization records found for user.",
            organization_ids=organization_ids,
            member_ids=member_ids,
        )
        raise errors.MatchMultipleError("Multiple organization records found for user.")

    # If there are multiple matches in a single org, we want to use the most recent record
    member_list = list(encountered_orgs.values())[0]
    member_record = max(member_list, key=lambda x: x.created_at)
    return member_record


async def check_member_org_active_and_overeligibility(
    configuration_client: configuration_client.Configurations(),
    member_list: List[MemberVersioned],
) -> List[MemberVersioned]:
    """
    If we have received eligibility records that match a user's input, ensure the following:
    The records being considered are from 'active' organizations and
    If multiple records are returned after filtering, only one is returned per organization

    """

    # Loop through our returned results - we only should consider those that are from an active organization
    active_records = {}
    for r in member_list:
        if await is_cached_organization_active(r.organization_id, configuration_client):
            # Determine if we have examined records for this organization thusfar
            org_active_user = active_records.get(r.organization_id, None)

            # We have not seen any records for this org so far OR the current record is newer than the existing record
            if org_active_user is None or org_active_user.updated_at < r.updated_at:
                active_records[r.organization_id] = r

    active_records = list(active_records.values())

    # Raise an error if we don't have any valid records
    # TODO move this to middleware.py
    if len(active_records) == 0:
        raise errors.MatchError("No active records found for user.")

    return active_records


def is_verification_record_active(
    record: "model.EligibilityVerificationForUser",
) -> bool:
    """
    Check if the eligibility verification record is active.

    An eligibility verification record is considered active if:
    - It has an effective range with an open-ended upper bound (None), or
    - The current date is less than or equal to the upper bound of the effective range.

    Parameters:
    record (model.EligibilityVerificationForUser): The verification record to check.

    Returns:
    bool: True if the record is active, False otherwise.
    """
    if not record.effective_range:
        return False

    upper: Optional[date] = record.effective_range.upper
    current_date = date.today()

    # If there's no upper bound, the record is always active
    if upper is None:
        return True

    # Check if the current date is within the effective range
    return current_date <= upper
