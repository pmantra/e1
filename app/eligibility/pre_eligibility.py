import datetime

from db import model


def is_active(member) -> bool:
    if not member:
        return False
    upper = member.effective_range.upper
    if not upper:
        return True
    current_date = datetime.date.today()
    if current_date < upper:
        return True
    return False


def has_potential_eligibility_in_current_org(
    member: model.Member, matching_records: [model.Member]
):
    """
    checks if member's existing e9y record is inactive but has active e9y records associated with the same org
    """
    org_id = member.organization_id
    if not is_active(member) and matching_records:
        for record in matching_records:
            if is_active(record) and org_id == record.organization_id:
                return True
    return False


def has_potential_eligibility_in_other_org(
    member: model.Member, matching_records: [model.Member]
):
    """
    checks if member's existing e9y record is inactive but has active e9y records associated with another org
    """
    org_id = member.organization_id
    # current e9y is expired and member has valid e9y record with other org
    if not is_active(member) and matching_records:
        for record in matching_records:
            if is_active(record) and org_id != record.organization_id:
                return True
    return False


def has_existing_eligibility(member: model.Member, matching_records: [model.Member]):
    """
    checks if member's existing e9y record is the only record found
    """
    return (
        is_active(member)
        and matching_records
        and len(matching_records) == 1
        and matching_records[0].id == member.id
    )
