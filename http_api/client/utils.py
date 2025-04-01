from __future__ import annotations

import json
from dataclasses import asdict

from db.model import EligibilityVerificationForUser, MemberResponse


def convert_to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):  # Return if it's already a boolean
        return value

    true_values = {"true"}
    false_values = {"false"}

    lower_value = value.strip().lower()
    if lower_value in true_values:
        return True
    elif lower_value in false_values:
        return False
    else:
        raise ValueError(f"Invalid literal for boolean: {value}")


def create_member_response(member: MemberResponse) -> dict:
    data = asdict(member)

    # Convert date and datetime fields to ISO format for JSON compatibility
    if data["date_of_birth"]:
        data["date_of_birth"] = data["date_of_birth"].isoformat()
    if data["created_at"]:
        data["created_at"] = data["created_at"].isoformat()
    if data["updated_at"]:
        data["updated_at"] = data["updated_at"].isoformat()

    if data["effective_range"]:
        lower = data["effective_range"].lower
        upper = data["effective_range"].upper
        data["effective_range"] = {
            "lower": lower and lower.isoformat(),
            "upper": upper and upper.isoformat(),
            "lower_inc": data["effective_range"].lower_inc,
            "upper_inc": data["effective_range"].upper_inc,
        }

    return data


def create_verification_for_user_response(
    eligibility_verification_for_user: EligibilityVerificationForUser,
) -> dict:
    if eligibility_verification_for_user.verification_created_at:
        verification_created_at = (
            eligibility_verification_for_user.verification_created_at.isoformat()
        )
    else:
        verification_created_at = None

    if eligibility_verification_for_user.verification_updated_at:
        verification_updated_at = (
            eligibility_verification_for_user.verification_updated_at.isoformat()
        )
    else:
        verification_updated_at = None

    if eligibility_verification_for_user.verification_deactivated_at:
        verification_deactivated_at = (
            eligibility_verification_for_user.verification_deactivated_at.isoformat()
        )
    else:
        verification_deactivated_at = None

    if eligibility_verification_for_user.verified_at:
        verified_at = eligibility_verification_for_user.verified_at.isoformat()
    else:
        verified_at = None

    if eligibility_verification_for_user.date_of_birth:
        date_of_birth = eligibility_verification_for_user.date_of_birth.isoformat()
    else:
        date_of_birth = None

    effective_range = None
    if eligibility_verification_for_user.effective_range:
        lower = eligibility_verification_for_user.effective_range.lower
        upper = eligibility_verification_for_user.effective_range.upper
        effective_range = {
            "lower": lower and lower.isoformat(),
            "upper": upper and upper.isoformat(),
            "lower_inc": eligibility_verification_for_user.effective_range.lower_inc,
            "upper_inc": eligibility_verification_for_user.effective_range.upper_inc,
        }

    # If null, cast to blank string- otherwise will us "NONE" as the return value
    if eligibility_verification_for_user.eligibility_member_id is None:
        eligibility_member_id = ""
    else:
        eligibility_member_id = str(
            eligibility_verification_for_user.eligibility_member_id
        )

    if eligibility_verification_for_user.eligibility_member_version is None:
        eligibility_member_version = ""
    else:
        eligibility_member_version = str(
            eligibility_verification_for_user.eligibility_member_version
        )

    if eligibility_verification_for_user.eligibility_member_2_id is None:
        eligibility_member_2_id = ""
    else:
        eligibility_member_2_id = str(
            eligibility_verification_for_user.eligibility_member_2_id
        )

    if eligibility_verification_for_user.eligibility_member_2_version is None:
        eligibility_member_2_version = ""
    else:
        eligibility_member_2_version = str(
            eligibility_verification_for_user.eligibility_member_2_version
        )

    # Cast our record to a string
    if eligibility_verification_for_user.record:
        record = json.dumps(eligibility_verification_for_user.record)
    else:
        record = "{}"

    # Cast additional fields to a string
    if eligibility_verification_for_user.additional_fields:
        additional_fields = json.dumps(
            eligibility_verification_for_user.additional_fields
        )
    else:
        additional_fields = "{}"

    verification_session = ""
    if eligibility_verification_for_user.verification_session:
        verification_session = str(
            eligibility_verification_for_user.verification_session
        )

    return {
        "verification_id": eligibility_verification_for_user.verification_id,
        "user_id": eligibility_verification_for_user.user_id,
        "organization_id": eligibility_verification_for_user.organization_id,
        "eligibility_member_id": eligibility_member_id,
        "first_name": eligibility_verification_for_user.first_name,
        "last_name": eligibility_verification_for_user.last_name,
        "date_of_birth": date_of_birth,
        "unique_corp_id": eligibility_verification_for_user.unique_corp_id,
        "dependent_id": eligibility_verification_for_user.dependent_id,
        "work_state": eligibility_verification_for_user.work_state,
        "email": eligibility_verification_for_user.email,
        "record": record,
        "verification_type": eligibility_verification_for_user.verification_type,
        "employer_assigned_id": eligibility_verification_for_user.employer_assigned_id,
        "effective_range": effective_range,
        "verification_created_at": verification_created_at,
        "verification_updated_at": verification_updated_at,
        "verification_deactivated_at": verification_deactivated_at,
        "gender_code": eligibility_verification_for_user.gender_code,
        "do_not_contact": eligibility_verification_for_user.do_not_contact,
        "verified_at": verified_at,
        "additional_fields": additional_fields,
        "verification_session": verification_session,
        "eligibility_member_version": eligibility_member_version,
        "is_v2": eligibility_verification_for_user.is_v2,
        "verification_1_id": eligibility_verification_for_user.verification_1_id,
        "verification_2_id": eligibility_verification_for_user.verification_2_id,
        "eligibility_member_2_id": eligibility_member_2_id,
        "eligibility_member_2_version": eligibility_member_2_version,
    }
