import asyncpg

from db.model import EligibilityVerificationForUser

__all__ = ("convert_record_to_eligibility_verification_for_user",)


def convert_record_to_eligibility_verification_for_user(
    record: asyncpg.Record,
) -> EligibilityVerificationForUser:
    """
    converts the SQL record returned by the query to EligibilityVerificationForUser record
    @param record:
    @return: EligibilityVerificationForUser record
    """
    return EligibilityVerificationForUser(
        verification_id=record["verification_id"],
        user_id=record["user_id"],
        organization_id=record["organization_id"],
        eligibility_member_id=record["eligibility_member_id"],
        first_name=record["first_name"],
        last_name=record["last_name"],
        date_of_birth=record["date_of_birth"],
        unique_corp_id=record["unique_corp_id"],
        dependent_id=record["dependent_id"],
        work_state=record["work_state"],
        email=record["email"],
        record=record["record"],
        verification_type=record["verification_type"],
        employer_assigned_id=record["employer_assigned_id"],
        effective_range=record["effective_range"],
        verification_created_at=record["verification_created_at"],
        verification_updated_at=record["verification_updated_at"],
        verification_deactivated_at=record["verification_deactivated_at"],
        gender_code=record["gender_code"],
        do_not_contact=record["do_not_contact"],
        verified_at=record["verified_at"],
        additional_fields=record["additional_fields"],
        eligibility_member_version=record["eligibility_member_version"],
        verification_session=str(record["verification_session"])
        if "verification_session" in record and record["verification_session"]
        else None,
        is_v2=False,
        verification_1_id=record["verification_id"],
        verification_2_id=record["verification_2_id"],
        eligibility_member_2_id=record["eligibility_member_2_id"],
        eligibility_member_2_version=record["eligibility_member_2_version"],
    )
