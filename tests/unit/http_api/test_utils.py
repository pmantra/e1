import uuid
from datetime import date, datetime, timezone

import pytest
from http_api.client.utils import (
    convert_to_bool,
    create_member_response,
    create_verification_for_user_response,
)
from tests.test_utils import generate_random_int, generate_random_string

from db.model import DateRange, EligibilityVerificationForUser, MemberResponse


def test_convert_to_bool():
    assert convert_to_bool(True)
    assert not convert_to_bool(False)

    assert convert_to_bool("TrUe")
    assert convert_to_bool("True")
    assert convert_to_bool("TRUE")

    assert not convert_to_bool("FalsE")
    assert not convert_to_bool("False")
    assert not convert_to_bool("FALSE")

    with pytest.raises(Exception) as exc_info:
        convert_to_bool("1")
    assert "Invalid literal for boolean" in str(exc_info.value)


def test_create_verification_for_user_response():
    verification_id = generate_random_int(5)
    user_id = generate_random_int(5)
    organization_id = generate_random_int(10)
    eligibility_member_id = generate_random_int(10)
    first_name = generate_random_string(5)
    last_name = generate_random_string(5)
    unique_corp_id = generate_random_string(5)
    dependent_id = generate_random_string(5)
    work_state = generate_random_string(2)
    work_country = generate_random_string(5)
    email = generate_random_string(15)

    record_val1 = generate_random_string(10)
    record_val2 = generate_random_int(5)
    record = {
        "key1": record_val1,
        "key2": record_val2,
    }

    custom_attribute_val1 = generate_random_string(10)
    custom_attribute_val2 = generate_random_int(5)
    custom_attributes = {
        "key1": custom_attribute_val1,
        "key2": custom_attribute_val2,
    }

    verification_type = generate_random_string(10)
    employer_assigned_id = generate_random_string(10)
    effective_range = DateRange(
        lower=None, upper=date(2022, 11, 2), lower_inc=True, upper_inc=True
    )
    gender_code = generate_random_string(10)
    do_not_contact = generate_random_string(10)
    verification_session = uuid.uuid4()
    eligibility_member_version = generate_random_int(1)
    is_v2 = True
    verification_1_id = generate_random_int(1)
    verification_2_id = generate_random_int(1)
    eligibility_member_2_id = generate_random_int(10)
    eligibility_member_2_version = generate_random_int(1)

    eligibility_verification_for_user = EligibilityVerificationForUser(
        verification_id=verification_id,
        user_id=user_id,
        organization_id=organization_id,
        eligibility_member_id=eligibility_member_id,
        first_name=first_name,
        last_name=last_name,
        date_of_birth=date(1990, 3, 24),
        unique_corp_id=unique_corp_id,
        dependent_id=dependent_id,
        work_state=work_state,
        work_country=work_country,
        email=email,
        record=record,
        custom_attributes=custom_attributes,
        verification_type=verification_type,
        employer_assigned_id=employer_assigned_id,
        verification_created_at=date(1990, 1, 1),
        verification_updated_at=date(1990, 1, 1),
        verification_deactivated_at=None,
        verified_at=date(1990, 2, 1),
        additional_fields=None,
        gender_code=gender_code,
        do_not_contact=do_not_contact,
        verification_session=verification_session,
        eligibility_member_version=eligibility_member_version,
        is_v2=is_v2,
        verification_1_id=verification_1_id,
        verification_2_id=verification_2_id,
        eligibility_member_2_id=eligibility_member_2_id,
        eligibility_member_2_version=eligibility_member_2_version,
        effective_range=effective_range,
    )

    output = create_verification_for_user_response(eligibility_verification_for_user)
    assert output.get("verification_id") == verification_id
    assert output.get("user_id") == user_id
    assert output.get("organization_id") == organization_id
    assert output.get("eligibility_member_id") == str(eligibility_member_id)
    assert output.get("first_name") == first_name
    assert output.get("last_name") == last_name
    assert output.get("date_of_birth") == "1990-03-24"
    assert output.get("unique_corp_id") == unique_corp_id
    assert output.get("dependent_id") == dependent_id
    assert output.get("work_state") == work_state
    assert output.get("email") == email
    assert output.get("verification_type") == verification_type
    assert output.get("employer_assigned_id") == employer_assigned_id
    assert output.get("verification_created_at") == "1990-01-01"
    assert output.get("verification_updated_at") == "1990-01-01"
    assert output.get("verification_deactivated_at") is None
    assert output.get("verified_at") == "1990-02-01"
    assert output.get("gender_code") == gender_code
    assert output.get("do_not_contact") == do_not_contact
    assert output.get("additional_fields") == "{}"
    assert output.get("verification_session") == str(verification_session)
    assert output.get("eligibility_member_version") == str(eligibility_member_version)
    assert output.get("is_v2") is True
    assert output.get("verification_1_id") == verification_1_id
    assert output.get("verification_2_id") == verification_2_id
    assert output.get("eligibility_member_2_id") == str(eligibility_member_2_id)
    assert output.get("eligibility_member_2_version") == str(
        eligibility_member_2_version
    )

    assert f'"{record_val1}"' in output.get("record")
    assert str(record_val2) in output.get("record")

    assert output.get("effective_range") == {
        "lower": None,
        "upper": "2022-11-02",
        "lower_inc": False,
        "upper_inc": True,
    }


def test_create_member_response():
    member = MemberResponse(
        id=2915,
        version=0,
        organization_id=109,
        first_name="Test",
        last_name="Email",
        date_of_birth=date(1990, 3, 24),
        file_id=7,
        work_state="",
        work_country=None,
        email="TestEmailio5828@test.com",
        unique_corp_id="5828",
        employer_assigned_id=None,
        dependent_id="",
        effective_range=DateRange(
            lower=None, upper=date(2022, 11, 2), lower_inc=True, upper_inc=True
        ),
        record={
            "key": "record:persist:109:7:5387caf5-339e-4961-a021-04acda10a3d1",
            "email": "TestEmailio5828@test.com",
            "errors": "",
            "file_id": 7,
            "warnings": "",
            "last_name": "Email",
            "first_name": "Test",
            "date_of_birth": "1990-03-24",
            "unique_corp_id": "5828",
            "organization_id": 109,
        },
        custom_attributes=None,
        do_not_contact=None,
        gender_code=None,
        created_at=datetime(2022, 8, 11, 23, 45, 40, 2059, tzinfo=timezone.utc),
        updated_at=datetime(2022, 11, 2, 21, 36, 30, 981791, tzinfo=timezone.utc),
        is_v2=False,
        member_1_id=2915,
        member_2_id=None,
    )

    response = create_member_response(member)

    expected_response = {
        "id": 2915,
        "version": 0,
        "organization_id": 109,
        "first_name": "Test",
        "last_name": "Email",
        "date_of_birth": "1990-03-24",
        "file_id": 7,
        "work_state": "",
        "work_country": None,
        "email": "TestEmailio5828@test.com",
        "unique_corp_id": "5828",
        "employer_assigned_id": None,
        "dependent_id": "",
        "effective_range": {
            "lower": None,
            "upper": "2022-11-02",
            "lower_inc": False,
            "upper_inc": True,
        },
        "record": {
            "key": "record:persist:109:7:5387caf5-339e-4961-a021-04acda10a3d1",
            "email": "TestEmailio5828@test.com",
            "errors": "",
            "file_id": 7,
            "warnings": "",
            "last_name": "Email",
            "first_name": "Test",
            "date_of_birth": "1990-03-24",
            "unique_corp_id": "5828",
            "organization_id": 109,
        },
        "custom_attributes": None,
        "do_not_contact": None,
        "gender_code": None,
        "created_at": "2022-08-11T23:45:40.002059+00:00",
        "updated_at": "2022-11-02T21:36:30.981791+00:00",
        "is_v2": False,
        "member_1_id": 2915,
        "member_2_id": None,
    }

    assert expected_response == response
