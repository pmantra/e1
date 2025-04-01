import datetime

import pytest
from tests.factories import data_models

import app.eligibility.pre_eligibility
from app.eligibility import translate
from db import model
from db.mono.client import MavenOrganization


@pytest.mark.parametrize(
    argnames="org,expected_config,expected_headers",
    argvalues=[
        (  # No headers from mono
            MavenOrganization(id=1, name="Foo", directory_name="foo", json={}),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(),
        ),
        (  # medical_plan_only config
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={},
                employee_only=0,
                medical_plan_only=1,
            ),
            model.Configuration(
                organization_id=1,
                directory_name="foo",
                employee_only=False,
                medical_plan_only=True,
            ),
            model.HeaderMapping(),
        ),
        (  # No headers from mono with activated_at
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={},
                activated_at=datetime.datetime(2023, 5, 15),
            ),
            model.Configuration(
                organization_id=1,
                directory_name="foo",
                activated_at=datetime.datetime(2023, 5, 15),
            ),
            model.HeaderMapping(),
        ),
        # No headers from mono, no directory name
        (
            MavenOrganization(id=1, name="Foo", directory_name=None, json={}),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(),
        ),
        # Headers from mono w invalid values
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={translate.FIELD_MAP_KEY: {"some_header": "", "": "some_alias"}},
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(),
        ),
        # Headers from mono, non default mapping
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={translate.FIELD_MAP_KEY: {"date_of_birth": "dob"}},
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(date_of_birth="dob"),
        ),
        # Legacy headers from mono
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={translate.FIELD_MAP_KEY: {"employee_first_name": "first name"}},
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(first_name="first name"),
        ),
        # Headers include optional affiliation headers - use default headers
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={
                    translate.FIELD_MAP_KEY: {"date_of_birth": "date_of_birth"},
                    translate.AFFILIATIONS_FIELD_MAP: {"client_id": "client_id"},
                },
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(),
        ),
        # Headers include optional affiliation headers - do not use default values
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={
                    translate.FIELD_MAP_KEY: {"date_of_birth": "date_of_birth"},
                    translate.AFFILIATIONS_FIELD_MAP: {
                        "client_id": "client identifier"
                    },
                },
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(client_id="client identifier"),
        ),
        (  # No headers from mono with activated_at and terminated_at
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={},
                activated_at=datetime.datetime(2023, 5, 15),
                terminated_at=datetime.datetime(2023, 5, 20),
            ),
            model.Configuration(
                organization_id=1,
                directory_name="foo",
                activated_at=datetime.datetime(2023, 5, 15),
                terminated_at=datetime.datetime(2023, 5, 20),
            ),
            model.HeaderMapping(),
        ),
        # Headers with custom attribute headers
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={
                    translate.CUSTOM_ATTRIBUTES_FIELD_MAP: {
                        "custom_attributes.employment_status": "employment_status",
                        "custom_attributes.group_number": "group_number",
                    },
                },
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(
                [
                    ("custom_attributes.employment_status", "employment_status"),
                    ("custom_attributes.group_number", "group_number"),
                ]
            ),
        ),
        # Health plan field maps
        (
            MavenOrganization(
                id=1,
                name="Foo",
                directory_name="foo",
                json={
                    translate.FIELD_MAP_KEY: {"date_of_birth": "date_of_birth"},
                    translate.HEALTH_PLAN_FIELD_MAP: {
                        "client_name": "client name",
                        "fertility_indicator": "foobar",
                    },
                },
            ),
            model.Configuration(organization_id=1, directory_name="foo"),
            model.HeaderMapping(
                client_name="client name", fertility_indicator="foobar"
            ),
        ),
    ],
    ids=[
        "no-headers",
        "org-config",
        "no-headers-with-activation",
        "no-headers-no-directory",
        "invalid-header-values",
        "non-default-header-values",
        "legacy-headers",
        "default-affiliation-headers",
        "non-default-affiliation-header",
        "no-headers-with-activation-and-termination",
        "headers-with-custom-attribute-headers",
        "health-headers-with-custom-values",
    ],
)
def test_org_to_config(
    org: MavenOrganization,
    expected_config: model.Configuration,
    expected_headers: model.HeaderMapping,
):
    assert translate.org_to_config(org) == (expected_config, expected_headers)


@pytest.mark.parametrize(
    argnames="row,member",
    argvalues=[
        (
            {"organization_id": 1, "file_id": 3},
            model.Member(
                organization_id=1,
                first_name="",
                last_name="",
                date_of_birth="",
                work_state="",
                file_id=3,
                record={"organization_id": 1, "file_id": 3},
            ),
        ),
        (
            {"organization_id": 1, "file_id": 3, "date_of_birth": "1970-01-01"},
            model.Member(
                organization_id=1,
                first_name="",
                last_name="",
                date_of_birth=datetime.date(1970, 1, 1),
                work_state="",
                file_id=3,
                record={
                    "organization_id": 1,
                    "file_id": 3,
                    "date_of_birth": "1970-01-01",
                },
            ),
        ),
    ],
)
def test_row_to_member(row, member):
    assert translate.row_to_member(row) == member


present_date = datetime.datetime.utcnow().date()
past_date = present_date - datetime.timedelta(days=10)
future_date = present_date + datetime.timedelta(days=10)
active_member = data_models.MemberFactory.create(
    id=1,
    organization_id=1,
    record={"record_source": "census"},
    effective_range=model.DateRange(upper=future_date),
)
inactive_member_same_org = data_models.MemberFactory.create(
    id=1,
    organization_id=1,
    record={"record_source": "census"},
    effective_range=model.DateRange(upper=past_date),
)
inactive_member_different_org = data_models.MemberFactory.create(
    id=1,
    organization_id=2,
    record={"record_source": "census"},
    effective_range=model.DateRange(upper=past_date),
)


@pytest.mark.parametrize(
    argnames="effective_range_upper, expected",
    argvalues=[
        (None, True),
        (past_date, False),
        (future_date, True),
    ],
    ids=[
        "effective_range_upper_none",
        "effective_range_upper_past",
        "effective_range_upper_future",
    ],
)
def test_is_active(effective_range_upper, expected):
    # Given
    test_member = data_models.MemberFactory.create(
        id=1,
        organization_id=1,
        record={"record_source": "census"},
        effective_range=model.DateRange(upper=effective_range_upper),
    )

    # When/Then
    assert app.eligibility.pre_eligibility.is_active(test_member) == expected


@pytest.mark.parametrize(
    argnames="member, matching_records, expected",
    argvalues=[
        (active_member, [active_member], False),
        (inactive_member_same_org, [active_member], True),
        (inactive_member_different_org, [active_member], False),
    ],
    ids=[
        "active_member",
        "inactive_member_same_org",
        "inactive_member_different_org",
    ],
)
def test_has_potential_eligibility_in_current_org(member, matching_records, expected):
    assert (
        app.eligibility.pre_eligibility.has_potential_eligibility_in_current_org(
            member, matching_records
        )
        == expected
    )


@pytest.mark.parametrize(
    argnames="member, matching_records, expected",
    argvalues=[
        (active_member, [active_member], False),
        (inactive_member_same_org, [active_member], False),
        (inactive_member_different_org, [active_member], True),
    ],
    ids=[
        "active_member",
        "inactive_member_same_org",
        "inactive_member_different_org",
    ],
)
def test_has_potential_eligibility_in_other_org(member, matching_records, expected):
    assert (
        app.eligibility.pre_eligibility.has_potential_eligibility_in_other_org(
            member, matching_records
        )
        == expected
    )


@pytest.mark.parametrize(
    argnames="member, matching_records, expected",
    argvalues=[
        (active_member, [active_member], True),
        (inactive_member_same_org, [active_member], False),
        (inactive_member_different_org, [active_member], False),
    ],
    ids=[
        "active_member",
        "inactive_member_same_org",
        "inactive_member_different_org",
    ],
)
def test_has_existing_eligibility(member, matching_records, expected):
    assert (
        app.eligibility.pre_eligibility.has_existing_eligibility(
            member, matching_records
        )
        == expected
    )
