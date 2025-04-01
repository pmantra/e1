import pytest
from tests.factories import data_models

from app.eligibility.populations import model as pop_model
from app.utils import eligibility_member
from db import model
from db.clients import member_versioned_client, population_client

pytestmark = pytest.mark.asyncio


async def test_get_member_attribute(test_file, member_versioned_test_client):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            work_state="NY",
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="work_state",
    )

    # Then
    assert attr_value == "NY"


@pytest.mark.parametrize(
    argnames="wallet_enabled,expected_value",
    argvalues=[
        (True, "true"),
        (False, "false"),
    ],
    ids=["wallet enabled", "wallet not enabled"],
)
async def test_get_member_attribute_bool(
    wallet_enabled, expected_value, test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            record={"wallet_enabled": wallet_enabled},
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="record.wallet_enabled",
    )

    # Then
    assert attr_value == expected_value


async def test_get_member_attribute_bool_not_set(
    test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            record={},
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="record.wallet_enabled",
    )

    # Then
    assert attr_value is None


async def test_get_member_attribute_unknown_attribute_returns_none(
    test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="play_state",
    )

    # Then
    assert attr_value is None


async def test_get_member_attribute_custom_attribute(
    test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            custom_attributes={"group_number": 38},
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="custom_attributes.group_number",
    )

    # Then
    assert attr_value == 38


async def test_get_member_attribute_custom_attribute_no_custom_attributes_returns_none(
    test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="custom_attributes.group_number",
    )

    # Then
    assert attr_value is None


async def test_get_member_attribute_custom_attribute_unknown_custom_attribute_returns_none(
    test_file, member_versioned_test_client
):
    # Given
    test_member = await member_versioned_test_client.persist(
        model=data_models.MemberVersionedFactory.create(
            organization_id=test_file.organization_id,
            file_id=test_file.id,
            custom_attributes={"group_number": 38},
        )
    )

    # When
    attr_value = eligibility_member.get_member_attribute(
        member=test_member,
        attribute_key="custom_attributes.group_numbest",
    )

    # Then
    assert attr_value is None


@pytest.mark.usefixtures("test_member_verification")
@pytest.mark.parametrize(
    argnames="work_state,custom_attributes,expected_sub_pop_id",
    argvalues=[
        ("NY", '{"employment_status": "Part", "group_number": "3"}', 203),
        ("YN", '{"employment_status": "Full"}', 404),
    ],
    ids=["NY-Part-3", "Default-Full-Null"],
)
async def test_get_advanced_sub_pop_id_for_member(
    work_state: str,
    custom_attributes: str,
    expected_sub_pop_id: int,
    test_member_versioned: model.MemberVersioned,
    mapped_advanced_population: pop_model.Population,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
):
    # Given
    test_member_versioned.work_state = work_state
    test_member_versioned.custom_attributes = custom_attributes
    await member_versioned_test_client.persist(model=test_member_versioned)

    # When
    returned_sub_pop_id = await eligibility_member.get_advanced_sub_pop_id_for_member(
        member=test_member_versioned,
        population_id=mapped_advanced_population.id,
        population_db_client=population_test_client,
    )

    # Then
    assert returned_sub_pop_id == expected_sub_pop_id


@pytest.mark.usefixtures("test_member_verification")
@pytest.mark.parametrize(
    argnames="record,expected_sub_pop_id",
    argvalues=[
        ({"wallet_enabled": True}, 101),
        ({"wallet_enabled": False}, 102),
        ({}, 103),
    ],
    ids=["wallet enabled", "wallet not enabled", "wallet not set"],
)
async def test_get_advanced_sub_pop_id_for_member_bool(
    record: dict,
    expected_sub_pop_id: int,
    test_member_versioned: model.MemberVersioned,
    mapped_populations_with_bool: pop_model.Population,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
):
    # Given
    test_member_versioned.record = record
    await member_versioned_test_client.persist(model=test_member_versioned)

    # When
    returned_sub_pop_id = await eligibility_member.get_advanced_sub_pop_id_for_member(
        member=test_member_versioned,
        population_id=mapped_populations_with_bool.id,
        population_db_client=population_test_client,
    )

    # Then
    assert returned_sub_pop_id == expected_sub_pop_id


@pytest.mark.usefixtures("test_member_verification")
async def test_get_advanced_sub_pop_id_for_member_no_population_returns_none(
    test_member_versioned: model.MemberVersioned,
    population_test_client: population_client.Populations,
):
    # Given
    # When
    returned_sub_pop_id = await eligibility_member.get_advanced_sub_pop_id_for_member(
        member=test_member_versioned,
        population_id=7357,
        population_db_client=population_test_client,
    )

    # Then
    assert returned_sub_pop_id is None


@pytest.mark.usefixtures("test_member_verification")
async def test_get_advanced_sub_pop_id_for_member_non_mapped_attribute_returns_none(
    test_member_versioned: model.MemberVersioned,
    mapped_advanced_population: pop_model.Population,
    member_versioned_test_client: member_versioned_client.MembersVersioned,
    population_test_client: population_client.Populations,
):
    # Given
    test_member_versioned.work_state = "NY"
    test_member_versioned.custom_attributes = {
        "employment_status": "Part",
        "group_number": "404",
    }
    await member_versioned_test_client.persist(model=test_member_versioned)

    # When
    returned_sub_pop_id = await eligibility_member.get_advanced_sub_pop_id_for_member(
        member=test_member_versioned,
        population_id=mapped_advanced_population.id,
        population_db_client=population_test_client,
    )

    # Then
    assert returned_sub_pop_id is None
