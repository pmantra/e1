from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest

from app.eligibility.populations import model as pop_model
from app.tasks import calculate_sub_populations
from db.clients import postgres_connector

pytestmark = pytest.mark.asyncio


@pytest.fixture
def lookup_info() -> Tuple[List[str], Dict[str, Dict | int] | int]:
    """
    This fixture contains a lookup_keys_list and a lookup_map as a
    single tuple.

    The following is the data contained in the lookup_map organized by
    attribute and by sub-population:

    attribute values:
      attr_1:
        attr_1_val_1
        attr_1_val_2
      attr_2:
        attr_2_val_1
        attr_2_val_2
      attr_3:
        attr_3_val_1
        pop_model.SpecialCaseAttributes.IS_NULL
        pop_model.SpecialCaseAttributes.DEFAULT_CASE

    sub-populations:
      1:
         attr_1_val_1 -> attr_2_val_1 -> attr_3_val_1
         attr_1_val_1 -> attr_2_val_2 -> pop_model.SpecialCaseAttributes.IS_NULL
         attr_1_val_2 -> attr_2_val_1 -> pop_model.SpecialCaseAttributes.DEFAULT_CASE
      2:
         attr_1_val_1 -> attr_2_val_1 -> pop_model.SpecialCaseAttributes.IS_NULL
         attr_1_val_1 -> attr_2_val_2 -> attr_3_val_1
         attr_1_val_1 -> attr_2_val_2 -> pop_model.SpecialCaseAttributes.DEFAULT_CASE
         attr_1_val_2 -> attr_2_val_1 -> pop_model.SpecialCaseAttributes.IS_NULL
         attr_1_val_2 -> attr_2_val_2 -> attr_3_val_1
         attr_1_val_2 -> attr_2_val_2 -> pop_model.SpecialCaseAttributes.DEFAULT_CASE
      3:
         attr_1_val_1 -> attr_2_val_1 -> pop_model.SpecialCaseAttributes.DEFAULT_CASE
         attr_1_val_2 -> attr_2_val_1 -> attr_3_val_1
         attr_1_val_2 -> attr_2_val_2 -> pop_model.SpecialCaseAttributes.IS_NULL
    """
    # Attributes that hold the special cases if with_special_cases is True
    return (
        # lookup_keys_list
        ["attr_1", "attr_2", "attr_3"],
        # lookup_map
        {
            "attr_1_val_1": {
                "attr_2_val_1": {
                    "attr_3_val_1": 1,
                    pop_model.SpecialCaseAttributes.IS_NULL: 2,
                    pop_model.SpecialCaseAttributes.DEFAULT_CASE: 3,
                },
                "attr_2_val_2": {
                    "attr_3_val_1": 2,
                    pop_model.SpecialCaseAttributes.IS_NULL: 1,
                    pop_model.SpecialCaseAttributes.DEFAULT_CASE: 2,
                },
            },
            "attr_1_val_2": {
                "attr_2_val_1": {
                    "attr_3_val_1": 3,
                    pop_model.SpecialCaseAttributes.IS_NULL: 2,
                    pop_model.SpecialCaseAttributes.DEFAULT_CASE: 1,
                },
                "attr_2_val_2": {
                    "attr_3_val_1": 2,
                    pop_model.SpecialCaseAttributes.IS_NULL: 3,
                    pop_model.SpecialCaseAttributes.DEFAULT_CASE: 2,
                },
            },
        },
    )


async def mock_add_sub_population_criteria_count(
    sub_population_id: int,
    query_criteria: str,
    no_op: bool = True,
    connector: Optional[postgres_connector.PostgresConnector] = None,
    context: Dict[str, Any] = {},
) -> None:
    """
    This mock function will add one to the sub-population count each time it is
    called, allowing tests to determine if each sub-population criteria leaf
    was reached.
    """
    sub_population_counts = context.get("sub_population_counts", {})
    sub_population_counts[sub_population_id] = (
        sub_population_counts.get(sub_population_id, 0) + 1
    )


async def test_process_lookup_sub_map(
    lookup_info: Tuple[List[str], Dict[str, Dict | int] | int]
):
    # Given
    lookup_keys_list, lookup_map = lookup_info
    sub_population_counts = {}

    # When
    with mock.patch(
        "app.tasks.calculate_sub_populations._add_sub_population_criteria_count"
    ) as mock_add_count:
        mock_add_count.side_effect = mock_add_sub_population_criteria_count
        ret_val = await calculate_sub_populations._process_lookup_sub_map(
            lookup_keys_list=lookup_keys_list,
            lookup_map=lookup_map,
            query_criteria="",
            final_processor=calculate_sub_populations._add_sub_population_criteria_count,
            context={"sub_population_counts": sub_population_counts},
        )

    # Then
    # Processing is successful and the counts are filled in (1 per set of criteria)
    assert ret_val is True
    assert sub_population_counts.get(1, 0) == 3
    assert sub_population_counts.get(2, 0) == 6
    assert sub_population_counts.get(3, 0) == 3


@pytest.mark.parametrize(
    argnames="replacement_lookup_keys_list",
    argvalues=[
        [],
        ["too_short"],
        ["this", "is", "too", "long"],
        ["this", "is", "way", "way", "too", "long"],
    ],
    ids=[
        "empty_list",
        "too_short",
        "too_long",
        "way_way_too_long",
    ],
)
async def test_process_lookup_sub_map_fail(
    lookup_info: Tuple[List[str], Dict[str, Dict | int] | int],
    replacement_lookup_keys_list: List[str],
):
    # Given
    lookup_keys_list, lookup_map = lookup_info
    lookup_keys_list = replacement_lookup_keys_list
    sub_population_counts = {}

    # When
    with mock.patch(
        "app.tasks.calculate_sub_populations._add_sub_population_criteria_count"
    ) as mock_add_count:
        mock_add_count.side_effect = mock_add_sub_population_criteria_count
        ret_val = await calculate_sub_populations._process_lookup_sub_map(
            lookup_keys_list=lookup_keys_list,
            lookup_map=lookup_map,
            query_criteria="",
            final_processor=calculate_sub_populations._add_sub_population_criteria_count,
            context={"sub_population_counts": sub_population_counts},
        )

    # Then
    # Processing is unsuccessful
    assert ret_val is False


@pytest.mark.parametrize(
    argnames="sub_population_criteria_count, num_criteria_sets",
    argvalues=[
        (0, 1),
        (0, 3),
        (8, 1),
        (8, 3),
        (99, 0),
    ],
    ids=[
        "zero_count_once",
        "zero_count_thrice",
        "eight_count_once",
        "eight_count_thrice",
        "ninety-nine_count_never",
    ],
)
async def test_add_sub_population_criteria_count(
    sub_population_criteria_count,
    num_criteria_sets,
    members_versioned,
):
    # Given
    sub_population_id = 1
    sub_population_counts = {}
    query_method = getattr(members_versioned, "get_count_for_sub_population_criteria")
    query_method.return_value = sub_population_criteria_count

    # When
    for _ in range(num_criteria_sets):
        await calculate_sub_populations._add_sub_population_criteria_count(
            sub_population_id=sub_population_id,
            query_criteria="",
            context={"sub_population_counts": sub_population_counts},
        )

    # Then
    assert sub_population_counts.get(sub_population_id, 0) == (
        sub_population_criteria_count * num_criteria_sets
    )


@pytest.mark.parametrize(
    argnames="criteria,is_equality_comparison,current_query_criteria,expected_output",
    argvalues=[
        # append_equals_none_to_empty_query_criteria
        (
            [("attr_1", pop_model.SpecialCaseAttributes.IS_NULL)],
            True,
            "",
            "attr_1 IS NULL",
        ),
        # append_not_equals_none_to_empty_query_criteria
        (
            [("attr_1", pop_model.SpecialCaseAttributes.IS_NULL)],
            False,
            "",
            "attr_1 IS NOT NULL",
        ),
        # append_equals_multiple_to_empty_query_criteria
        (
            [
                ("attr_1", "value_1"),
                ("attr_2", "value_2"),
            ],
            True,
            "",
            "attr_1 = 'value_1' AND attr_2 = 'value_2'",
        ),
        # append_not_equals_multiple_to_empty_query_criteria
        (
            [
                ("attr_1", "value_1"),
                ("attr_2", "value_2"),
            ],
            False,
            "",
            "attr_1 != 'value_1' AND attr_2 != 'value_2'",
        ),
        # append_equals_multiple_with_none_to_existing_query_criteria
        (
            [
                ("attr_1", "value_1"),
                ("attr_2", pop_model.SpecialCaseAttributes.IS_NULL),
            ],
            True,
            "attr_0 = 'value_0'",
            "attr_0 = 'value_0' AND attr_1 = 'value_1' AND attr_2 IS NULL",
        ),
        # append_not_equals_multiple_with_none_to_existing_query_criteria
        (
            [
                ("attr_1", "value_1"),
                ("attr_2", pop_model.SpecialCaseAttributes.IS_NULL),
            ],
            False,
            "attr_0 = 'value_0'",
            "attr_0 = 'value_0' AND attr_1 != 'value_1' AND attr_2 IS NOT NULL",
        ),
    ],
    ids=[
        "append_equals_none_to_empty_query_criteria",
        "append_not_equals_none_to_empty_query_criteria",
        "append_equals_multiple_to_empty_query_criteria",
        "append_not_equals_multiple_to_empty_query_criteria",
        "append_equals_multiple_with_none_to_existing_query_criteria",
        "append_not_equals_multiple_with_none_to_existing_query_criteria",
    ],
)
def test_append_sub_population_criteria_to_query_criteria(
    criteria: List[Tuple[str, str]],
    is_equality_comparison: bool,
    current_query_criteria: str,
    expected_output: str,
):
    # When/Then
    assert (
        calculate_sub_populations._append_sub_population_criteria_to_query_criteria(
            criteria=criteria,
            is_equality_comparison=is_equality_comparison,
            current_query_criteria=current_query_criteria,
        )
        == expected_output
    )


@pytest.mark.parametrize(
    argnames="input_attribute_name,expected_output_attribute_name",
    argvalues=[
        ("", ""),
        ("test", "test"),
        ("test.this", "test->>'this'"),
        ("test.this.too", "test->'this'->>'too'"),
        ("test.this.and.this", "test->'this'->'and'->>'this'"),
    ],
    ids=["empty_string", "one_part", "two_parts", "three_parts", "four_parts"],
)
def test_translate_dotted_notation_attribute_key(
    input_attribute_name,
    expected_output_attribute_name,
):
    assert (
        calculate_sub_populations._translate_dotted_notation_attribute_key(
            input_attribute_name
        )
        == expected_output_attribute_name
    )
