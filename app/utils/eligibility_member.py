from __future__ import annotations

import orjson
from mmlib.ops import log

from app.eligibility.populations import model as e9y_model
from db import model as db_model
from db.clients import population_client

logger = log.getLogger(__name__)


def get_member_attribute(
    member: db_model.MemberVersioned,
    attribute_key: str,
) -> str | None:
    """
    Gets the member's attribute value for the attribute_key. The
    attribute_key supports dotted notation to allow traversing into
    the member object.
    """
    temp_value = member
    key_components = attribute_key.split(".")
    for key_component in key_components:
        if isinstance(temp_value, str):
            temp_value = orjson.loads(temp_value)
        elif not isinstance(temp_value, dict):
            temp_value = temp_value.__dict__
        temp_value = temp_value.get(key_component, None)
        # bug fix for boolean values search, ELIG-2275
        if type(temp_value) is bool:
            temp_value = "true" if temp_value else "false"
        if temp_value is None:
            return None

    return temp_value


async def get_advanced_sub_pop_id_for_member(
    member: db_model.MemberVersioned | db_model.Member2,
    population_id: int,
    population_db_client: population_client.Populations | None = None,
) -> int | None:
    """
    Gets the sub-population ID of advanced populations by taking into consideration
    the special cases of NULL and default values.
    """
    if population_db_client is None:
        population_db_client = population_client.Populations()

    population: e9y_model.Population = await population_db_client.get(pk=population_id)
    if population is None:
        return None

    # Get the lookup keys to walk through
    lookup_keys = population.sub_pop_lookup_keys_csv.split(",")
    # Get a pointer to the lookup map to walk through it key by key
    lookup_map_ptr = population.sub_pop_lookup_map_json

    attribute_sequence = ""
    for key in lookup_keys:
        # Get the member attribute value for the given key
        member_attribute = get_member_attribute(member, key)
        # If there is no value, set it to the special case of IS_NULL
        if member_attribute is None:
            member_attribute = e9y_model.SpecialCaseAttributes.IS_NULL

        # Get the map value based on the attribute value, filling in the DEFAULT_CASE
        # as the default value, returning None if there is no DEFAULT_CASE
        attribute_sequence = f"{attribute_sequence},{member_attribute}"
        lookup_map_ptr = lookup_map_ptr.get(
            member_attribute,
            lookup_map_ptr.get(
                e9y_model.SpecialCaseAttributes.DEFAULT_CASE,
                None,
            ),
        )

        # If there was no mapped value found, return None
        if lookup_map_ptr is None:
            logger.error(
                f"Attribute combination not covered by population definition: {attribute_sequence}",
                member_id=member.id,
                population_id=population_id,
            )
            return None

    # If the pointer is not currently pointing to a string, return None since it is
    # not at the value we are looking for
    if not isinstance(lookup_map_ptr, int):
        logger.error(
            f"Attribute combination retrieved unexpected value: {attribute_sequence}: {lookup_map_ptr}",
            member_id=member.id,
            population_id=population_id,
        )
        return None
    return lookup_map_ptr
