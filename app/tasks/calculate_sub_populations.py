from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

from app.eligibility.populations import model as pop_model
from app.eligibility.populations.repository import (
    member_sub_population as member_sub_pop_repo,
)
from db.clients import member_versioned_client, population_client, postgres_connector

logger = structlog.get_logger(__name__)


async def main(
    org_ids: Optional[List[int]] = None,
    no_op: bool = True,
):
    await get_sub_population_counts(
        org_ids=org_ids,
        no_op=no_op,
    )


async def get_sub_population_counts(
    org_ids: Optional[List[int]] = None,
    no_op: bool = True,
):
    sub_population_counts = {}
    await process_sub_populations(
        org_ids=org_ids,
        no_op=no_op,
        final_processor=_add_sub_population_criteria_count,
        context={"sub_population_counts": sub_population_counts},
    )

    # Final counts
    logger.info(f"Final counts: {sub_population_counts}")
    if not no_op:
        # TODO: Record final counts to DB
        pass


async def _add_sub_population_criteria_count(
    sub_population_id: int,
    query_criteria: str,
    no_op: bool = True,
    connector: Optional[postgres_connector.PostgresConnector] = None,
    context: Dict[str, Any] = {},
) -> None:
    """
    This function calls get_count_for_sub_population_criteria using the supplied
    query and adds it to a running count that's stored in a dictionary, which
    is keyed by the sub_population_id. This allows us to process each set of
    criteria separately to come up with the final counts for each sub-population.
    """
    sub_population_counts = context.get("sub_population_counts", {})
    # Run the query to get the count
    member_versioned = member_versioned_client.MembersVersioned(connector=connector)
    sub_population_count = await member_versioned.get_count_for_sub_population_criteria(
        query_criteria
    )

    if sub_population_count == 0:
        # Log warning if this sub_population count is 0
        logger.info(
            "Sub-population count for criteria is 0",
            query_criteria=query_criteria,
        )

    # Add the count to the running totals
    sub_population_counts[
        sub_population_id
    ] = sub_population_count + sub_population_counts.get(sub_population_id, 0)


async def get_sub_population_member_ids(
    org_ids: Optional[List[int]] = None,
    no_op: bool = True,
):
    await process_sub_populations(
        org_ids=org_ids,
        no_op=no_op,
        final_processor=_persist_sub_population_member_ids,
    )


async def _persist_sub_population_member_ids(
    sub_population_id: int,
    query_criteria: str,
    no_op: bool = True,
    connector: Optional[postgres_connector.PostgresConnector] = None,
    context: Dict[str, Any] = {},
) -> None:
    """
    This function calls get_ids_for_sub_population_criteria using the supplied
    query and adds it to a running count that's stored in a dictionary, which
    is keyed by the sub_population_id. This allows us to process each set of
    criteria separately to come up with the final counts for each sub-population.
    """
    # Run the query to get the member IDs
    member_versioned = member_versioned_client.MembersVersioned(connector=connector)
    sub_pop_member_ids = await member_versioned.get_ids_for_sub_population_criteria(
        query_criteria
    )

    # If there are members in the sub-population, save them to the DB
    if len(sub_pop_member_ids) > 0:
        logger.info(
            f"{sub_population_id}: {sub_pop_member_ids}",
            sub_population_id=sub_population_id,
            query_criteria=query_criteria,
        )
        if not no_op:
            # Upsert member IDs to TBD table
            msp_repo = member_sub_pop_repo.MemberSubPopulationRepository()
            await msp_repo.persist_member_sub_population_records(
                [
                    pop_model.MemberSubPopulation(
                        member_id=member_id,
                        sub_population_id=sub_population_id,
                    )
                    for member_id in sub_pop_member_ids
                ]
            )


async def process_sub_populations(
    org_ids: Optional[List[int]] = None,
    no_op: bool = True,
    final_processor: Callable[
        [int, str, bool, Optional[postgres_connector.PostgresConnector], ...], bool
    ] = None,
    context: Dict[str, Any] = {},
):
    """
    This is the main function of the cron job. It optionally takes a list of
    organization IDs. The cron job will not provide a list, so the function will
    run for all organizations. The optional parameter was included to allow
    users to run the process on specific organizations. The function also
    takes a no_op boolean to indicate whether the final totals should
    be written to the database. This will allow us to run tests without
    writing to the database.
    """
    # Set up DB connection
    dsn = postgres_connector.get_dsn()
    pool = postgres_connector.create_pool(dsn=dsn, min_size=5, max_size=10)
    connector = postgres_connector.PostgresConnector(dsn=dsn, pool=pool)

    # Get active populations
    pop_client = population_client.Populations(connector=connector)
    active_populations = []
    if org_ids is None:
        active_populations_list = await pop_client.get_all_active_populations()
        active_populations = [(pop.id, pop) for pop in active_populations_list]
    else:
        for org_id in org_ids:
            active_populations.append(
                (
                    org_id,
                    await pop_client.get_active_population_for_organization_id(
                        organization_id=org_id,
                    ),
                )
            )

    # For each active population, construct and run the SQL criteria by recursively
    # processing the sub-maps and add their counts to sub_population_count
    for org_id, population in active_populations:
        if population is None:
            logger.warning(
                f"There is no active population for organization {org_id}",
                organization_id=org_id,
            )
            continue
        structlog.contextvars.bind_contextvars(
            organization_id=population.organization_id,
            population_id=population.id,
        )
        logger.info(
            "Calculating sub-population count",
            organization_id=population.organization_id,
            population_id=population.id,
        )
        lookup_keys = population.sub_pop_lookup_keys_csv
        lookup_map = population.sub_pop_lookup_map_json
        if population.sub_pop_lookup_keys_csv and population.sub_pop_lookup_map_json:
            # Move the CSV lookup keys into a list of strings for processing
            lookup_keys_list = [key.strip() for key in lookup_keys.split(",")]
            await _process_lookup_sub_map(
                lookup_keys_list=lookup_keys_list,
                lookup_map=lookup_map,
                query_criteria=f"organization_id = {population.organization_id} AND effective_range @> CURRENT_DATE",
                final_processor=final_processor,
                no_op=no_op,
                connector=connector,
                context=context,
            )
        structlog.contextvars.unbind_contextvars("organization_id", "population_id")


async def _process_lookup_sub_map(
    lookup_keys_list: List[str],
    lookup_map: Dict[str, Dict | int] | int,
    query_criteria: str,
    final_processor: Callable[
        [int, str, bool, Optional[postgres_connector.PostgresConnector], ...], bool
    ],
    no_op: bool = True,
    connector: Optional[postgres_connector.PostgresConnector] = None,
    context: Dict[str, Any] = {},
) -> bool:
    """
    This function processes the lookup map by recursively building the SQL criteria
    from the map. Once it gets to the leaf node, which should have the sub-population
    ID, it calls _add_sub_population_criteria_count to query based on the criteria
    and to add it to the running total.

    It handles the default case by collecting information about the non-default cases
    first and then excluding those in the criteria, leaving all other cases as part of
    the default case.
    """
    # Handle lookup_map leaf where the lookup_keys_list is empty and the lookup_map
    # is the sub_population_id
    if len(lookup_keys_list) == 0:
        sub_population_id = lookup_map
        if not isinstance(sub_population_id, int):
            logger.error(
                "Sub-population is not defined correctly",
                query_criteria=query_criteria,
                lookup_keys_list=lookup_keys_list,
                sub_population_id=sub_population_id,
            )
            # Stop processing by returning False
            return False

        if final_processor is not None:
            # Run the final processor
            await final_processor(
                sub_population_id=sub_population_id,
                query_criteria=query_criteria,
                no_op=no_op,
                connector=connector,
                context=context,
            )
        return True

    if not isinstance(lookup_map, dict):
        logger.error(
            "Sub-population is not defined correctly",
            query_criteria=query_criteria,
            lookup_keys_list=lookup_keys_list,
            lookup_map=lookup_map,
        )
        # Stop processing by returning False
        return False
    # Convert criterion_key dotted notation
    criterion_key = _translate_dotted_notation_attribute_key(lookup_keys_list[0])
    # If default case is defined, we will need to save the non-default cases to exclude
    # them from the default query
    has_default_case = pop_model.SpecialCaseAttributes.DEFAULT_CASE in lookup_map
    non_default_values = []
    # Process the map
    for criterion_value, stored_value in lookup_map.items():
        # Handle default case last once we know what all the non-default values are
        if criterion_value == pop_model.SpecialCaseAttributes.DEFAULT_CASE:
            continue

        criterion = (criterion_key, criterion_value)
        # Save non-default criterion for processing default case
        if has_default_case:
            non_default_values.append(criterion)
        # Update query to include latest key-value information
        temp_query_criteria = _append_sub_population_criteria_to_query_criteria(
            criteria=[criterion],
            is_equality_comparison=True,
            current_query_criteria=query_criteria,
        )

        # Process the criteria sub-map
        if not await _process_lookup_sub_map(
            lookup_keys_list=lookup_keys_list[1:],
            lookup_map=stored_value,
            query_criteria=temp_query_criteria,
            final_processor=final_processor,
            no_op=no_op,
            connector=connector,
            context=context,
        ):
            # If sub-map processing failed, stop processing by returning False
            # If everything is fine, don't return anything yet so that other
            # map keys are also processed
            return False

    # Handle default values
    if has_default_case:
        temp_query_criteria = query_criteria
        # Only need to add to the temp_query_criteria if there are
        # non_default_values defined (i.e., more than just the default)
        if len(non_default_values) > 0:
            temp_query_criteria = _append_sub_population_criteria_to_query_criteria(
                criteria=non_default_values,
                is_equality_comparison=False,
                current_query_criteria=temp_query_criteria,
            )

        # Process the criteria sub-map
        return await _process_lookup_sub_map(
            lookup_keys_list=lookup_keys_list[1:],
            lookup_map=lookup_map.get(pop_model.SpecialCaseAttributes.DEFAULT_CASE, {}),
            query_criteria=temp_query_criteria,
            final_processor=final_processor,
            no_op=no_op,
            connector=connector,
            context=context,
        )

    # Getting here means that the map was processed correctly and no default
    # case was defined
    return True


def _append_sub_population_criteria_to_query_criteria(
    criteria: List[Tuple[str, str]],
    is_equality_comparison: bool,
    current_query_criteria: str,
) -> str:
    """
    This function creates and appends a sub-population's criteria to an
    existing query criteria.

    It goes through the list of criteria and applies an equality or
    inequality operator based on the value of is_equality_comparison.
    This is then appended to the end of current_query_criteria, adding
    an AND if needed.

    It also looks for the specially defined case of IS_NULL to replace
    with the appropriate NULL comparison.
    """
    # Set the comparators
    if is_equality_comparison:
        value_comparator = "="
        null_comparator = "IS NULL"
    else:
        value_comparator = "!="
        null_comparator = "IS NOT NULL"

    return_query_criteria = current_query_criteria
    # Go through each criterion and add it to the return query
    for criterion_key, criterion_value in criteria:
        if len(return_query_criteria) > 0:
            return_query_criteria += " AND "
        # Check for special IS_NULL case
        if criterion_value != pop_model.SpecialCaseAttributes.IS_NULL:
            return_query_criteria += (
                f"{criterion_key} {value_comparator} '{criterion_value}'"
            )
        else:
            return_query_criteria += f"{criterion_key} {null_comparator}"

    return return_query_criteria


def _translate_dotted_notation_attribute_key(dotted_notation_key: str) -> str:
    """
    This function converts the attribute names that are stored in dotted notation
    into fields names that can be used by database queries. It assumes that the root
    key is the main field and subsequent keys are part of the JSON object stored in
    that field. It will extract each key after the first one as a JSON object by using
    the "->" operator, except the last one, which it converts into a string for
    use by using the "->>" operator.
    """
    criterion_key = ""
    key_components = dotted_notation_key.split(".")
    num_components = len(key_components)
    for i, component in enumerate(key_components):
        # Regular attributes and roots of attributes don't need to be
        # escaped in any special way
        if i == 0:
            criterion_key = component
            continue

        # For components that are in-between the root and the leaf, we
        # want to use "->" to get the JSON object
        if i < num_components - 1:
            criterion_key += f"->'{component}'"
            continue

        # For the leaf component, we want to use "->>" to get the text value
        criterion_key += f"->>'{component}'"

    return criterion_key
