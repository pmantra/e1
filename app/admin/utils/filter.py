from __future__ import annotations

import re
from typing import Any, Dict, List

import structlog.stdlib

from db.sqlalchemy.models.population import Populations
from sqlalchemy import not_
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.schema import Column

logger = structlog.getLogger(__name__)


def apply_filters(
    filter_groups_data: Dict[str, List[Dict[str, Any]]],
    request_args: Dict[str, str],
    the_query: Query,
) -> (List, Query):
    """
    This function applies filtering on a SQLAlchemy query based on how filtering information
    is passed in by flask-admin. The filter groups should be the structure that served as
    the source of truth to the page that created the filter args. It returns a list of the
    active filters along with a new Query object that has had the filters applied to it.
    """
    active_filters = []
    # For each key/value pair in teh request args...
    for key, value in request_args.items():
        # Check if the key is for a filter (as formatted by flask-admin JS)
        filter_match = re.fullmatch("flt\\d+_(\\d+)", key)
        if filter_match:
            if value == "":
                # Ignore empty values
                continue
            try:
                # Determine how filter information based on the filter index and
                # the filter groups that were passed on
                filter_index = int(filter_match.group(1))
                filter_applied = False
                column_alias, operation = find_filter_info(
                    filter_groups_data=filter_groups_data,
                    filter_index=filter_index,
                )
                # Apply the filter, passing in the value based on the column alias
                if column_alias == "Id":
                    (filter_applied, the_query,) = apply_value_filter(
                        attribute=Populations.id,
                        operation=operation,
                        value=value,
                        the_query=the_query,
                    )
                elif column_alias == "Organization Id":
                    (filter_applied, the_query,) = apply_value_filter(
                        attribute=Populations.organization_id,
                        operation=operation,
                        value=value,
                        the_query=the_query,
                    )
                elif column_alias == "Active":
                    (filter_applied, the_query,) = apply_boolean_filter(
                        attribute=Populations.active,
                        operation=operation,
                        value=value,
                        the_query=the_query,
                    )

                if filter_applied:
                    # If the filter was applied, add it to the active_filters list
                    active_filters.append([filter_index, column_alias, str(value)])
                else:
                    # If the filter failed to be applied, log it and continue
                    logger.warning(
                        "Unrecognized filter: Needs implementation to support this filter",
                        column_alias=column_alias,
                        operation=operation,
                        value=value,
                    )
            except (ValueError, Exception) as e:
                # Unexpected filter value or error, log it and continue
                logger.error(
                    "There was an error applying filtering on populations",
                    key=key,
                    value=value,
                    details=e,
                )
    return active_filters, the_query


def apply_value_filter(
    attribute: Column,
    operation: str,
    value: str,
    the_query: Query,
) -> (bool, Query):
    """
    Applies the operation filter to the query. The function returns a boolean
    indicating if the filter was applied, along with the updated Query object.
    The filter might not be applied if the operation is not known or not yet
    implemented. This basic implementation currently supports "equals" and
    "in list", but more could be added later.
    """
    if operation == "equals":
        the_query = the_query.filter(attribute == value)
    elif operation == "in list":
        value_list = value.split(",")
        the_query = the_query.filter(attribute.in_(value_list))
    else:
        return False, the_query
    return True, the_query


def apply_boolean_filter(
    attribute: Column,
    operation: str,
    value: str,
    the_query: Query,
) -> (bool, Query):
    """
    Applies the boolean filter to the query. The function returns a boolean
    indicating if the filter was applied, along with the updated Query object.
    The filter might not be applied if the operation is not known or not yet
    implemented. This basic implementation currently supports "equals", but
    more could be added later.
    """
    if operation == "equals":
        value = int(value)
        if value:
            the_query = the_query.filter(attribute)
        else:
            the_query = the_query.filter(not_(attribute))
    else:
        return False, the_query
    return True, the_query


def find_filter_info(
    filter_groups_data: Dict[str, List[Dict[str, Any]]], filter_index: int
) -> (str, str):
    """
    This uses the filter index and goes through the filter groups to get information about
    what kind of filtering is to be used. It returns the key, which is the column alias,
    as well as the operation to be used as the filter.
    """
    for key, value in filter_groups_data.items():
        for filter_dict in value:
            if filter_dict.get("index", -1) == filter_index:
                return key, filter_dict.get("operation", "")
