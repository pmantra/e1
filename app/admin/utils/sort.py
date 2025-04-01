from __future__ import annotations

from typing import Any, Dict, List

import structlog.stdlib

from db.sqlalchemy.models.population import Populations
from sqlalchemy.orm.query import Query

logger = structlog.getLogger(__name__)


def apply_sort(
    list_columns: List[Dict[str, Any]],
    sort_index: int,
    sort_direction: int,
    the_query: Query,
) -> Query:
    """
    Applies sorting on the query and returns the sorted Query object
    """
    sort_criteria = getattr(
        Populations, list_columns[sort_index].get("attribute", "id"), None
    )
    if sort_criteria is not None:
        if sort_direction > 0:
            sort_criteria = sort_criteria.asc()
        else:
            sort_criteria = sort_criteria.desc()
        the_query = the_query.order_by(sort_criteria)
    return the_query
