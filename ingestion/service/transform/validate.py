from __future__ import annotations

import datetime

import ddtrace
from ingestion import model

__all__ = "is_effective_range_activated"


@ddtrace.tracer.wrap()
def is_effective_range_activated(
    activated_at: datetime, effective_range: model.EffectiveRange | None
) -> bool:
    """
    check if end of effective_range is before activated_at or not

    Parameter:
      activated_at: activate datetime
      effective_range: effective date range

    Returns:
      true - effective_range is None (no terminate date)
        or effective_range upper/ends is None (no terminate date)
        or activated before effective range ends
      false - activated after effective range ends
    """
    if effective_range is None or effective_range["upper"] is None:
        return True
    return activated_at.date() < effective_range["upper"]
