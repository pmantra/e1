import datetime

import pytest
from ingestion import model, service


class TestIsEffectiveRangeActivated:
    historical_date = datetime.datetime.combine(
        datetime.date.today(), datetime.datetime.min.time()
    ) - datetime.timedelta(days=365)
    current_date = datetime.datetime.today()
    future_date = datetime.datetime.combine(
        datetime.date.today(), datetime.datetime.min.time()
    ) + datetime.timedelta(days=365)

    @staticmethod
    @pytest.mark.parametrize(
        argnames="activated_at, effective_range,expected",
        argvalues=[
            (current_date, None, True),
            (current_date, model.EffectiveRange(upper=None), True),
            (current_date, model.EffectiveRange(upper=historical_date.date()), False),
            (current_date, model.EffectiveRange(upper=future_date.date()), True),
        ],
        ids=[
            "effective_range_none",
            "effective_upper_none",
            "range_ends_before_activated",
            "range_ends_after_activated",
        ],
    )
    def test_is_effective_range_activated(effective_range, expected, activated_at):
        # When
        result = service.is_effective_range_activated(activated_at, effective_range)

        # Then
        assert result == expected
