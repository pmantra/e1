from __future__ import annotations

import pytest
from tests.factories.data_models import MemberVersionedFactory

from app.dryrun import utils as dry_run_utils
from app.eligibility.populations import model as pop_model
from db import model as db_model


class TestUtils:
    def create_member(
        self, work_country: str, work_state: str, insurance_plan: str | None
    ) -> db_model.MemberVersioned:
        result = MemberVersionedFactory.create()
        result.record = {"insurance_plan": insurance_plan}
        result.work_country = work_country
        result.work_state = work_state
        return result

    @pytest.mark.parametrize(
        "work_country,work_state,insurance_plan,expected",
        [
            # date_of_birth None, Not None
            ("US", "NY", "UNH", 1),
            ("US", "NY", None, 2),
            ("US", "NY", "BCBS-MN", 3),
            ("US", "MN", "BCBS", 4),
            ("US", "MN", "UNH", 5),
            ("US", "MN", "CVS", 6),
            ("US", "MN", "OTHER", None),
            ("CA", "ON", "CanadaIns", 7),
            ("CA", "ON", None, 8),
            ("CA", "ON", "FRAUD", None),
            ("CA", "BC", "BCIns", 9),
            ("CA", "BC", "CanadaIns", 10),
            ("CA", "BC", "UNK", 11),
            ("CA", "BC", None, None),
            ("CA", "ST", "UNH", None),
            ("BR", "P1", None, 12),
            ("FR", "T1", "FINS", 13),
            ("FR", None, "FINS", None),
            (None, "UN", None, 14),
        ],
    )
    def test_find_population(self, work_country, work_state, insurance_plan, expected):
        population = pop_model.Population(
            organization_id=1,
            sub_pop_lookup_keys_csv="work_country,work_state,record.insurance_plan",
            sub_pop_lookup_map_json={
                "US": {
                    "NY": {
                        "UNH": 1,
                        "ATTRIBUTE_IS_NULL": 2,
                        "ATTRIBUTE_DEFAULT_CASE": 3,
                    },
                    "MN": {
                        "BCBS": 4,
                        "UNH": 5,
                        "CVS": 6,
                    },
                },
                "CA": {
                    "ON": {
                        "CanadaIns": 7,
                        "ATTRIBUTE_IS_NULL": 8,
                    },
                    "BC": {"BCIns": 9, "CanadaIns": 10, "ATTRIBUTE_DEFAULT_CASE": 11},
                },
                "ATTRIBUTE_DEFAULT_CASE": {
                    "ATTRIBUTE_DEFAULT_CASE": {
                        "ATTRIBUTE_IS_NULL": 12,
                        "ATTRIBUTE_DEFAULT_CASE": 13,
                    }
                },
                "ATTRIBUTE_IS_NULL": 14,
            },
        )
        member = self.create_member(work_country, work_state, insurance_plan)
        res = dry_run_utils.find_population(population=population, member=member)
        assert res.sub_pop_id == expected
