from __future__ import annotations

import dataclasses
import enum
from datetime import datetime


class FeatureTypes(enum.IntEnum):
    TRACK_FEATURE = 1
    WALLET_FEATURE = 2


class SpecialCaseAttributes(str, enum.Enum):
    DEFAULT_CASE = "ATTRIBUTE_DEFAULT_CASE"
    IS_NULL = "ATTRIBUTE_IS_NULL"


@dataclasses.dataclass
class Population:
    organization_id: int
    sub_pop_lookup_keys_csv: str
    sub_pop_lookup_map_json: dict
    id: int | None = None
    activated_at: datetime | None = None
    deactivated_at: datetime | None = None
    advanced: bool = False
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclasses.dataclass
class SubPopulation:
    population_id: int
    feature_set_name: str
    feature_set_details_json: dict
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclasses.dataclass
class PopulationInformation:
    population_id: int
    sub_pop_lookup_keys_csv: str
    advanced: bool
    organization_id: int | None = None


@dataclasses.dataclass
class MemberSubPopulation:
    member_id: int
    sub_population_id: int
    created_at: datetime | None = None
    updated_at: datetime | None = None
