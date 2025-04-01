import functools
from typing import Tuple, Type, TypeVar

import inflection

from app.eligibility.domain.model import RowT
from db import model
from db.clients import utils
from db.mono.client import MavenOrganization

from .convert import to_date

FIELD_MAP_KEY = "field_map"
CUSTOM_ATTRIBUTES_FIELD_MAP = "custom_attributes_field_map"
AFFILIATIONS_FIELD_MAP = "optional_field_map_affiliations"
HEALTH_PLAN_FIELD_MAP = "health_plan_field_map_affiliations"


LEGACY_NAMES = (
    ("employee_first_name", "first_name"),
    ("employee_last_name", "last_name"),
)
LEGACY_NAME_KEYS = frozenset((o for o, n in LEGACY_NAMES))

T = TypeVar("T")

dump = utils.dump_json


def org_to_config(
    org: MavenOrganization,
    *,
    config_cls: Type[T] = model.Configuration,
    implementation: model.ClientSpecificImplementation = None,
) -> Tuple[T, model.HeaderMapping]:
    defaults = model.HeaderMapping().with_all_headers()

    # Grab the header mapping values that mono has provided us
    mono_provided_headers = org.json.get(FIELD_MAP_KEY, {})
    # Add the custom attribute header mapping values that mono has provided to us
    mono_provided_headers.update(org.json.get(CUSTOM_ATTRIBUTES_FIELD_MAP, {}))
    # We have 'optional' headers that also need to be inserted
    mono_provided_headers.update(org.json.get(AFFILIATIONS_FIELD_MAP, {}))
    mono_provided_headers.update(org.json.get(HEALTH_PLAN_FIELD_MAP, {}))

    headers = model.HeaderMapping(
        {
            header: alias
            for header, alias in mono_provided_headers.items()
            # We don't need to persist mappings for:
            #   default aliases, 1:1 mappings, empty aliases
            if header and alias and (header != alias or defaults.get(header) != alias)
        }
    )

    # We changed some internal names so we need to remap those as well.
    # If the new mapping has these legacy keys, map them to the new name.
    if intersection := LEGACY_NAME_KEYS & headers.keys():
        names = dict(LEGACY_NAMES)
        for k in intersection:
            headers[names[k]] = headers.pop(k)
    # Directory name must not be null for e9y, but can be null in mono.
    directory_name = org.directory_name or slug(org.name)

    data_provider = True if org.data_provider == 1 else False
    employee_only = True if org.employee_only == 1 else False
    medical_plan_only = True if org.medical_plan_only == 1 else False
    eligibility_type = org.eligibility_type

    return (
        config_cls(
            organization_id=org.id,
            directory_name=directory_name,
            email_domains=org.email_domains,
            implementation=implementation,
            data_provider=data_provider,
            activated_at=org.activated_at,
            terminated_at=org.terminated_at,
            employee_only=employee_only,
            medical_plan_only=medical_plan_only,
            eligibility_type=eligibility_type,
        ),
        headers,
    )


def row_to_member(row: RowT, *, member_cls: Type[T] = model.Member) -> T:
    if dob := row.get("date_of_birth", ""):
        dob = to_date(dob)
    member = member_cls(
        organization_id=row["organization_id"],
        file_id=row["file_id"],
        first_name=row.get("first_name", ""),
        last_name=row.get("last_name", ""),
        date_of_birth=dob,
        work_state=row.get("work_state") or row.get("state", ""),
        email=row.get("email", ""),
        unique_corp_id=row.get("unique_corp_id", ""),
        dependent_id=row.get("dependent_id", ""),
        record=row,
    )
    return member


@functools.lru_cache(maxsize=10_000)
def slug(string: str) -> str:
    return inflection.dasherize(inflection.parameterize(string))
