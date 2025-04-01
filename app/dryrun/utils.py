from __future__ import annotations

from app.dryrun import model as dry_run_model
from app.eligibility.populations import model as pop_model
from app.utils import eligibility_member
from db import model as db_model


def to_member(record: db_model.FileParseResult) -> db_model.MemberVersioned:

    try:
        return db_model.MemberVersioned(
            file_id=record.file_id,
            organization_id=record.organization_id,
            date_of_birth=record.date_of_birth,
            first_name=record.first_name,
            last_name=record.last_name,
            email=record.email,
            unique_corp_id=record.unique_corp_id,
            dependent_id=record.dependent_id,
            employer_assigned_id=record.employer_assigned_id,
            do_not_contact=record.do_not_contact,
            gender_code=record.gender_code,
            work_state=record.work_state,
            work_country=record.work_country,
            record=record.record,
            custom_attributes=record.custom_attributes,
            effective_range=record.effective_range,
            hash_value=record.hash_value,
            hash_version=record.hash_version,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )
    except Exception as e:
        raise e


def find_population(
    population: pop_model.Population, member: db_model.MemberVersioned
) -> dry_run_model.MemberVersionedWithPopulation:
    keys = population.sub_pop_lookup_keys_csv.split(",")
    if len(keys) == 0:
        return dry_run_model.MemberVersionedWithPopulation(
            member_versioned=member, sub_pop_id=None
        )
    map = population.sub_pop_lookup_map_json
    for key in keys:
        attr_val = eligibility_member.get_member_attribute(member, key)
        map_key = (
            pop_model.SpecialCaseAttributes.IS_NULL if attr_val is None else attr_val
        )

        if map_key in map:
            map = map[map_key]
        elif (
            map_key != pop_model.SpecialCaseAttributes.IS_NULL
            and pop_model.SpecialCaseAttributes.DEFAULT_CASE in map
        ):
            map = map[pop_model.SpecialCaseAttributes.DEFAULT_CASE]
        else:
            return dry_run_model.MemberVersionedWithPopulation(
                member_versioned=member, sub_pop_id=None
            )

        if isinstance(map, int):
            return dry_run_model.MemberVersionedWithPopulation(
                member_versioned=member, sub_pop_id=map
            )
    return dry_run_model.MemberVersionedWithPopulation(
        member_versioned=member, sub_pop_id=None
    )
