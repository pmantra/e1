from __future__ import annotations

from typing import List

from app.eligibility.populations import model as pop_model
from db.clients import member_sub_population_client as msp_client


class MemberSubPopulationRepository:
    def __init__(
        self,
        member_sub_population_client: msp_client.MemberSubPopulations | None = None,
    ):
        self._member_sub_population_client: msp_client.MemberSubPopulations = (
            member_sub_population_client or msp_client.MemberSubPopulations()
        )

    async def persist_member_sub_population_records(
        self,
        member_sub_population_records: List[pop_model.MemberSubPopulation],
    ):
        """Save a list of member sub-population information"""
        return await self._member_sub_population_client.bulk_persist(
            models=member_sub_population_records
        )

    async def get_sub_population_id_for_member_id(
        self,
        member_id: int,
    ):
        """Get the sub-population ID for the member ID information"""
        return await self._member_sub_population_client.get_sub_population_id_for_member_id(
            member_id=member_id
        )

    async def get_all_member_ids_for_sub_population_id(
        self,
        sub_population_id: int,
    ):
        """Get a list of member IDs for the sub-population ID"""
        return await self._member_sub_population_client.get_all_member_ids_for_sub_population_id(
            sub_population_id=sub_population_id
        )

    async def get_all_active_member_ids_for_sub_population_id(
        self,
        sub_population_id: int,
    ):
        """Get a list of active member IDs for the sub-population ID"""
        return await self._member_sub_population_client.get_all_active_member_ids_for_sub_population_id(
            sub_population_id=sub_population_id
        )
