from __future__ import annotations

from typing import Dict, List

from app.dryrun import model as dry_run_model
from app.dryrun import repository as dry_run_repository
from app.dryrun import utils as dry_run_utils
from app.eligibility.populations import model as pop_model
from db import model as db_model


class PopulationCalculator:
    def __init__(self, override_sub_population: Dict[int, int] | None) -> None:
        self.population_client = dry_run_repository.Populations()
        self.override_sub_population = override_sub_population

    async def calculate_sub_pops(
        self, organization_id: int, members: List[db_model.MemberVersioned]
    ) -> dry_run_model.PopulationData:
        effective_pop = await self.get_effective_population_for_organization_id(
            organization_id=organization_id
        )
        if not effective_pop:
            raise dry_run_model.NoEffectivePopulation(
                organization_id=organization_id, member_count=len(members)
            )

        populated_members = []
        for member in members:
            populated_members.append(
                dry_run_utils.find_population(population=effective_pop, member=member)
            )
        return dry_run_model.PopulationData(
            population_id_used=effective_pop.id, populated_members=populated_members
        )

    async def get_effective_population_for_organization_id(
        self, organization_id: int
    ) -> pop_model.Population | None:
        """
        In case there is an active population, use it
        Otherwise use the most recent created one
        """
        if (
            self.override_sub_population
            and organization_id in self.override_sub_population
        ):
            return await self.population_client.get(
                self.override_sub_population[organization_id]
            )
        active_pop = (
            await self.population_client.get_active_population_for_organization_id(
                organization_id=organization_id
            )
        )
        if active_pop:
            return active_pop
        all_pops = await self.population_client.get_all_for_organization_id(
            organization_id=organization_id
        )
        return max(all_pops, key=lambda pop: pop.created_at, default=None)
