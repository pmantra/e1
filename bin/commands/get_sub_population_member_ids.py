import asyncio

from bin.commands.base import BaseAppCommand

SUBTITLE = "get-sub-population-member-ids"


class GetSubPopulationMemberIdsCommand(BaseAppCommand):
    """Run the purge expired-records job"""

    name = "get-sub-population-member-ids"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.tasks import calculate_sub_populations

        asyncio.run(
            calculate_sub_populations.get_sub_population_member_ids(no_op=False)
        )
        return 0
