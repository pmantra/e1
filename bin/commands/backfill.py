from bin.commands.base import BaseAppCommand


class BackfillCommand(BaseAppCommand):
    """Run the eligibility.member -> api_data.ao_eligibility_member (big query) backfill.

    backfill
    """

    name = "backfill"

    def handle(self) -> int:
        from app.tasks import backfill

        backfill.main()
        return 0
