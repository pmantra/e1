from bin.commands.base import BaseAppCommand

SUBTITLE = "purge-duplicate-optum-records"


class PurgeDuplicateOptumRecordsCommand(BaseAppCommand):
    """Run the purge duplicate optum records job"""

    name = "purge-duplicate-optum-records"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.tasks import purge_duplicate_optum_records

        purge_duplicate_optum_records.main()
        return 0
