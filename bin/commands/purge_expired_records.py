from bin.commands.base import BaseAppCommand

SUBTITLE = "purge-expired-records"


class PurgeExpiredRecordsCommand(BaseAppCommand):
    """Run the purge expired-records job"""

    name = "purge-expired-records"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.tasks import purge_expired_records

        purge_expired_records.main()
        return 0
