from bin.commands.base import BaseAppCommand

SUBTITLE = """
╔═╗┬ ┬┌┐┌┌─┐
╚═╗└┬┘││││  
╚═╝ ┴ ┘└┘└─┘
"""


class SyncCommand(BaseAppCommand):
    """Run the maven.organization->eligibility.configuration sync.

    sync
    """

    name = "sync"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.tasks import sync

        sync.main()
        return 0
