from bin.commands.base import BaseAppCommand

SUBTITLE = "pre-verify"


class PreVerifyCommand(BaseAppCommand):
    """Run the pre-verification job"""

    name = "pre-verify"
    subtitle = SUBTITLE

    def handle(self) -> int:
        from app.tasks import pre_verify

        pre_verify.main()
        return 0
