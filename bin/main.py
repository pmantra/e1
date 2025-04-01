import sys

import constants
from config import settings

# generated protobuf python files do absolute imports, not relative package imports,
# so they need access to all of their modules on the PYTHONPATH.
# this also makes it easier for us to import these files directly
sys.path.insert(0, str(constants.PROTOBUF_DIR))

import warnings

import cleo
from cleo.parser import Option

from bin.commands import COMMANDS
from bin.log import LoggingApplicationConfig

warnings.filterwarnings("ignore", category=DeprecationWarning)


class ApplicationConfig(LoggingApplicationConfig):
    def configure(self):
        super().configure()
        self.add_option(
            long_name="no-metrics",
            short_name="M",
            description="Disable metrics reporting.",
            value_name="no-metrics",
        )
        self.add_option(
            long_name="no-trace",
            short_name="T",
            description="Disable event tracing.",
            value_name="no-trace",
        )
        self.add_option(
            long_name="no-error-reporting",
            short_name="E",
            description="Disable error reporting.",
            value_name="no-errors",
        )
        self.add_option(
            long_name="dev-local",
            flags=Option.BOOLEAN,
            description="Run as dev-local.",
            value_name="dev-local",
        )


class Main(cleo.Application):
    def __init__(self):
        app_settings = settings.App()
        super().__init__(
            config=ApplicationConfig(constants.APP_NAME, app_settings.version)
        )


app = Main()
app.add_commands(*(c() for c in COMMANDS))

run = app.run
