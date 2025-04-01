import logging
from typing import IO

from cleo import Application
from cleo.config.application_config import ApplicationConfig
from clikit.api.args import RawArgs
from clikit.api.io import InputStream, OutputStream
from clikit.api.io import flags as verbosity

_levels = {
    logging.CRITICAL: verbosity.NORMAL,
    logging.ERROR: verbosity.NORMAL,
    logging.WARNING: verbosity.VERBOSE,
    logging.INFO: verbosity.VERY_VERBOSE,
    logging.DEBUG: verbosity.DEBUG,
}
_verbosity = {v: level for level, v in _levels.items()}


class ClikitHandler(logging.Handler):
    """Logging handler that redirects all messages to clikit io object.

    https://github.com/sdispater/cleo/issues/49#issuecomment-536453393
    """

    def __init__(self, io, level=logging.NOTSET):
        super().__init__(level=level)
        self.io = io

    def emit(self, record: logging.LogRecord):
        level = _levels[record.levelno]
        if record.levelno >= logging.WARNING:
            text = record.getMessage()
            self.io.error_line(text, flags=level)
        elif self.io.verbosity >= level:
            text = record.getMessage()
            self.io.write_line(text)

    @classmethod
    def setup_for(cls, name, io):
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)
        log.handlers = [cls(io)]
        log.debug("Logger initialized.")


class LoggingApplicationConfig(ApplicationConfig):
    def __init__(self, name: str = None, version: str = None, *loggers: str):
        super().__init__(name=name, version=version)
        self.loggers = loggers

    def create_io(
        self,
        application: Application,
        args: RawArgs,
        input_stream: InputStream = None,
        output_stream: OutputStream = None,
        error_stream: OutputStream = None,
    ) -> IO:
        io = super().create_io(
            application,
            args,
            input_stream=input_stream,
            output_stream=output_stream,
            error_stream=error_stream,
        )
        for n in (self.name,) + self.loggers:
            ClikitHandler.setup_for(n, io)
        return io
