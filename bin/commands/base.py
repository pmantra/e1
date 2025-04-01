from textwrap import indent

import uvloop
from cleo import Command

BANNER = r"""
 ______     __         __     ______     __     ______     __     __         __     ______   __  __    
/\  ___\   /\ \       /\ \   /\  ___\   /\ \   /\  == \   /\ \   /\ \       /\ \   /\__  _\ /\ \_\ \   
\ \  __\   \ \ \____  \ \ \  \ \ \__ \  \ \ \  \ \  __<   \ \ \  \ \ \____  \ \ \  \/_/\ \/ \ \____ \  
 \ \_____\  \ \_____\  \ \_\  \ \_____\  \ \_\  \ \_____\  \ \_\  \ \_____\  \ \_\    \ \_\  \/\_____\ 
  \/_____/   \/_____/   \/_/   \/_____/   \/_/   \/_____/   \/_/   \/_____/   \/_/     \/_/   \/_____/ 
                                                                                                       
"""


class BaseAppCommand(Command):

    context = None
    banner = BANNER
    subtitle: str = None

    def hello(self):
        banner = self.banner.rstrip() if self.subtitle else self.banner
        self.line(banner, style="info")
        if self.subtitle:
            bannerlen = len(self.banner.lstrip().splitlines()[0])
            subtitlelen = len(self.subtitle.lstrip().splitlines()[0])
            prefix = " " * (bannerlen - subtitlelen)
            self.line(indent(self.subtitle.lstrip(), prefix=prefix), style="comment")

    def wrap_handle(self, args, io, command):
        import datadog
        import ddtrace
        from mmlib.ops import tracing

        import constants
        from app.common import log
        from config import settings

        # load .env if it exists
        settings.load_env()

        # Set up uvloop for our async event loop.
        if self.name != "admin":
            uvloop.install()

        # Default command setup.
        self._args = args
        self._io = io
        self._command = command

        # Say hello!
        self.hello()

        constants.APP_FACET = self.name
        app_settings = settings.App()
        gcp_settings = settings.GCP()
        log_settings = settings.Log()
        dd_settings = settings.Datadog()
        # Configure the top-level application runtime.
        log.configure(
            project=gcp_settings.project,
            service=dd_settings.service,
            version=app_settings.version,
            json=log_settings.json,
            level=log_settings.level,
        )

        if self.option("no-trace"):
            ddtrace.tracer.enabled = False
        else:
            tracing.configure(1.0)
        if not self.option("no-metrics"):
            datadog.initialize()

        return self.handle()
