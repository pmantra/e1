from base64 import b64decode
from pathlib import Path
from typing import List

import jinja2
from flask import Flask, send_from_directory
from flask_admin import Admin, BaseView
from mmlib.ops import error
from mmlib.ops.log import get_logger
from werkzeug import exceptions

import constants
from bin.commands.base import BaseAppCommand
from db.clients import postgres_connector
from sqlalchemy.exc import PendingRollbackError

LOG = get_logger(__name__)
THIS_DIR = Path(__file__).resolve().parent
app = Flask(__name__)
_admin_views: List[BaseView] = []

SUBTITLE = """
╔═╗┌┬┐┌┬┐┬┌┐┌
╠═╣ │││││││││
╩ ╩─┴┘┴ ┴┴┘└┘
"""

# Flask views


@app.route("/")
def index():
    tmp = """
    <p><a href="/eligibility-admin/">Click me to get to E9Y Admin!</a></p>
    """

    return tmp


class AdminCommand(BaseAppCommand):
    """The command for running an internal Admin server.

    admin
    """

    name = "admin"
    subtitle = SUBTITLE

    def handle(self):
        from app.admin.views import views as app_admin_views
        from config import settings
        from db.flask import init_app

        # configure flask
        app.config.update(_create_flask_config(settings))
        init_app(app)

        # install our middleware
        # disabling iap altogether for eligibility, at least right now, since it is being proxied through
        # mono admin, and this is stopping those connections. FIXME ideally the admin stands alone behind IAP
        # app.before_request(middleware.iap_before)

        admin = Admin(
            app,
            url="/eligibility-admin",
            name=constants.APP_NAME,
            template_mode="bootstrap3",
            base_template="admin/main.html",
        )

        # this custom template loader prefers templates relative to your
        # /app/admin/templates folder then falls back on /admin/templates
        app_loader = jinja2.FileSystemLoader(
            [
                Path(app_admin_views.__file__).parent / "templates",
            ]
        )

        app.jinja_loader = app_loader

        global _admin_views
        _admin_views = app_admin_views.get_views()
        # attach our admin views to the admin
        for view in _admin_views:
            admin.add_view(view)

        # WARNING: Do NOT run in threaded mode, unless you can solve the rather sticky
        # problem of managing a clean asyncio event loop and thread-safe
        # connection-pool per thread.
        app.run(host="0.0.0.0", use_reloader=False, threaded=True)
        return 0


@app.errorhandler(500)
def internal_error(error):
    LOG.error("Eligibility admin internal error")

    if isinstance(error, PendingRollbackError):
        rollback_errored_sessions()

    # Raise the error again so that it can be handled by the default handler
    raise exceptions.InternalServerError(str(error))


def rollback_errored_sessions():
    """
    Try rollback and reset connection pool to fix the issue
    """
    connections = postgres_connector.application_connectors()
    connection: postgres_connector.PostgresConnector = connections["main"]

    LOG.warning("PendingRollbackError encountered. Resetting connection pool.")
    # reset the connection pool
    connection.close()
    connection.initialize()


def _create_flask_config(settings):
    conf = {
        # FIXME
        # if this is set to True, you will get some kind of "double server" thing going on
        # where the app starts twice, causing our local dev stats prometheus port to
        # attempt to be listened on twice, causing a failed startup for the app. don't
        # enable this if you can avoid it, or until you solve the double listening issue
        "DEBUG": False,
        "SECRET_KEY": b64decode(settings.Admin().secret_key),
    }
    return conf


@app.route("/eligibility-admin/static/e9y/<path:path>")
def static_e9y_file(path):
    return send_from_directory(Path("app/admin/views/static").resolve(), path)


def report_exception(sender, exception, **extra):
    with error.report_exceptions(reraise=False):
        raise exception
