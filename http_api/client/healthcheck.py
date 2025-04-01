from __future__ import annotations

import http

from aiohttp import web


def init_healthchecks(app: web.Application):
    """Bootstrap your server with standard healthchecks."""
    app.router.add_view("/livez", LivenessView)
    app.router.add_view("/readyz", ReadinessView)
    app.router.add_view("/startupz", StartupView)


class LivenessView(web.View):
    """A view which indicates this service is able to accept traffic.

    See Also:
         https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-a-liveness-http-request
    """

    async def get(self) -> web.Response:
        return web.json_response(data={"ready": True, "status": http.HTTPStatus.OK})


class ReadinessView(web.View):
    """A view which indicates this service is able to process requests.

    FIXME:
        Update this endpoint to make use of your application's service(s),
        not the hello-world example service.

    See Also:
        https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-readiness-probes
    """

    async def get(self) -> web.Response:
        # FIXME update when we integrate our top-level service.
        status = http.HTTPStatus.OK
        return web.json_response(
            data={"ready": True, "status": status},
            status=status,
        )


class StartupView(ReadinessView):
    """A view which indicates a (re)starting service is ready to process requests.

    FIXME:
        Update this endpoint to make use of your application's service(s),
        not the hello-world example service.

    See Also:
        https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/#define-startup-probes
    """
