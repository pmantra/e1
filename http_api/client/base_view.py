from aiohttp import web

from app.eligibility import service


class BaseView(web.View):
    @property
    def service(self) -> service.EligibilityService:
        return service.service()
