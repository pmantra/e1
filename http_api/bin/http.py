from aiohttp import web
from http_api.client import factory


def run():
    app: web.Application = factory.create_app()
    web.run_app(app=app, port=8888)
