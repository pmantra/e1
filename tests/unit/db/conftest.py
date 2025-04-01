import pytest

from db.clients.postgres_connector import compose_dsn


@pytest.fixture(autouse=True)
def clear_cache_compose_dsn():
    compose_dsn.cache_clear()
