import pytest

from app.utils import async_ttl_cache


def dummy_function(caller_id: str) -> str:
    return f"value_{caller_id}"


# Set up test function w/ ttl of 3 and max size of 2
@async_ttl_cache.AsyncTTLCache(time_to_live=3, max_size=2)
async def cached_async_coroutine(caller_id: str):
    return dummy_function(caller_id=caller_id)


@pytest.fixture
def cached_async_coroutine_fixture():
    cached_async_coroutine.reset()
    return cached_async_coroutine
