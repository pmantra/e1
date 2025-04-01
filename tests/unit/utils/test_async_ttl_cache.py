import datetime
from unittest import mock

import pytest


@pytest.mark.asyncio
async def test_async_ttl_cache(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        # Underlying function hasn't been called yet
        assert dummy_function_mock.call_count == 0

        # When
        await cached_async_coroutine_fixture("test")

        # Then
        # Underlying function has been called
        assert dummy_function_mock.call_count == 1


@pytest.mark.asyncio
async def test_async_ttl_cache_use_cache(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        await cached_async_coroutine_fixture("test")
        # Underlying function has already been called once
        assert dummy_function_mock.call_count == 1

        # When
        await cached_async_coroutine_fixture("test")

        # Then
        # Underlying function has not been called a second time
        assert dummy_function_mock.call_count == 1


@pytest.mark.asyncio
async def test_async_ttl_cache_reset_cache(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        await cached_async_coroutine_fixture("test")
        # Underlying function has already been called once
        assert dummy_function_mock.call_count == 1

        # When
        # Cache is reset
        cached_async_coroutine_fixture.reset()
        await cached_async_coroutine_fixture("test")

        # Then
        # Underlying function has been called a second time
        assert dummy_function_mock.call_count == 2


@pytest.mark.asyncio
async def test_async_ttl_cache_time_out(cached_async_coroutine_fixture):
    # Given
    with mock.patch(
        "tests.unit.utils.conftest.dummy_function"
    ) as dummy_function_mock, mock.patch(
        "app.utils.async_ttl_cache.AsyncTTLCache._InnerCache._get_time_to_live_value"
    ) as ttl_func_mock:
        dummy_function_mock.return_value = "mock_value"
        # Set TTL to be 0:00:04 ago so that a second call will already be considered expired
        # The TTL value is set when the initial call is made, so the mocked TTL value must
        # be set before the first call to cached_async_coroutine.
        ttl_func_mock.return_value = datetime.datetime.now() - datetime.timedelta(
            seconds=4
        )
        await cached_async_coroutine_fixture("test")
        # Underlying function has already been called once
        assert dummy_function_mock.call_count == 1

        # When
        # Call cached_async_coroutine again
        await cached_async_coroutine_fixture("test")

        # Then
        # Underlying function has been called a second time
        assert dummy_function_mock.call_count == 2


@pytest.mark.asyncio
async def test_async_ttl_cache_multiple_calls(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        await cached_async_coroutine_fixture("test000")
        await cached_async_coroutine_fixture("test001")
        # Underlying function has already been called twice with different arguments
        assert dummy_function_mock.call_count == 2

        # When
        await cached_async_coroutine_fixture("test000")

        # Then
        # Underlying function has not been called again
        assert dummy_function_mock.call_count == 2


@pytest.mark.asyncio
async def test_async_ttl_cache_max_size(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        await cached_async_coroutine_fixture("test000")
        await cached_async_coroutine_fixture("test001")
        await cached_async_coroutine_fixture("test002")
        # Underlying function has already been called thrice with different arguments, which
        # is greater than the max size limit of 2
        assert dummy_function_mock.call_count == 3

        # When
        # Original function is called
        await cached_async_coroutine_fixture("test000")

        # Then
        # Underlying function has been called again due to max size limit having been reached
        assert dummy_function_mock.call_count == 4


@pytest.mark.asyncio
async def test_async_ttl_cache_max_size_lru(cached_async_coroutine_fixture):
    # Given
    with mock.patch("tests.unit.utils.conftest.dummy_function") as dummy_function_mock:
        dummy_function_mock.return_value = "mock_value"
        await cached_async_coroutine_fixture("test000")
        await cached_async_coroutine_fixture("test001")
        await cached_async_coroutine_fixture("test000")
        await cached_async_coroutine_fixture("test002")
        # Underlying function has already been called thrice with different arguments, which
        # is greater than the max size limit of 2
        assert dummy_function_mock.call_count == 3

        # When
        # Original function is called
        await cached_async_coroutine_fixture("test000")

        # Then
        # Underlying function has not been called again despite max size limit due to not
        # being least recently used
        assert dummy_function_mock.call_count == 3
