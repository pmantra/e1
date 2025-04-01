import pytest

from db import redis

pytestmark = pytest.mark.asyncio


class TestRedisKeyStore:
    @staticmethod
    async def test_set(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        # When
        response = await keystore.set(key, value)
        # Then
        assert response == 1
        assert await keystore.redis.redis.exists(key)

    @staticmethod
    async def test_get(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        await keystore.set(key, value)
        # When
        retrieved = await keystore.get(key)
        # Then
        assert retrieved == {**value, "key": key}

    @staticmethod
    async def test_delete(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        await keystore.set(key, value)
        # When
        deleted = await keystore.delete(key)
        # Then
        assert deleted == 1

    @staticmethod
    async def test_mset(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        pairs = {key: value}
        # When
        response = await keystore.mset(**pairs)
        assert response == 1
        assert await keystore.redis.redis.exists(key)

    @staticmethod
    async def test_imget(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        await keystore.set(key, value)
        # When
        values = [*(await keystore.imget(key, "otherkey"))]
        # Then
        assert values == [{**value, "key": key}]

    @staticmethod
    async def test_imget_with_keys(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        await keystore.set(key, value)
        # When
        values = dict((await keystore.imget(key, "otherkey", with_keys=True)))
        # Then
        assert values == {key: {**value, "key": key}}

    @staticmethod
    async def test_imget_with_null(keystore: redis.RedisKeyStore):
        # Given
        key, value = "key", {"imma": "value"}
        await keystore.set(key, value)
        # When
        values = dict(
            (await keystore.imget(key, "otherkey", with_keys=True, filter_null=False))
        )
        # Then
        assert values == {key: {**value, "key": key}, "otherkey": None}

    @staticmethod
    async def test_incr(keystore: redis.RedisKeyStore):
        # Given
        key = "key"
        # When
        response = await keystore.incr(key)
        # Then
        assert response == 1

    @staticmethod
    async def test_incr_override_amount(keystore: redis.RedisKeyStore):
        # Given
        key = "key"
        # When
        response = await keystore.incr(key, amount=8)
        # Then
        assert response == 8

    @staticmethod
    async def test_incr_twice(keystore: redis.RedisKeyStore):
        # Given
        key = "key"
        # When
        response_first = await keystore.incr(key)
        response_second = await keystore.incr(key)
        # Then
        assert (response_first, response_second) == (1, 2)

    @staticmethod
    async def test_delete_pattern(keystore: redis.RedisKeyStore):
        # Given
        pairs = {f"key:{i}": i for i in range(10)}
        await keystore.mset(**pairs)

        # When
        await keystore.delete_pattern(pattern="key:*")

        # Then
        val = await keystore.get("key:1")
        another_val = await keystore.get("key:6")
        assert not val and not another_val

    @staticmethod
    async def test_delete_pattern_correct_pattern(keystore: redis.RedisKeyStore):
        # Given
        pairs = {f"key:{i}": i for i in range(10)}
        await keystore.mset(**pairs)
        await keystore.set("akey:1", 1)

        # When
        await keystore.delete_pattern(pattern="key:*")

        # Then
        val = await keystore.get("key:1")
        another_val = await keystore.get("akey:1")
        assert not val and another_val
