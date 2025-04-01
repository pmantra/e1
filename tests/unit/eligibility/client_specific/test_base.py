from __future__ import annotations

import dataclasses
from typing import Any

import aiohttp
import orjson
import pytest
from tests.factories import data_models as factory

from app.eligibility.client_specific import base

pytestmark = pytest.mark.asyncio


@pytest.fixture
def caller() -> FooSpecificCaller:
    return FooSpecificCaller()


@pytest.fixture
def client() -> FooSpecificProtocol:
    return FooSpecificProtocol()


class TestClientSpecificCaller:
    @staticmethod
    async def test_call(caller, response_mock):
        # Given
        json = {"eligible": True}
        expected = {"eligible": True, "validated": True}
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(caller.url, status=200, body=orjson.dumps(json))
        # When
        response = await caller.call(request=request)
        # Then
        assert response == expected

    @staticmethod
    @pytest.mark.parametrize(argnames="errno", argvalues=[300, 400, 500])
    async def test_call_error(errno, caller, response_mock):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(caller.url, status=errno, body=b"No dice!")
        # When/Then
        with pytest.raises((aiohttp.ClientError, base.ClientSpecificError)):
            await caller.call(request=request)

    @staticmethod
    async def test_call_nonjson_response(caller, response_mock):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(caller.url, status=200, body=b"not json")
        # When/Then
        with pytest.raises(base.ResponseDecodeError):
            await caller.call(request=request)

    @staticmethod
    async def test_call_invalid_content_type_manual_decode(caller, response_mock):
        # Given
        json = {"eligible": True}
        expected = {"eligible": True, "validated": True}
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            caller.url,
            status=200,
            body=orjson.dumps(json),
            content_type="application/octet-stream",
        )
        # When
        response = await caller.call(request=request)
        # Then
        assert response == expected

    @staticmethod
    async def test_call_invalid_content_type_manual_decode_fails(caller, response_mock):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            caller.url,
            status=200,
            body=b"not json",
            content_type="application/octet-stream",
        )
        # When/Then
        with pytest.raises(base.ResponseDecodeError):
            await caller.call(request=request)

    @staticmethod
    async def test_call_invalid_content_type_invalid_encoding(caller, response_mock):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            caller.url,
            status=200,
            body="Ђ".encode("utf-16"),
            content_type="application/octet-stream; charset=utf-8",
        )
        # When/Then
        with pytest.raises(base.InvalidResponseType):
            await caller.call(request=request)


class TestClientClientSpecificProtocol:
    @staticmethod
    async def test_verify_pass(client, response_mock):
        # Given
        json = {"eligible": True}
        expected = {"eligible": True, "validated": True}
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            client.caller.url,
            status=200,
            body=orjson.dumps(json),
            content_type="application/octet-stream",
        )
        # When
        response = await client.verify(request)
        # Then
        assert response == expected

    @staticmethod
    async def test_verify_fail(client, response_mock):
        # Given
        json = {"eligible": False}
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            client.caller.url,
            status=200,
            body=orjson.dumps(json),
            content_type="application/json",
        )
        # When
        response = await client.verify(request)
        # Then
        assert response is None

    @staticmethod
    async def test_verify_invalid_response(client, response_mock):
        # Given
        request = factory.ClientSpecificEmployeeRequestFactory.create()
        response_mock.post(
            client.caller.url,
            status=200,
            body="Ђ".encode("utf-16"),
            content_type="application/octet-stream; charset=utf-8",
        )
        # When
        response = await client.verify(request)
        # Then
        assert response is None


class FooSpecificCaller(base.ClientSpecificCaller):
    def __init__(self):
        super().__init__(url="http://eligibility.foo.bar/check")

    async def _get_headers(self) -> dict[str, str]:
        return {}

    def _get_payload(self, request: base.ClientSpecificRequest) -> dict[str, Any]:
        return dataclasses.asdict(request)

    def _do_validate(
        self, body: dict, *, response: aiohttp.ClientResponse = None
    ) -> dict:
        body["validated"] = True
        return body

    async def _do_request(
        self, session: aiohttp.ClientSession, payload: dict
    ) -> aiohttp.ClientResponse:
        return await session.post(self.url, data=payload)


class FooSpecificProtocol(base.ClientSpecificProtocol[dict]):
    def __init__(self):
        self.caller = FooSpecificCaller()

    def check_eligibility(self, response: dict) -> dict | None:
        if response["eligible"]:
            return response
        return None
