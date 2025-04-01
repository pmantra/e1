from __future__ import annotations

import dataclasses
import datetime
from typing import Any, Generic, NoReturn, TypeVar
from urllib import parse

import aiohttp
import async_timeout
import ddtrace
import orjson
from ddtrace.ext import SpanTypes
from mmlib.ops import log

from app.eligibility import translate

_ResponseT = TypeVar("_ResponseT", bound=dict)
logger = log.getLogger(__name__)


class ClientSpecificCaller(Generic[_ResponseT]):
    url: str
    service: str
    resource: str
    timeout: int = 5

    def __init__(self, url: str):
        self.url = url
        parsed = parse.urlparse(self.url)
        self.service = parsed.hostname
        self.resource = parsed.path

    async def call(self, request: ClientSpecificRequest) -> _ResponseT:
        with ddtrace.tracer.trace(
            "client_specific_call",
            service=self.service,
            resource=self.resource,
            span_type=SpanTypes.HTTP,
        ):
            payload = self._get_payload(request=request)
            headers = await self._get_headers()
            async with self.session(headers=headers) as session:
                response = await self._do_request(session=session, payload=payload)
                if not response.ok:
                    return await self._handle_client_error(
                        request=request, response=response
                    )
                data = await self._extract_body(response)
                return self._do_validate(body=data, response=response)

    def session(self, *, headers: dict = None) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            json_serialize=translate.dump,
        )

    async def _extract_body(self, response: aiohttp.ClientResponse) -> dict:
        try:
            return await response.json(loads=orjson.loads)
        except aiohttp.ContentTypeError as e:
            try:
                text = await response.text()
                try:
                    return orjson.loads(text)
                except orjson.JSONDecodeError:
                    raise self._response_decode_error(response) from e
            except UnicodeDecodeError:
                raise InvalidResponseType(
                    "Received non-JSON content type with invalid encoding from server",
                    response=response,
                ) from e
        except orjson.JSONDecodeError as e:
            raise self._response_decode_error(response) from e

    @staticmethod
    def _response_decode_error(response):
        return ResponseDecodeError(
            "Got non-JSON response from server.", response=response
        )

    async def _do_request(
        self, session: aiohttp.ClientSession, payload: dict
    ) -> aiohttp.ClientResponse:
        ...

    async def _get_headers(self) -> dict[str, str]:
        raise NotImplementedError()

    def _get_payload(self, request: ClientSpecificRequest) -> dict[str, Any]:
        raise NotImplementedError()

    def _do_validate(
        self, body: dict, *, response: aiohttp.ClientResponse = None
    ) -> _ResponseT:
        raise NotImplementedError()

    async def _handle_client_error(
        self, request: ClientSpecificRequest, response: aiohttp.ClientResponse
    ) -> _ResponseT | NoReturn:
        msg = "Got an upstream server error."
        if 300 <= response.status <= 499:
            msg = "No match found for user."
        raise ClientSpecificError(msg, response=response)


class ClientSpecificProtocol(Generic[_ResponseT]):
    caller: ClientSpecificCaller[_ResponseT]
    timeout: int = 6

    async def verify(self, request: ClientSpecificRequest) -> _ResponseT | None:
        ddtrace.tracer.set_tags(
            {
                "request.is_employee": request.is_employee,
                "request.unique_corp_id": request.unique_corp_id,
            }
        )
        try:
            async with async_timeout.timeout(self.timeout):
                response: _ResponseT = await self.caller.call(request)
            return self.check_eligibility(response)
        except ClientSpecificError as e:
            span = ddtrace.tracer.current_span()
            if span:
                span.set_tags(e.context)
            return None

    def check_eligibility(self, response: _ResponseT) -> _ResponseT | None:
        raise NotImplementedError()


@dataclasses.dataclass
class ClientSpecificRequest:
    is_employee: bool
    unique_corp_id: str
    date_of_birth: datetime.date
    dependent_date_of_birth: datetime.date = None


class ClientSpecificError(Exception):
    context: dict

    def __init__(
        self,
        message: str = None,
        *,
        response: aiohttp.ClientResponse = None,
        context: dict = None,
    ):
        rctx = _get_response_context(response) if response else {}
        context = context or {}
        self.context = {
            **context,
            **rctx,
            "error.message": message,
            "error.type": self.__class__.__name__,
        }
        super().__init__(message)


class InvalidResponseType(ClientSpecificError, TypeError):
    ...


class ResponseDecodeError(ClientSpecificError, ValueError):
    ...


class ResponseValidationError(ClientSpecificError, ValueError):
    ...


def _get_response_context(response: aiohttp.ClientResponse) -> dict:
    return {
        "response.url": response.request_info.url.human_repr(),
        "response.status": response.status,
        "response.reason": response.reason,
        "response.ok": response.ok,
        "response.method": response.method,
        "response.content_type": response.content_type,
        "response.charset": response.charset,
    }
