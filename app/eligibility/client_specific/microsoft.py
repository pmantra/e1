from __future__ import annotations

import asyncio
import contextvars
import os
from typing import Optional, Tuple, TypedDict

import aiohttp
import msal
import typic
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.x509 import load_pem_x509_certificate
from mmlib.ops import log

from config import settings
from config.settings import Microsoft

from .base import (
    ClientSpecificCaller,
    ClientSpecificError,
    ClientSpecificProtocol,
    ClientSpecificRequest,
    ResponseValidationError,
)

logger = log.getLogger(__name__)

_CLIENT_CHECK: contextvars.ContextVar[
    MicrosoftSpecificProtocol | None
] = contextvars.ContextVar("microsoft_client", default=None)


def get_client() -> MicrosoftSpecificProtocol:
    if (client := _CLIENT_CHECK.get()) is None:
        client = MicrosoftSpecificProtocol()
        _CLIENT_CHECK.set(client)
    return client


class MicrosoftResponse(TypedDict):
    insuranceType: Optional[str]
    state: Optional[str]
    country: Optional[str]


class MicrosoftSpecificProtocol(ClientSpecificProtocol[MicrosoftResponse]):
    def __init__(self):
        self.caller = MicrosoftSpecificCaller()

    def check_eligibility(
        self, response: MicrosoftResponse
    ) -> MicrosoftResponse | None:
        if response["insuranceType"] in self._INELIGIBLE:
            return None
        return response

    _INELIGIBLE = frozenset((None, "Waive Medical"))


def _read_private_key(private_key_location) -> str:
    """Reads the private key from the specified location."""
    with open(private_key_location) as file:
        return file.read()


def _read_certificate(certificate_location) -> Tuple[str, str]:
    """Reads the certificate and extracts the thumbprint."""
    with open(certificate_location) as file:
        public_certificate = file.read()
    cert = load_pem_x509_certificate(
        data=bytes(public_certificate, "UTF-8"), backend=default_backend()
    )
    thumbprint = cert.fingerprint(hashes.SHA1()).hex()
    return public_certificate, thumbprint


def _get_client_credential(
    private_key_path: str, certificate_path: str
) -> dict[str, str] | str:
    """Reads the private key and certificate, and extracts the thumbprint."""
    private_key = _read_private_key(private_key_location=private_key_path)
    public_certificate, thumbprint = _read_certificate(
        certificate_location=certificate_path
    )

    return {
        "private_key": private_key,
        "thumbprint": thumbprint,
        "public_certificate": public_certificate,
    }


def _get_private_key_path(msft_settings: Microsoft) -> str:
    """Reads the private key from settings or environment variable."""
    if msft_settings.private_key_path:
        return msft_settings.private_key_path
    return os.environ.get("MSFT_PRIVATE_KEY_PATH", "")


def _get_certificate_path(msft_settings: Microsoft) -> str:
    """Reads the certificate from settings or environment variable."""
    if msft_settings.certificate_path:
        return msft_settings.certificate_path
    return os.environ.get("MSFT_CERTIFICATE_PATH", "")


class MicrosoftSpecificCaller(ClientSpecificCaller[MicrosoftResponse]):
    def __init__(self):
        msft_settings = settings.Microsoft()
        super().__init__(url=msft_settings.url)
        self.auth_url = msft_settings.authority
        self.auth = msal.ConfidentialClientApplication(
            msft_settings.client_id,
            authority=msft_settings.authority,
            client_credential=_get_client_credential(
                private_key_path=_get_private_key_path(msft_settings=msft_settings),
                certificate_path=_get_certificate_path(msft_settings=msft_settings),
            ),
            # token_cache=...  # Default cache is in memory only.
            # You can learn how to use SerializableTokenCache from
            # https://msal-python.rtfd.io/en/latest/#msal.SerializableTokenCache
        )
        self.auth_scopes = [msft_settings.scope]
        self.validator: typic.ConstraintsProtocolT[
            MicrosoftResponse
        ] = typic.get_constraints(MicrosoftResponse)

    async def get_token(self) -> str:
        # Firstly, looks up a token from cache
        # Since we are looking for token for the current app, NOT for an end user,
        # notice we give account parameter as None.
        logger.info("Acquiring authentication token.")
        data: dict | None = self.auth.acquire_token_silent(
            scopes=self.auth_scopes, account=None
        )
        if not data:
            # msal uses a synchronous http client, let's call this blocking code on a separate thread.
            # see: https://github.com/AzureAD/microsoft-authentication-library-for-python/issues/88
            logger.debug("Contacting authority.", url=self.auth_url)
            data = await asyncio.to_thread(
                self.auth.acquire_token_for_client, scopes=self.auth_scopes
            )
        if self._TOKEN_KEY not in data:
            data["authority"] = self.auth_url
            raise MicrosoftAuthError(
                "Couldn't parse auth response.",
                context=data,
            )
        logger.info("Acquired authentication token.")
        return data[self._TOKEN_KEY]

    _TOKEN_KEY = "access_token"

    async def _get_headers(self) -> dict[str, str]:
        token = await self.get_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _get_payload(self, request: ClientSpecificRequest) -> MicrosoftRequestPayload:
        payload = {
            "EmployeeId": request.unique_corp_id,
            "IsEmployee": request.is_employee,
            "DateOfBirth": request.date_of_birth.isoformat(),
            "DependentDateOfBirth": (
                request.dependent_date_of_birth.isoformat()
                if request.dependent_date_of_birth
                else None
            ),
        }

        return payload

    async def _do_request(
        self, session: aiohttp.ClientSession, payload: dict
    ) -> aiohttp.ClientResponse:
        return await session.post(self.url, json=payload)

    def _do_validate(
        self, body: dict, *, response: aiohttp.ClientResponse = None
    ) -> MicrosoftResponse:
        try:
            return self.validator.validate(body)
        except typic.ConstraintValueError as e:
            raise ResponseValidationError(str(e), response=response) from e


class MicrosoftRequestPayload(TypedDict):
    EmployeeId: str
    IsEmployee: bool
    DateOfBirth: str
    DependentDateOfBirth: str = None


class MicrosoftAuthError(ClientSpecificError):
    ...
