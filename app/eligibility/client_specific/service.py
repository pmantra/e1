from __future__ import annotations

import datetime
from typing import Callable, Protocol

import ddtrace
from ddtrace import tracer
from mmlib.ops import log, stats

from app.eligibility import constants, errors
from app.eligibility.client_specific import microsoft
from app.eligibility.client_specific.base import (
    ClientSpecificProtocol,
    ClientSpecificRequest,
)
from app.utils import feature_flag
from config import settings
from constants import POD
from db import model
from db.clients import member_2_client, member_client, member_versioned_client

logger = log.getLogger(__name__)

# mapping of a client specific implementation to its relevant settings
IMPL_TO_SETTINGS = {model.ClientSpecificImplementation.MICROSOFT: settings.Microsoft}


class ClientSpecificService:
    """An abstraction layer over individual ClientSpecificProtocol implementations.

    This is the main entrypoint into Client-Specific logic.
    """

    def __init__(
        self,
        members: member_client.Members,
        members_versioned: member_versioned_client.MembersVersioned,
        members_2: member_2_client.Member2Client,
    ):
        self.members = members
        self.members_versioned = members_versioned
        self.members_2 = members_2

    def perform_client_specific_verification(
        self,
        is_employee: bool,
        date_of_birth: datetime.date,
        organization_id: int,
        unique_corp_id: str,
        implementation: model.ClientSpecificImplementation,
        dependent_date_of_birth: datetime.date = None,
    ):
        mode = self._get_mode(implementation)
        ddtrace.tracer.set_tags(
            {
                "client_specific.mode": mode.value,
                "client_specific.implementation": implementation.value,
                "maven.organization_id": organization_id,
            }
        )
        if mode not in self._CLIENT_CHECKS:
            # This block will only be reached if someone added a new ClientSpecificMode but did not implement it.
            raise NotImplementedError(
                f"Please define the process for carrying out a client specific check with mode: {mode}"
            )

        request = ClientSpecificRequest(
            is_employee=is_employee,
            unique_corp_id=unique_corp_id,
            date_of_birth=date_of_birth,
            dependent_date_of_birth=dependent_date_of_birth,
        )
        operation = self._CLIENT_CHECKS[mode]
        return operation(
            self,
            request=request,
            organization_id=organization_id,
            implementation=implementation,
        )

    async def _perform_client_specific_check(
        self,
        request: ClientSpecificRequest,
        organization_id: int,
        implementation: model.ClientSpecificImplementation,
    ) -> model.Member:
        client_protocol: ClientSpecificProtocol = get_client_specific_protocol(
            implementation
        )
        with tracer.trace("client_specific_check"):
            try:
                logger.info(
                    "Starting client specific check.",
                    implementation=implementation,
                    unique_corp_id=request.unique_corp_id,
                )
                client_response = await client_protocol.verify(request)
            except Exception as e:
                logger.error(
                    "Client check encountered an unhandled exception.",
                    exception=repr(e),
                    implementation=implementation,
                    unique_corp_id=request.unique_corp_id,
                )
                raise errors.UpstreamClientSpecificException(implementation, e) from e

        if client_response is None:
            logger.info(
                "Client check found no match.",
                implementation=implementation,
                unique_corp_id=request.unique_corp_id,
            )

            raise errors.ClientSpecificMatchError(implementation)

        logger.info(
            "Client check indicated that member was eligible.",
            check_result_keys=client_response.keys(),
            country_eligible=client_response.get("country", ""),
            implementation=implementation,
            unique_corp_id=request.unique_corp_id,
        )
        # Copy some data from the request into the member record.
        record = {"is_employee": request.is_employee, **client_response}
        # Client check members will not have a database PK or first and last name.
        return model.Member(
            id=0,
            first_name="",
            last_name="",
            date_of_birth=request.date_of_birth,
            record=record,
            unique_corp_id=request.unique_corp_id,
            organization_id=organization_id,
            created_at=datetime.datetime.utcnow(),
            updated_at=datetime.datetime.utcnow(),
        )

    async def _perform_census_check(
        self,
        request: ClientSpecificRequest,
        organization_id: int,
        implementation: model.ClientSpecificImplementation,
    ) -> model.Member | model.Member2:

        if feature_flag.organization_enabled_for_e9y_2_write(organization_id):
            census_member = await self.members_2.get_by_client_specific_verification(
                organization_id=organization_id,
                unique_corp_id=request.unique_corp_id,
                date_of_birth=request.date_of_birth,
            )
        else:
            census_member = (
                await self.members_versioned.get_by_client_specific_verification(
                    organization_id=organization_id,
                    unique_corp_id=request.unique_corp_id,
                    date_of_birth=request.date_of_birth,
                )
            )

        if census_member is None:
            logger.info(
                "Census check found no match.",
                implementation=implementation,
                unique_corp_id=request.unique_corp_id,
            )
            raise errors.ClientSpecificMatchError(implementation)

        return census_member

    async def _perform_client_specific_census_fallback(
        self,
        request: ClientSpecificRequest,
        organization_id: int,
        implementation: model.ClientSpecificImplementation,
    ):
        try:
            return await self._perform_client_specific_check(
                request, organization_id, implementation
            )
        except (
            errors.ClientSpecificMatchError,
            errors.UpstreamClientSpecificException,
        ) as e:
            logger.info(
                "Falling back to census check.",
                implementation=implementation,
                unique_corp_id=request.unique_corp_id,
            )
            try:
                census_member = await self._perform_census_check(
                    request, organization_id, implementation
                )
                logger.info(
                    "Recovered client specific check by falling back to census.",
                    implementation=implementation,
                    unique_corp_id=request.unique_corp_id,
                )
                stats.increment(
                    metric_name=f"{_CHECK_CLIENT_SPECIFIC_PREFIX}.census_fallback_eligible",
                    tags=[f"implementation:{implementation.value}"],
                    pod_name=POD,
                )
                return census_member
            except errors.ClientSpecificMatchError:
                logger.info(
                    "Both client check and census check found no match.",
                    implementation=implementation,
                    unique_corp_id=request.unique_corp_id,
                )
                raise e from None

    _CLIENT_CHECKS: dict[model.ClientSpecificMode, _ClientSpecificOperationT] = {
        model.ClientSpecificMode.ONLY_CLIENT_CHECK: _perform_client_specific_check,
        model.ClientSpecificMode.ONLY_CENSUS: _perform_census_check,
        model.ClientSpecificMode.FALLBACK_TO_CENSUS: _perform_client_specific_census_fallback,
    }

    @staticmethod
    def _get_mode(
        implementation: model.ClientSpecificImplementation,
    ) -> model.ClientSpecificMode:  # pragma: no cover
        if (client_setting := IMPL_TO_SETTINGS.get(implementation)) is None:
            raise NotImplementedError(
                f"{implementation.value} is not a valid implementation"
            )

        return model.ClientSpecificMode(client_setting().mode)


class _ClientSpecificOperationT(Protocol):
    async def __call__(
        _,
        self: ClientSpecificService,
        request: ClientSpecificRequest,
        organization_id: int,
        implementation: model.ClientSpecificImplementation,
    ) -> model.Member:
        ...


def get_client_specific_protocol(
    implementation: model.ClientSpecificImplementation,
) -> ClientSpecificProtocol:
    return _IMPL_TO_CHECK_TYPE[implementation]()


async def initialize():
    for protocol in _IMPL_TO_CHECK_TYPE.values():
        protocol()


async def teardown():
    # no-op
    ...


_IMPL_TO_CHECK_TYPE: dict[
    model.ClientSpecificImplementation, Callable[[], ClientSpecificProtocol]
]
_IMPL_TO_CHECK_TYPE = {
    model.ClientSpecificImplementation.MICROSOFT: microsoft.get_client,
}
assert all(
    i in _IMPL_TO_CHECK_TYPE for i in model.ClientSpecificImplementation
), "Please define a ClientCheck for new client specific implementation."


_CHECK_CLIENT_SPECIFIC_PREFIX = (
    f"{constants.STATS_PREFIX}.CheckClientSpecificEligibility"
)
