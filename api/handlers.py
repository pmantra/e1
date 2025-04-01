from __future__ import annotations

import contextlib
import datetime
import json
from typing import List

import ddtrace
import grpclib.server
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import Int64Value
from google.rpc.error_details_pb2 import BadRequest, ErrorInfo
from maven_schemas import eligibility_grpc as e9ygrpc
from maven_schemas import eligibility_pb2 as e9ypb
from maven_schemas.eligibility import eligibility_test_utility_grpc as teste9ygrpc
from maven_schemas.eligibility import eligibility_test_utility_pb2 as teste9ypb
from maven_schemas.eligibility import pre_eligibility_grpc as pre9ygrpc
from maven_schemas.eligibility import pre_eligibility_pb2 as pre9ypb
from mmlib.grpc.server import Handler, health_check
from mmlib.ops import log

from app.eligibility import errors, query_service, service, translate
from app.eligibility.pre_eligibility import (
    has_existing_eligibility,
    has_potential_eligibility_in_current_org,
    has_potential_eligibility_in_other_org,
    is_active,
)
from app.eligibility.query_framework import MemberSearchError
from config import settings
from db import model
from db.model import Member2

LOG = log.get_logger(__name__)


class EligibilityService(e9ygrpc.EligibilityServiceBase, Handler):
    @property
    def service(self) -> service.EligibilityService:
        return service.service()

    @property
    def query_service(self) -> query_service.EligibilityQueryService:
        return query_service.query_service()

    async def CheckStandardEligibility(self, stream):
        _log = LOG.bind(method="standard")
        _log.info("Checking for employee.")
        async with handle_e9y_errors(stream) as request:
            member = await self.service.check_standard_eligibility(
                date_of_birth=request.date_of_birth,
                email=request.company_email,
            )
            _log.info("Found a member for the provided information.")
            await stream.send_message(_create_member_response(member))

    async def CheckAlternateEligibility(self, stream):
        _log = LOG.bind(method="alternate")
        _log.info("Checking for employee.")
        async with handle_e9y_errors(stream) as request:
            member = await self.service.check_alternate_eligibility(
                date_of_birth=request.date_of_birth,
                first_name=request.first_name,
                last_name=request.last_name,
                work_state=request.work_state,
                unique_corp_id=request.unique_corp_id,
            )
            _log.info("Found a member for the provided information.")
            await stream.send_message(_create_member_response(member))

    async def CheckClientSpecificEligibility(self, stream):
        _log = LOG.bind(method="client_specific")
        _log.info("Checking for employee.")
        async with handle_e9y_errors(stream) as request:
            member = await self.service.check_client_specific_eligibility(
                is_employee=request.is_employee,
                organization_id=request.organization_id,
                unique_corp_id=request.unique_corp_id,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.dependent_date_of_birth,
            )
            await stream.send_message(_create_member_response(member))

    async def CheckNoDOBEligibility(self, stream):
        _log = LOG.bind(method="no_dob")
        _log.info("Checking for employee.")
        async with handle_e9y_errors(stream) as request:
            member = (
                await self.service.check_organization_specific_eligibility_without_dob(
                    email=request.email,
                    first_name=request.first_name,
                    last_name=request.last_name,
                )
            )
            await stream.send_message(_create_member_response(member))

    async def CheckEligibilityOverEligibility(self, stream):
        _log = LOG.bind(method="overeligibility")
        _log.info("Checking for employee via overeligibility.")
        async with handle_e9y_errors(stream) as request:
            member_list = await self.service.check_overeligibility(
                date_of_birth=request.date_of_birth,
                work_state=request.work_state,
                first_name=request.first_name,
                last_name=request.last_name,
                unique_corp_id=request.unique_corp_id,
                email=request.company_email,
                user_id=int(request.user_id),
            )

            _log.info("Found member(s) for the provided information.")
            await stream.send_message(_create_member_list_response(member_list))

    async def CheckBasicEligibility(self, stream):
        _log = LOG.bind(method="basic_eligibility")
        _log.info("Checking for member records via basic eligibility.")
        async with handle_e9y_errors(stream) as request:
            member_list = await self.query_service.check_basic_eligibility(
                user_id=request.user_id,
                date_of_birth=request.date_of_birth,
                first_name=request.first_name,
                last_name=request.last_name,
            )

            _log.info("Found member record(s) using basic eligibility verification.")
            await stream.send_message(_create_member_list_response(member_list))

    async def CheckEmployerEligibility(self, stream):
        _log = LOG.bind(method="employer_eligibility")
        _log.info("Checking for member records via employer eligibility.")
        async with handle_e9y_errors(stream) as request:
            member = await self.query_service.check_employer_eligibility(
                user_id=request.user_id,
                email=request.company_email,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.dependent_date_of_birth,
                employee_first_name=request.employee_first_name,
                employee_last_name=request.employee_last_name,
                first_name=request.first_name,
                last_name=request.last_name,
                work_state=request.work_state,
            )

            _log.info("Found member record using employer verification.")
            await stream.send_message(_create_member_response(member))

    async def CheckHealthPlanEligibility(self, stream):
        _log = LOG.bind(method="healthplan_eligibility")
        _log.info("Checking for member records via healthplan eligibility.")
        async with handle_e9y_errors(stream) as request:
            member = await self.query_service.check_healthplan_eligibility(
                user_id=request.user_id,
                date_of_birth=request.date_of_birth,
                dependent_date_of_birth=request.dependent_date_of_birth,
                first_name=request.first_name,
                last_name=request.last_name,
                unique_corp_id=request.subscriber_id,
                employee_first_name=request.employee_first_name,
                employee_last_name=request.employee_last_name,
            )

            _log.info("Found member record using healthplan verification.")
            await stream.send_message(_create_member_response(member))

    async def GetMemberById(self, stream):
        async with handle_e9y_errors(stream) as request:
            member_id = request.id
            _log = LOG.bind(member_id=member_id)
            _log.info("Checking for member by ID.")
            member = await self.service.get_by_member_id(id=member_id)
            _log.info("Found a member for the provided information.")
            await stream.send_message(_create_member_response(member))

    async def GetMemberByOrgIdentity(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                method="org_identity",
                organization_id=request.organization_id,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
            )
            _log.info("Checking for member.")
            member = await self.service.get_by_org_identity(
                organization_id=request.organization_id,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
            )
            _log.info("Found a member for the provided information.")
            await stream.send_message(_create_member_response(member))

    async def GetWalletEnablementById(self, stream):
        async with handle_e9y_errors(stream) as request:
            member_id = request.id
            _log = LOG.bind(member_id=member_id)
            _log.info("Checking for wallet enablement by ID.")
            enablement = await self.service.get_wallet_enablement(member_id=member_id)
            _log.info("Found a wallet enablement for the provided information.")
            await stream.send_message(_create_wallet_enablement_response(enablement))

    async def GetWalletEnablementByUserId(self, stream):
        async with handle_e9y_errors(stream) as request:
            user_id = request.id
            _log = LOG.bind(user_id=user_id)
            _log.info("Checking for wallet enablement by user ID.")
            enablement = await self.service.get_wallet_enablement_by_user_id(
                user_id=user_id
            )
            _log.info("Found a wallet enablement for the provided information.")
            await stream.send_message(_create_wallet_enablement_response(enablement))

    async def GetWalletEnablementByOrgIdentity(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                organization_id=request.organization_id,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
            )
            _log.info("Checking for wallet enablement by org identity.")
            enablement = await self.service.get_wallet_enablement_by_identity(
                organization_id=request.organization_id,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
            )
            _log.info("Found a wallet enablement for the provided information.")
            await stream.send_message(_create_wallet_enablement_response(enablement))

    async def GetVerificationForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                user_id=request.user_id,
                organization_id=request.organization_id,
                active_verifications_only=request.active_verifications_only,
            )
            _log.info("Getting eligibility verification record by user id.")
            organization_id = None
            if request.organization_id != "":
                organization_id = int(request.organization_id)

            verification_for_user = await self.service.get_verification_for_user(
                user_id=request.user_id,
                organization_id=organization_id,
                active_verifications_only=request.active_verifications_only,
            )
            _log.info("Found verification record for the user")
            await stream.send_message(
                _create_verification_for_user_response(verification_for_user)
            )

    async def GetAllVerificationsForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                user_id=request.user_id,
                organization_ids=request.organization_ids,
                active_verifications_only=request.active_verifications_only,
            )
            _log.info("Getting all eligibility verification records by user id.")

            verifications_for_user = await self.service.get_all_verifications_for_user(
                user_id=request.user_id,
                organization_ids=request.organization_ids,
                active_verifications_only=request.active_verifications_only,
            )

            _log.info("Found verification record(s) for the user")

            e9ypb_records = []
            for verification_for_user in verifications_for_user:
                e9ypb_record = _create_verification_for_user_response(
                    verification_for_user
                )
                e9ypb_records.append(e9ypb_record)

            await stream.send_message(
                e9ypb.VerificationList(verification_list=e9ypb_records)
            )

    async def CreateVerificationForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            # Handle optional values that may have default values of "" set
            eligibility_member_id = None
            if request.eligibility_member_id != "":
                eligibility_member_id = int(request.eligibility_member_id)
            user_id = None
            if request.user_id != "":
                user_id = int(request.user_id)
            additional_fields = None
            if request.additional_fields != "":
                additional_fields = json.loads(request.additional_fields)

            _log = LOG.bind(
                user_id=request.user_id,
                organization_id=request.organization_id,
                eligibility_member_id=eligibility_member_id,
                unique_corp_id=request.unique_corp_id,
            )
            _log.info("Creating verification for user")
            user_verification = await self.service.create_verification_for_user(
                user_id=user_id,
                eligibility_member_id=eligibility_member_id,
                organization_id=request.organization_id,
                verification_type=request.verification_type,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
                first_name=request.first_name,
                last_name=request.last_name,
                date_of_birth=request.date_of_birth,
                email=request.email,
                work_state=request.work_state,
                verified_at=request.verified_at,
                additional_fields=additional_fields,
                verification_session=request.verification_session,
            )
            _log.info("Successfully created verification for user")
            await stream.send_message(
                _create_verification_for_user_response(user_verification)
            )

    async def CreateMultipleVerificationsForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            user_id = request.user_id.value if request.HasField("user_id") else None

            eligibility_member_ids = []
            organization_ids = []
            unique_corp_ids = []
            translated_verification_data_list = []
            for verification_data in request.verification_data_list:
                # Check if set
                if (
                    verification_data.eligibility_member_id
                    and verification_data.eligibility_member_id.value
                ):
                    eligibility_member_ids.append(
                        verification_data.eligibility_member_id.value
                    )

                organization_ids.append(verification_data.organization_id)
                unique_corp_ids.append(verification_data.unique_corp_id)
                translated_verification_data_list.append(
                    convert_verification_data_pb_to_verification_data(verification_data)
                )

            _log = LOG.bind(
                user_id=user_id,
                organization_ids=organization_ids,
                eligibility_member_ids=eligibility_member_ids,
                unique_corp_ids=unique_corp_ids,
            )
            _log.info("Creating multiple verifications for user")
            verifications = await self.service.create_multiple_verifications_for_user(
                verification_data_list=translated_verification_data_list,
                verification_type=request.verification_type,
                first_name=request.first_name,
                last_name=request.last_name,
                user_id=user_id,
                verified_at=request.verified_at,
                date_of_birth=request.date_of_birth,
                verification_session=request.verification_session,
            )
            _log.info("Successfully created verification(s) for user")
            e9ypb_records = []
            for verification in verifications:
                verification_for_user = _create_verification_for_user_response(
                    verification
                )
                e9ypb_records.append(verification_for_user)
            await stream.send_message(
                e9ypb.VerificationList(verification_list=e9ypb_records)
            )

    async def CreateFailedVerification(self, stream):
        async with handle_e9y_errors(stream) as request:

            # Handle optional values that may have default values of "" set
            organization_id, eligibility_member_id, user_id, additional_fields = (
                None,
                None,
                None,
                None,
            )
            if request.organization_id != "":
                organization_id = int(request.organization_id)
            if request.eligibility_member_id != "":
                eligibility_member_id = int(request.eligibility_member_id)
            if request.user_id != "":
                user_id = int(request.user_id)
            if request.additional_fields != "":
                additional_fields = json.loads(request.additional_fields)
            _log = LOG.bind(
                organization_id=request.organization_id,
                eligibility_member_id=request.eligibility_member_id,
                unique_corp_id=request.unique_corp_id,
            )
            _log.info("Creating failed verification attempt")
            verification_attempt = await self.service.create_failed_verification(
                verification_type=request.verification_type,
                date_of_birth=request.date_of_birth,
                unique_corp_id=request.unique_corp_id,
                dependent_id=request.dependent_id,
                first_name=request.first_name,
                last_name=request.last_name,
                email=request.email,
                work_state=request.work_state,
                eligibility_member_id=eligibility_member_id,
                organization_id=organization_id,
                policy_used=request.policy_used,
                verified_at=request.verified_at,
                additional_fields=additional_fields,
                user_id=user_id,
            )

            _log.info("Successfully created failed_verification record")
            await stream.send_message(
                _create_failed_verification_response(verification_attempt)
            )

    @ddtrace.tracer.wrap()
    async def DeactivateVerificationForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                verification_id=request.verification_id,
                user_id=request.user_id,
            )
            _log.info("Deactivating verification record for user")
            deactivated_record = await self.service.deactivate_verification_for_user(
                verification_id=request.verification_id,
                user_id=request.user_id,
            )
            _log.info("Successfully deactivated verification record for user")
            await stream.send_message(
                _deactivate_verification_record_for_user_response(
                    verification_record=deactivated_record
                )
            )

    async def GetEligibleFeaturesForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                user_id=request.user_id,
                feature_type=request.feature_type,
            )
            _log.info("Getting eligible features for user")
            features = await self.service.get_eligible_features_for_user(
                user_id=request.user_id,
                feature_type=request.feature_type,
            )
            _log.info("Successfully fetched eligible features for the user")
            await stream.send_message(
                _create_get_eligible_features_for_user_response(features)
            )

    async def GetEligibleFeaturesForUserAndOrg(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                user_id=request.user_id,
                organization_id=request.organization_id,
                feature_type=request.feature_type,
            )
            _log.info("Getting eligible features for user")
            features = await self.service.get_eligible_features_for_user_and_org(
                user_id=request.user_id,
                organization_id=request.organization_id,
                feature_type=request.feature_type,
            )
            _log.info("Successfully fetched eligible features for the user")
            await stream.send_message(
                _create_get_eligible_features_for_user_and_org_response(features)
            )

    async def GetEligibleFeaturesBySubPopulationId(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(
                sub_population_id=request.sub_population_id,
                feature_type=request.feature_type,
            )
            _log.info("Getting eligible features for user")
            features = await self.service.get_eligible_features_by_sub_population_id(
                sub_population_id=request.sub_population_id,
                feature_type=request.feature_type,
            )
            _log.info("Successfully fetched eligible features for the user")
            await stream.send_message(
                _create_get_eligible_features_by_sub_population_id_response(features)
            )

    async def GetSubPopulationIdForUser(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(user_id=request.user_id)
            _log.info("Getting sub-population ID for user")
            sub_pop_id, _ = await self.service.get_sub_population_id_for_user(
                user_id=request.user_id,
            )
            _log.info("Successfully fetched sub-population ID for the user")
            await stream.send_message(
                _create_get_sub_population_id_for_user_response(sub_pop_id)
            )

    async def GetSubPopulationIdForUserAndOrg(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(user_id=request.user_id)
            _log.info("Getting sub-population ID for user and organization")
            sub_pop_id, _ = await self.service.get_sub_population_id_for_user_and_org(
                user_id=request.user_id, organization_id=request.organization_id
            )
            _log.info(
                "Successfully fetched sub-population ID for the user and organization"
            )
            await stream.send_message(
                _create_get_sub_population_id_for_user_and_org_response(sub_pop_id)
            )

    async def GetOtherUserIdsInFamily(self, stream):
        async with handle_e9y_errors(stream) as request:
            _log = LOG.bind(user_id=request.user_id)
            _log.info(
                "Getting other user_id's that share the same unique_corp_id for user"
            )
            user_ids = await self.service.get_other_user_ids_in_family(
                user_id=request.user_id,
            )
            _log.info("Successfully fetched family's user_ids for the user")
            await stream.send_message(
                _create_get_other_user_ids_in_family_response(user_ids)
            )


@contextlib.asynccontextmanager
async def handle_e9y_errors(stream: grpclib.server.Stream) -> Message:
    message = await stream.recv_message()
    try:
        yield message
    except service.ValidationError as e:
        await stream.send_trailing_metadata(
            status=grpclib.Status.INVALID_ARGUMENT,
            status_message=str(e),
            status_details=[
                BadRequest(
                    field_violations=[
                        BadRequest.FieldViolation(
                            field=f, description=f"Got unsupported value: {v!r}."
                        )
                        for f, v in e.fields.items()
                    ]
                )
            ],
        )
    except (errors.MatchError, MemberSearchError) as e:
        status = grpclib.Status.NOT_FOUND
        await stream.send_trailing_metadata(
            status=grpclib.Status.NOT_FOUND,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": str(e.method) if hasattr(e, "method") else "None",
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except errors.MatchMultipleError as e:
        status = grpclib.Status.NOT_FOUND
        method_value = (
            e.method.value if hasattr(e, "method") and e.method else "unknown"
        )
        await stream.send_trailing_metadata(
            status=grpclib.Status.NOT_FOUND,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": method_value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except errors.ConfigurationError as e:
        status = grpclib.Status.UNIMPLEMENTED
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except errors.UpstreamClientSpecificException as e:
        status = grpclib.Status.UNAVAILABLE
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except errors.CreateVerificationError as e:
        status = grpclib.Status.INTERNAL
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )

    except errors.RecordAlreadyClaimedError as e:
        status = grpclib.Status.ALREADY_EXISTS  # 409 error code
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )

    except errors.DeactivateVerificationError as e:
        status = grpclib.Status.INTERNAL
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except Exception as e:
        status = grpclib.Status.UNKNOWN
        method_value = (
            e.method.value if hasattr(e, "method") and e.method else "unknown"
        )
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": method_value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )


@contextlib.asynccontextmanager
async def handle_pre9y_errors(stream: grpclib.server.Stream) -> Message:
    """
    separate error handler for pre-eligibility service
    this handles Match Error to return a success response
    with MATCH_TYPE.UNKNOWN instead of returning an error response
    """
    message = await stream.recv_message()
    try:
        yield message
    except service.ValidationError as e:
        await stream.send_trailing_metadata(
            status=grpclib.Status.INVALID_ARGUMENT,
            status_message=str(e),
            status_details=[
                BadRequest(
                    field_violations=[
                        BadRequest.FieldViolation(
                            field=f, description=f"Got unsupported value: {v!r}."
                        )
                        for f, v in e.fields.items()
                    ]
                )
            ],
        )
    except errors.MatchError as e:
        # return response with MatchType.UNKNOWN instead of returning an error response
        # the error response is creating unnecessary alerts and is not necessary
        _log = LOG.bind(
            error=e,
        )
        _log.info("No match found in e9y")
        await stream.send_message(
            _create_pre_eligibility_response(member=None, matching_records=[])
        )
    except errors.ConfigurationError as e:
        status = grpclib.Status.UNIMPLEMENTED
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )
    except errors.UpstreamClientSpecificException as e:
        status = grpclib.Status.UNAVAILABLE
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )


@contextlib.asynccontextmanager
async def handle_e9y_test_errors(stream: grpclib.server.Stream) -> Message:
    """
    separate error handler for eligibility-test service
    this handles all errors associated with eligibility-test
    """
    message = await stream.recv_message()
    try:
        yield message
    except service.ValidationError as e:
        await stream.send_trailing_metadata(
            status=grpclib.Status.INVALID_ARGUMENT,
            status_message=str(e),
            status_details=[
                BadRequest(
                    field_violations=[
                        BadRequest.FieldViolation(
                            field=f, description=f"Got unsupported value: {v!r}."
                        )
                        for f, v in e.fields.items()
                    ]
                )
            ],
        )
    # handle non-existent organization_id
    except errors.OrganizationNotFound:
        await stream.send_trailing_metadata(
            status=grpclib.Status.INVALID_ARGUMENT,
            status_message="Organization not found",
            status_details=[
                BadRequest(
                    field_violations=[
                        BadRequest.FieldViolation(
                            description="Organization with that id doesn't exist"
                        )
                    ]
                )
            ],
        )
    # handle environment error
    except EnvironmentError or errors.ConfigurationError as e:
        status = grpclib.Status.UNIMPLEMENTED
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                )
            ],
        )
    except errors.UpstreamClientSpecificException as e:
        status = grpclib.Status.UNAVAILABLE
        await stream.send_trailing_metadata(
            status=status,
            status_message=str(e),
            status_details=[
                ErrorInfo(
                    reason=status.name,
                    domain="eligibility",
                    metadata={
                        "method": e.method.value,
                        "providedFields": ",".join(
                            fd.name for fd, v in message.ListFields()
                        ),
                    },
                )
            ],
        )


def _create_member_response(
    member: model.Member | model.MemberVersioned | Member2 | model.MemberResponse,
) -> e9ypb.Member:
    effective_range = None
    if member.effective_range:
        lower = member.effective_range.lower
        upper = member.effective_range.upper
        effective_range = e9ypb.DateRange(
            lower=lower and lower.isoformat(),
            upper=upper and upper.isoformat(),
            lower_inc=member.effective_range.lower_inc,
            upper_inc=member.effective_range.upper_inc,
        )
    created_at = Timestamp()
    created_at.FromDatetime(member.created_at)
    updated_at = Timestamp()
    updated_at.FromDatetime(member.updated_at)
    prepped_custom_attributes = None
    # Create a shallow copy of custom attributes to make all non-string and non-bytes values into strings
    if member.custom_attributes is not None:
        prepped_custom_attributes = member.custom_attributes.copy()
        for key, value in prepped_custom_attributes.items():
            if not isinstance(value, (str, bytes)):
                prepped_custom_attributes[key] = translate.dump(value)

    version = (
        member.version
        if (isinstance(member, model.MemberResponse) or isinstance(member, Member2))
        else 0
    )
    file_id = member.file_id if not isinstance(member, Member2) else 0
    is_v2 = member.is_v2 if isinstance(member, model.MemberResponse) else False
    member_1_id = (
        member.member_1_id if isinstance(member, model.MemberResponse) else None
    )
    member_2_id = (
        member.member_2_id if isinstance(member, model.MemberResponse) else None
    )
    member_2_version = version if is_v2 else None

    return e9ypb.Member(
        id=member_1_id or member.id,
        record=translate.dump(member.record),
        custom_attributes=prepped_custom_attributes,
        organization_id=member.organization_id,
        file_id=file_id,
        first_name=member.first_name,
        last_name=member.last_name,
        date_of_birth=member.date_of_birth.isoformat(),
        work_state=member.work_state,
        work_country=member.work_country,
        email=member.email,
        unique_corp_id=member.unique_corp_id,
        dependent_id=member.dependent_id,
        effective_range=effective_range,
        employer_assigned_id=member.employer_assigned_id,
        created_at=created_at,
        updated_at=updated_at,
        version=version,
        is_v2=is_v2,
        member_1_id=member_1_id,
        member_2_id=member_2_id,
        member_2_version=member_2_version,
    )


def _create_member_list_response(
    memberList: [model.MemberResponse],
) -> e9ypb.MemberList:
    if memberList == []:
        return e9ypb.MemberList(member_list=[])
    else:
        return_val = []
        for m in memberList:
            return_val.append(_create_member_response(m))
        return e9ypb.MemberList(member_list=return_val)


def _create_pre_eligibility_response(
    member: model.Member | None,
    matching_records: [model.Member],
) -> pre9ypb.PreEligibilityResponse:
    match_type = get_match_type(
        member=member,
        matching_records=matching_records,
    )
    pre_eligibility_organizations = get_pre_eligibility_organizations(
        matching_records=matching_records
    )
    return pre9ypb.PreEligibilityResponse(
        match_type=match_type,
        pre_eligibility_organizations=pre_eligibility_organizations,
    )


def _create_test_eligibility_members_response(
    members: List[e9ypb.Member],
) -> teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationResponse:
    # Create a list to hold the transformed members
    transformed_members = []

    # Iterate over the input members
    for member in members:
        # Transform each member and append it to the transformed_members list
        e9ypb_member = _create_member_response(member)
        transformed_members.append(e9ypb_member)

    # Return the transformed members wrapped in a response object
    return teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationResponse(
        members=transformed_members
    )


def _create_wallet_enablement_response(
    enablement: model.WalletEnablementResponse,
) -> e9ypb.WalletEnablement:
    created_at = Timestamp()
    created_at.FromDatetime(enablement.created_at)
    updated_at = Timestamp()
    updated_at.FromDatetime(enablement.updated_at)
    start_date = (
        enablement.start_date.isoformat()
        if enablement.start_date
        else enablement.start_date
    )
    eligibility_end_date = (
        enablement.effective_range.upper.isoformat()
        if enablement.effective_range.upper
        else None
    )
    eligibility_date = (
        enablement.eligibility_date.isoformat() if enablement.eligibility_date else None
    )

    return e9ypb.WalletEnablement(
        member_id=enablement.member_1_id or enablement.member_id,
        organization_id=enablement.organization_id,
        enabled=enablement.enabled,
        insurance_plan=enablement.insurance_plan,
        start_date=start_date,
        eligibility_date=eligibility_date,
        created_at=created_at,
        updated_at=updated_at,
        eligibility_end_date=eligibility_end_date,
        is_v2=enablement.is_v2,
        member_1_id=enablement.member_1_id,
        member_2_id=enablement.member_2_id,
    )


def _create_verification_for_user_response(
    eligibility_verification_for_user: model.EligibilityVerificationForUser,
) -> e9ypb.VerificationForUser:
    # convert verification_created_at from datetime to pb2 Timestamp
    verification_created_at = Timestamp()
    verification_created_at.FromDatetime(
        eligibility_verification_for_user.verification_created_at
    )

    # convert verification_updated_at from datetime to pb2 Timestamp
    verification_updated_at = Timestamp()
    verification_updated_at.FromDatetime(
        eligibility_verification_for_user.verification_updated_at
    )

    # convert verified_at from datetime to pb2 Timestamp
    verified_at = Timestamp()
    verified_at.FromDatetime(eligibility_verification_for_user.verified_at)

    # convert verification_deactivated_at from datetime to pb2 Timestamp
    verification_deactivated_at = None
    if eligibility_verification_for_user.verification_deactivated_at:
        verification_deactivated_at = Timestamp()
        verification_deactivated_at.FromDatetime(
            eligibility_verification_for_user.verification_deactivated_at
        )

    # convert DOB to string if populated
    if eligibility_verification_for_user.date_of_birth:
        date_of_birth = eligibility_verification_for_user.date_of_birth.isoformat()
    else:
        date_of_birth = None

    effective_range = None
    # populate effective range
    if eligibility_verification_for_user.effective_range:
        lower = eligibility_verification_for_user.effective_range.lower
        upper = eligibility_verification_for_user.effective_range.upper
        effective_range = e9ypb.DateRange(
            lower=lower and lower.isoformat(),
            upper=upper and upper.isoformat(),
            lower_inc=eligibility_verification_for_user.effective_range.lower_inc,
            upper_inc=eligibility_verification_for_user.effective_range.upper_inc,
        )

    # If null, cast to blank string- otherwise will us "NONE" as the return value
    if eligibility_verification_for_user.eligibility_member_id is None:
        eligibility_member_id = ""
    else:
        eligibility_member_id = str(
            eligibility_verification_for_user.eligibility_member_id
        )

    if eligibility_verification_for_user.eligibility_member_version is None:
        eligibility_member_version = ""
    else:
        eligibility_member_version = str(
            eligibility_verification_for_user.eligibility_member_version
        )

    if eligibility_verification_for_user.eligibility_member_2_id is None:
        eligibility_member_2_id = ""
    else:
        eligibility_member_2_id = str(
            eligibility_verification_for_user.eligibility_member_2_id
        )

    if eligibility_verification_for_user.eligibility_member_2_version is None:
        eligibility_member_2_version = ""
    else:
        eligibility_member_2_version = str(
            eligibility_verification_for_user.eligibility_member_2_version
        )
    # Cast our record to a string

    if eligibility_verification_for_user.record:
        record = json.dumps(eligibility_verification_for_user.record)
    else:
        record = "{}"
    if eligibility_verification_for_user.additional_fields:
        additional_fields = json.dumps(
            eligibility_verification_for_user.additional_fields
        )
    else:
        additional_fields = "{}"

    verification_session = ""
    if eligibility_verification_for_user.verification_session:
        verification_session = str(
            eligibility_verification_for_user.verification_session
        )

    # convert to grpc response
    return e9ypb.VerificationForUser(
        verification_id=eligibility_verification_for_user.verification_id,
        user_id=eligibility_verification_for_user.user_id,
        organization_id=eligibility_verification_for_user.organization_id,
        eligibility_member_id=eligibility_member_id,
        first_name=eligibility_verification_for_user.first_name,
        last_name=eligibility_verification_for_user.last_name,
        date_of_birth=date_of_birth,
        unique_corp_id=eligibility_verification_for_user.unique_corp_id,
        dependent_id=eligibility_verification_for_user.dependent_id,
        work_state=eligibility_verification_for_user.work_state,
        email=eligibility_verification_for_user.email,
        record=record,
        verification_type=eligibility_verification_for_user.verification_type,
        employer_assigned_id=eligibility_verification_for_user.employer_assigned_id,
        effective_range=effective_range,
        verification_created_at=verification_created_at,
        verification_updated_at=verification_updated_at,
        verification_deactivated_at=verification_deactivated_at,
        gender_code=eligibility_verification_for_user.gender_code,
        do_not_contact=eligibility_verification_for_user.do_not_contact,
        verified_at=verified_at,
        additional_fields=additional_fields,
        verification_session=verification_session,
        eligibility_member_version=eligibility_member_version,
        is_v2=eligibility_verification_for_user.is_v2,
        verification_1_id=eligibility_verification_for_user.verification_1_id,
        verification_2_id=eligibility_verification_for_user.verification_2_id,
        eligibility_member_2_id=eligibility_member_2_id,
        eligibility_member_2_version=eligibility_member_2_version,
    )


def _create_failed_verification_response(
    verification_attempt: model.VerificationAttemptResponse,
) -> e9ypb.VerificationAttempt:
    # convert _created_at from datetime to pb2 Timestamp
    attempt_created_at = Timestamp()
    attempt_created_at.FromDatetime(verification_attempt.created_at)
    # convert updated_at from datetime to pb2 Timestamp
    attempt_updated_at = Timestamp()
    attempt_updated_at.FromDatetime(verification_attempt.updated_at)
    # convert verified_at from datetime to pb2 Timestamp
    attempt_verified_at = Timestamp()
    attempt_verified_at.FromDatetime(verification_attempt.verified_at)

    # Handle optional values that may have default values of "" set
    organization_id, user_id = "", ""
    if verification_attempt.organization_id:
        organization_id = str(verification_attempt.organization_id)

    if verification_attempt.user_id:
        user_id = str(verification_attempt.user_id)

    if verification_attempt.additional_fields:
        additional_fields = json.dumps(verification_attempt.additional_fields)
    else:
        additional_fields = "{}"

    # convert DOB to string if populated
    if verification_attempt.date_of_birth:
        date_of_birth = verification_attempt.date_of_birth.isoformat()
    else:
        date_of_birth = None

    eligibility_member_id = ""
    if verification_attempt.eligibility_member_id is not None:
        eligibility_member_id = str(verification_attempt.eligibility_member_id)

    eligibility_member_2_id = ""
    if verification_attempt.eligibility_member_2_id is not None:
        eligibility_member_2_id = str(verification_attempt.eligibility_member_2_id)

    # convert to grpc response
    return e9ypb.VerificationAttempt(
        verification_attempt_id=verification_attempt.id,
        user_id=user_id,
        organization_id=organization_id,
        unique_corp_id=verification_attempt.unique_corp_id,
        dependent_id=verification_attempt.dependent_id,
        first_name=verification_attempt.first_name,
        last_name=verification_attempt.last_name,
        email=verification_attempt.email,
        date_of_birth=date_of_birth,
        work_state=verification_attempt.work_state,
        verification_type=verification_attempt.verification_type,
        policy_used="",
        successful_verification=verification_attempt.successful_verification,
        created_at=attempt_created_at,
        updated_at=attempt_updated_at,
        verified_at=attempt_verified_at,
        additional_fields=additional_fields,
        is_v2=verification_attempt.is_v2,
        verification_attempt_1_id=verification_attempt.verification_attempt_1_id,
        verification_attempt_2_id=verification_attempt.verification_attempt_2_id,
        eligibility_member_id=eligibility_member_id,
        eligibility_member_2_id=eligibility_member_2_id,
    )


def _create_get_eligible_features_for_user_response(
    features: list[int] | None,
) -> e9ypb.GetEligibleFeaturesForUserResponse:
    return e9ypb.GetEligibleFeaturesForUserResponse(
        features=features,
        has_population=features is not None,
    )


def _create_get_eligible_features_for_user_and_org_response(
    features: list[int] | None,
) -> e9ypb.GetEligibleFeaturesForUserAndOrgResponse:
    return e9ypb.GetEligibleFeaturesForUserAndOrgResponse(
        features=features,
        has_population=features is not None,
    )


def _create_get_eligible_features_by_sub_population_id_response(
    features: list[int] | None,
) -> e9ypb.GetEligibleFeaturesBySubPopulationIdResponse:
    return e9ypb.GetEligibleFeaturesBySubPopulationIdResponse(
        features=features,
        has_definition=features is not None,
    )


def _create_get_sub_population_id_for_user_response(
    sub_population_id: int | None,
) -> e9ypb.GetSubPopulationIdForUserResponse:
    if sub_population_id is None:
        return e9ypb.GetSubPopulationIdForUserResponse()

    return e9ypb.GetSubPopulationIdForUserResponse(
        sub_population_id=Int64Value(value=sub_population_id),
    )


def _create_get_sub_population_id_for_user_and_org_response(
    sub_population_id: int | None,
) -> e9ypb.GetSubPopulationIdForUserAndOrgResponse:
    if sub_population_id is None:
        return e9ypb.GetSubPopulationIdForUserAndOrgResponse()

    return e9ypb.GetSubPopulationIdForUserAndOrgResponse(
        sub_population_id=Int64Value(value=sub_population_id),
    )


def _create_get_other_user_ids_in_family_response(
    user_ids: List[int],
) -> e9ypb.GetOtherUserIdsInFamilyResponse:
    return e9ypb.GetOtherUserIdsInFamilyResponse(
        user_ids=user_ids,
    )


def _deactivate_verification_record_for_user_response(
    verification_record: model.Verification,
) -> e9ypb.DeactivateVerificationForUserResponse:
    # convert verification_deactivated_at from datetime to pb2 Timestamp
    verification_deactivated_at = None
    if verification_record.deactivated_at:
        verification_deactivated_at = Timestamp()
        verification_deactivated_at.FromDatetime(verification_record.deactivated_at)
    return e9ypb.DeactivateVerificationForUserResponse(
        verification_id=verification_record.id,
        deactivated_at=verification_deactivated_at,
    )


@health_check()
async def system_is_healthy():  # pragma: no cover
    """This is a hook into the GRPC health check api https://github.com/grpc/grpc/blob/master/doc/health-checking.md

    This function feeds into the grpc health probe on the pod that can impact liveliness and readiness probes (if
    defined in k8s)."""
    return True


class EligibilityTestUtilityService(
    teste9ygrpc.EligibilityTestUtilityServiceBase, Handler
):
    """
    class contains the implementation of EligibilityTestUtilityService gRPC api
    """

    app_settings = settings.App()

    @property
    def e9y_test_utility_service(self) -> service.EligibilityTestUtilityService:
        return service.eligibility_test_utility_service()

    @ddtrace.tracer.wrap()
    async def CreateEligibilityMemberTestRecordsForOrganization(
        self, stream
    ) -> teste9ypb.CreateEligibilityMemberTestRecordsForOrganizationResponse:
        """Create eligibility test member records for an organization.

        This method creates eligibility test member records for the specified organization
        using the provided stream of data. It logs information about the operation and handles
        errors gracefully.

        Args:
            self: The class instance.
            stream: A stream of data containing information about the organization and test
                member records.

        Raises:
            Any exceptions raised during the operation will be handled by the
            handle_e9y_test_errors context manager.

        Returns:
            None. The method sends a response message through the stream to indicate
            the completion of the operation.
        """
        _log = LOG.bind(method="CreateEligibilityMemberTestRecordsForOrganization")
        async with handle_e9y_test_errors(stream) as request:
            organization_id = request.organization_id
            if request.organization_id != "":
                organization_id = int(request.organization_id)
            _log.info(
                "Attempting to create Test Members for Organization",
                organization_id=organization_id,
            )
            # check environment and make sure it is non-prod
            if not self.is_non_prod():
                raise EnvironmentError(
                    "This action is not allowed in production environment"
                )
            test_records = request.test_member_records
            # translate to model
            translated_test_records = []
            for test_record in test_records:
                translated = model.MemberTestRecord(
                    organization_id=organization_id,
                    first_name=test_record.first_name,
                    last_name=test_record.last_name,
                    date_of_birth=test_record.date_of_birth,
                    email=test_record.email,
                    dependent_id=test_record.dependent_id,
                    unique_corp_id=test_record.unique_corp_id,
                    effective_range=test_record.effective_range,
                    work_state=test_record.work_state,
                    work_country=test_record.work_country,
                )
                translated_test_records.append(translated)
            members = (
                await self.e9y_test_utility_service.create_members_for_organization(
                    organization_id=organization_id,
                    test_records=translated_test_records,
                )
            )

            await stream.send_message(
                _create_test_eligibility_members_response(members)
            )

    def is_non_prod(self):
        environment = self.app_settings.environment
        if environment in ["production", "prod"]:
            return False
        return True


class PreEligibilityService(pre9ygrpc.PreEligibilityServiceBase, Handler):
    """
    class contains the implementation of CheckPreEligibility gRPC endpoint
    """

    @property
    def pre9y_service(self) -> service.PreEligibilityService:
        return service.pre_eligibility_service()

    @property
    def e9y_service(self) -> service.EligibilityService:
        return service.service()

    @ddtrace.tracer.wrap()
    async def CheckPreEligibility(self, stream):
        _log = LOG.bind(method="get_members_by_name_and_date_of_birth")
        _log.info("Checking for member pre-eligibility.")
        async with handle_pre9y_errors(stream) as request:
            member_id = request.member_id
            _log = LOG.bind(method="pre_eligibility", member_id=member_id)
            _log.info("Checking for member pre-eligibility")

            has_member_id = request.member_id is not None
            has_first_name = request.first_name is not None
            has_last_name = request.last_name is not None
            has_date_of_birth = request.date_of_birth is not None
            _log.info(
                "Request contains below input params",
                has_member_id=has_member_id,
                has_first_name=has_first_name,
                has_last_name=has_last_name,
                has_date_of_birth=has_date_of_birth,
            )

            matching_records = (
                await self.pre9y_service.get_members_by_name_and_date_of_birth(
                    first_name=request.first_name,
                    last_name=request.last_name,
                    date_of_birth=request.date_of_birth,
                )
            )
            if matching_records:
                _log.info("Found member record(s) with matching name and date of birth")
            member = None
            if member_id:
                member = await self.e9y_service.get_by_member_id_from_member(
                    id=member_id
                )
                _log.info("Found a member with the provided member id")
            await stream.send_message(
                _create_pre_eligibility_response(
                    member=member, matching_records=matching_records
                )
            )


def get_match_type(
    member: model.Member, matching_records: [model.Member]
) -> pre9ypb.MatchType:
    """
    determines MatchType based on member's existing e9y record and records that match this member's name and dob
    @param member: known e9y record if it exists
    @param matching_records: records matching name and dob
    @return: MatchType
    """
    if member is None:
        # existing e9y is not known and e9y records exist with name and dob
        if len(matching_records) > 0:
            return pre9ypb.MatchType.POTENTIAL
        return pre9ypb.MatchType.UNKNOWN_ELIGIBILITY
    elif has_existing_eligibility(member, matching_records):
        return pre9ypb.MatchType.EXISTING_ELIGIBILITY
    elif has_potential_eligibility_in_current_org(member, matching_records):
        return pre9ypb.MatchType.POTENTIAL_CURRENT_ORGANIZATION
    elif has_potential_eligibility_in_other_org(member, matching_records):
        return pre9ypb.MatchType.POTENTIAL_OTHER_ORGANIZATION
    else:
        return pre9ypb.MatchType.UNKNOWN_ELIGIBILITY


def get_pre_eligibility_organizations(
    matching_records: [model.Member],
) -> [pre9ypb.PreEligibilityOrganization]:
    """
    translate to pre_eligibility_organizations
    """
    orgs = []
    for record in matching_records:
        if is_active(record):
            end_date = record.effective_range.upper
            eligibility_end_date = None
            if end_date is not None:
                end_time = datetime.datetime.combine(end_date, datetime.time.min)
                eligibility_end_date = Timestamp()
                eligibility_end_date.FromDatetime(end_time)
            org = pre9ypb.PreEligibilityOrganization(
                organization_id=record.organization_id,
                eligibility_end_date=eligibility_end_date,
            )
            orgs.append(org)
    return orgs


def convert_verification_data_pb_to_verification_data(
    verification_data,
) -> model.VerificationData:
    return model.VerificationData(
        eligibility_member_id=verification_data.eligibility_member_id.value
        if verification_data.eligibility_member_id is not None
        else None,
        organization_id=verification_data.organization_id,
        unique_corp_id=verification_data.unique_corp_id,
        dependent_id=verification_data.dependent_id,
        email=verification_data.email,
        work_state=verification_data.work_state,
        additional_fields=verification_data.additional_fields,
        verification_attempt_id=None,
        verification_id=None,
        member_1_id=None,
        member_2_id=None,
        member_2_version=None,
    )
