from __future__ import annotations

import http
import traceback
from typing import List

import structlog
from aiohttp import web
from http_api.client.base_view import BaseView
from http_api.client.utils import (
    convert_to_bool,
    create_member_response,
    create_verification_for_user_response,
)

from db.model import EligibilityVerificationForUser, MemberResponse

logger = structlog.getLogger(__name__)


def init_views(app: web.Application):
    app.router.add_view(
        "/api/v1/-/eligibility-api/get_member_by_id/{id}", GetMemberByIdView
    )
    app.router.add_view(
        "/api/v1/-/eligibility-api/check_standard_eligibility",
        CheckStandardEligibilityView,
    )
    app.router.add_view(
        "/api/v1/-/eligibility-api/get_sub_population_id/user/{user_id}/organization/{org_id}",
        GetSubPopulationIdView,
    )
    app.router.add_view(
        "/api/v1/-/eligibility-api/get_eligible_features/sub_population_id/{sub_population_id}/feature_type/{feature_type}",
        GetEligibleFeatureView,
    )
    app.router.add_view(
        "/api/v1/-/eligibility-api/get_verification_for_user",
        GetVerificationForUserView,
    )
    app.router.add_view(
        "/api/v1/-/eligibility-api/get_all_verifications_for_user",
        GetAllVerificationsForUserView,
    )


class GetAllVerificationsForUserView(BaseView):
    async def post(self):
        data = await self.request.json()

        user_id = data.get("user_id")
        organization_ids = data.get("organization_ids")
        active_verifications_only = data.get("active_verifications_only")

        if (
            not user_id
            or len(organization_ids) == 0
            or active_verifications_only is None
        ):
            logger.error(
                "Missing required parameters user_id, organization_ids or active_verifications_only in get_all_verifications_for_user"
            )
            return web.json_response(
                {
                    "error": "Missing required parameters: user_id, organization_ids or active_verifications_only"
                },
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            verifications_for_user: List[
                EligibilityVerificationForUser
            ] = await self.service.get_all_verifications_for_user(
                user_id=int(user_id),
                active_verifications_only=convert_to_bool(active_verifications_only),
                organization_ids=organization_ids,
            )

            logger.info(
                "Found verification record(s) for the user",
                user_id=user_id,
                organization_ids=organization_ids,
                active_verifications_only=active_verifications_only,
            )

            records = [
                create_verification_for_user_response(verification_for_user)
                for verification_for_user in verifications_for_user
            ]

            return web.json_response(
                data=records,
                status=http.HTTPStatus.OK,
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling get_all_verifications_for_user: {stack_trace}",
                error_message=str(e),
                error_type=str(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )


class GetVerificationForUserView(BaseView):
    async def post(self):
        data = await self.request.json()

        user_id = data.get("user_id")
        organization_id = data.get("organization_id")
        active_verifications_only = data.get("active_verifications_only")

        if not user_id or not organization_id or active_verifications_only is None:
            logger.error(
                "Missing required parameters user_id, organization_id or active_verifications_only in get_verification_for_user"
            )
            return web.json_response(
                {
                    "error": "Missing required parameters: user_id, organization_id or active_verifications_only"
                },
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            verification_for_user: EligibilityVerificationForUser = (
                await self.service.get_verification_for_user(
                    user_id=int(user_id),
                    active_verifications_only=convert_to_bool(
                        active_verifications_only
                    ),
                    organization_id=int(organization_id),
                )
            )

            logger.info(
                "Found verification record for the user",
                user_id=user_id,
                organization_id=organization_id,
                active_verifications_only=active_verifications_only,
            )

            return web.json_response(
                data=create_verification_for_user_response(verification_for_user),
                status=http.HTTPStatus.OK,
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling get_verification_for_user: {stack_trace}",
                error_message=str(e),
                error_type=str(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )


class GetEligibleFeatureView(BaseView):
    async def get(self):
        sub_population_id = self.request.match_info.get("sub_population_id", None)
        feature_type = self.request.match_info.get("feature_type", None)

        if not sub_population_id or not feature_type:
            logger.error(
                "Missing required query parameters sub_population_id or feature_type in get_eligible_features"
            )
            return web.json_response(
                {
                    "error": "Missing required query parameters: sub_population_id or feature_type"
                },
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            features: List[
                int
            ] | None = await self.service.get_eligible_features_by_sub_population_id(
                sub_population_id=int(sub_population_id),
                feature_type=int(feature_type),
            )

            logger.info(
                "Successfully fetched eligible features for the user",
                sub_population_id=sub_population_id,
                feature_type=feature_type,
            )

            return web.json_response(
                data={"features": features, "has_definition": features is not None},
                status=http.HTTPStatus.OK,
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling get_eligible_features: {stack_trace}",
                error_message=str(e),
                error_type=type(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )


class GetSubPopulationIdView(BaseView):
    async def get(self):
        user_id = self.request.match_info.get("user_id", None)
        org_id = self.request.match_info.get("org_id", None)

        if not user_id or not org_id:
            logger.error(
                "Missing required query parameters user_id or org_id in get_sub_population_id"
            )
            return web.json_response(
                {"error": "Missing required query parameters: user_id or org_id"},
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            sub_pop_id, _ = await self.service.get_sub_population_id_for_user_and_org(
                user_id=int(user_id),
                organization_id=int(org_id),
            )

            logger.info(
                "Successfully fetched sub-population ID for the user and organization",
                user_id=user_id,
                org_id=org_id,
                sub_pop_id=sub_pop_id,
            )

            return web.json_response(
                data={"sub_population_id": sub_pop_id}
                if sub_pop_id is not None
                else {},
                status=http.HTTPStatus.OK,
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling get_sub_population_id: {stack_trace}",
                error_message=str(e),
                error_type=type(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )


class GetMemberByIdView(BaseView):
    async def get(self):
        member_id = self.request.match_info.get("id", None)

        if member_id is None:
            logger.error("member_id is not set in /get_member_by_id/{id}")
            return web.json_response(
                data={},
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            member: MemberResponse = await self.service.get_by_member_id(
                id=int(member_id)
            )

            return web.json_response(
                data=create_member_response(member),
                status=http.HTTPStatus.OK,
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling get_by_member_id: {stack_trace}",
                error_message=str(e),
                error_type=type(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )


class CheckStandardEligibilityView(BaseView):
    async def post(self):
        data = await self.request.json()

        date_of_birth = data.get("date_of_birth")
        if date_of_birth is None:
            logger.error(
                "date_of_birth is not available in the request of /check_standard_eligibility"
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.BAD_REQUEST,
            )

        email = data.get("company_email")
        if email is None:
            logger.error(
                "email is not available in the request of /check_standard_eligibility"
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.BAD_REQUEST,
            )

        try:
            member: MemberResponse = await self.service.check_standard_eligibility(
                date_of_birth=date_of_birth,
                email=email,
            )

            return web.json_response(
                data=create_member_response(member),
                status=http.HTTPStatus.OK,
            )

        except Exception as e:
            stack_trace = traceback.format_exc()
            logger.error(
                f"Error in calling check_standard_eligibility: {stack_trace}",
                error_type=type(e),
                error_message=str(e),
            )
            return web.json_response(
                data={},
                status=http.HTTPStatus.INTERNAL_SERVER_ERROR,
            )
