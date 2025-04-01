from __future__ import annotations

import os

import grpc
from maven_schemas import eligibility_pb2 as e9ypb
from maven_schemas import eligibility_pb2_grpc as e9ygrpc
import structlog.stdlib

logger = structlog.getLogger(__name__)


class EligibilityServiceStub:
    def __init__(self):
        host = os.environ.get("ELIGIBILITY_GRPC_SERVER_HOST", "eligibility-api")
        port = os.environ.get("ELIGIBILITY_GRPC_SERVER_PORT", 50051)
        e9y_channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = e9ygrpc.EligibilityServiceStub(e9y_channel)

    def deactivate_verification(
        self, verification_id: int, user_id: int
    ) -> e9ypb.DeactivateVerificationForUserResponse:
        request = e9ypb.DeactivateVerificationForUserRequest(
            verification_id=verification_id,
            user_id=user_id,
        )
        deactivate_response = self.stub.DeactivateVerificationForUser(request)
        return deactivate_response
