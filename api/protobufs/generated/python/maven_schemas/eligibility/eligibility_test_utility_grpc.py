# Generated by the Protocol Buffers compiler. DO NOT EDIT!
# source: maven-schemas/eligibility/eligibility_test_utility.proto
# plugin: grpclib.plugin.main
import abc
import typing

import grpclib.client
import grpclib.const

if typing.TYPE_CHECKING:
    import grpclib.server

import maven_schemas.eligibility.eligibility_test_utility_pb2
import maven_schemas.eligibility_pb2


class EligibilityTestUtilityServiceBase(abc.ABC):
    @abc.abstractmethod
    async def CreateEligibilityMemberTestRecordsForOrganization(
        self,
        stream: "grpclib.server.Stream[maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationRequest, maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationResponse]",
    ) -> None:
        pass

    def __mapping__(self) -> typing.Dict[str, grpclib.const.Handler]:
        return {
            "/maven.eligibility.eligibility_test_utility.EligibilityTestUtilityService/CreateEligibilityMemberTestRecordsForOrganization": grpclib.const.Handler(
                self.CreateEligibilityMemberTestRecordsForOrganization,
                grpclib.const.Cardinality.UNARY_UNARY,
                maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationRequest,
                maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationResponse,
            ),
        }


class EligibilityTestUtilityServiceStub:
    def __init__(self, channel: grpclib.client.Channel) -> None:
        self.CreateEligibilityMemberTestRecordsForOrganization = grpclib.client.UnaryUnaryMethod(
            channel,
            "/maven.eligibility.eligibility_test_utility.EligibilityTestUtilityService/CreateEligibilityMemberTestRecordsForOrganization",
            maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationRequest,
            maven_schemas.eligibility.eligibility_test_utility_pb2.CreateEligibilityMemberTestRecordsForOrganizationResponse,
        )
