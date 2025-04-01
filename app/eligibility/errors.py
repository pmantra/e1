from __future__ import annotations

import asyncpg

from app.eligibility import constants


class EligibilityError(Exception):
    ...


class MatchError(EligibilityError, ValueError):
    method: constants.EligibilityMethod

    def __init__(self, *args):
        args = args or ("Matching member not found.",)
        super().__init__(*args)


class MemberSearchError(EligibilityError, ValueError):
    method: constants.EligibilityMethod

    def __init__(self, *args, method: constants.EligibilityMethod = None):
        self.method = method
        args = args or ("Matching member not found.",)
        super().__init__(*args)


class InactiveOrganizationError(MatchError):
    def __init__(self, method: constants.EligibilityMethod, message: str = None):
        message = (
            message
            or f"Member record(s) found by {method.value} belong to inactive organization."
        )
        super().__init__(message)
        self.method = method


class UnsupportedReturnTypeError(MatchError):
    def __init__(self, method: constants.EligibilityMethod, message: str = None):
        message = message or f"Unsupported return type for {method.value}."
        super().__init__(message)
        self.method = method


class MatchMultipleError(EligibilityError, ValueError):
    method: constants.EligibilityMethod

    def __init__(self, *args):
        args = args or ("multiple members found.",)
        super().__init__(*args)


class StandardMatchMultipleError(MatchMultipleError):
    method = constants.EligibilityMethod.STANDARD


class AlternateMatchMultipleError(MatchMultipleError):
    method = constants.EligibilityMethod.ALTERNATE


class StandardMatchError(MatchError):
    method = constants.EligibilityMethod.STANDARD


class AlternateMatchError(MatchError):
    method = constants.EligibilityMethod.ALTERNATE


class OverEligibilityError(MatchError):
    method = constants.EligibilityMethod.OVERELIGIBILITY


class ClientSpecificMatchError(MatchError):
    method = constants.EligibilityMethod.CLIENT_SPECIFIC

    def __init__(self, implementation, *args):
        self.implementation = implementation
        super().__init__("Could not find a member with provided credentials.", *args)


class NoDobMatchError(MatchError):
    method = constants.EligibilityMethod.NO_DOB

    def __init__(self, *args):
        super().__init__("Could not find a member with email and name.", *args)


class BasicEligibilityMatchError(MatchError):
    method = constants.EligibilityMethod.BASIC


class HealthPlanEligibilityMatchError(MatchError):
    method = constants.EligibilityMethod.HEALTH_PLAN


class GetMatchError(MatchError):
    method = constants.EligibilityMethod.GET_BY_ID


class IdentityMatchError(MatchError):
    method = constants.EligibilityMethod.GET_BY_ORG_IDENTITY


class ConfigurationError(EligibilityError, ValueError):
    method: constants.EligibilityMethod


class ClientSpecificConfigurationError(ConfigurationError):
    method = constants.EligibilityMethod.CLIENT_SPECIFIC


class UpstreamClientSpecificException(EligibilityError):
    method = constants.EligibilityMethod.CLIENT_SPECIFIC

    def __init__(self, implementation, upstream_exc):
        self.implementation = implementation
        super().__init__(
            "This client specific check is currently unavailable due to an upstream error: "
            f"[{implementation.value}] {upstream_exc!r}"
        )


class CreateVerificationError(EligibilityError):
    method = constants.EligibilityMethod.CREATE_VERIFICATION_FOR_USER


class RecordAlreadyClaimedError(EligibilityError):
    method = constants.EligibilityMethod.CREATE_VERIFICATION_FOR_USER


class DeactivateVerificationError(EligibilityError):
    method = constants.EligibilityMethod.DEACTIVATE_VERIFICATION_FOR_USER


class OrganizationNotFound(asyncpg.exceptions.ForeignKeyViolationError):
    method = constants.EligibilityMethod.CREATE_TEST_MEMBER_RECORDS_FOR_ORGANIZATION
