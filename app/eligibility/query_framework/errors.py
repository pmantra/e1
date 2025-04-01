from app.eligibility import constants
from app.eligibility.errors import EligibilityError, MatchError


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


class ValidationError(ValueError):
    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields
