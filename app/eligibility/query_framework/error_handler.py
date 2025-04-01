import functools

from mmlib.ops import log

from app.eligibility.constants import EligibilityMethod
from app.eligibility.query_framework.errors import MemberSearchError, ValidationError

logger = log.getLogger(__name__)


def eligibility_error_handler(method: EligibilityMethod):
    """Decorator to handle eligibility errors consistently."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            user_id = kwargs.get("user_id")
            method_name = method.value.lower().replace("_", "-")

            try:
                return await func(*args, **kwargs)

            except ValidationError as e:
                logger.warning(
                    f"Validation error in {method_name} eligibility check",
                    extra={
                        "error": str(e),
                        "fields": getattr(e, "fields", {}),
                        "user_id": user_id,
                    },
                )
                raise

            except MemberSearchError as e:
                # Ensure method is set properly - don't rely on e.method.value
                if not hasattr(e, "method") or e.method is None:
                    e.method = method

                logger.warning(
                    f"Member search error in {method_name} eligibility check",
                    extra={
                        "error": str(e),
                        "method": e.method.value
                        if (hasattr(e, "method") and e.method)
                        else method.value,
                        "user_id": user_id,
                    },
                )
                raise

            except Exception as e:
                logger.error(
                    f"Error in {method_name} eligibility check",
                    extra={
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "user_id": user_id,
                    },
                )
                # Convert generic exceptions to a structured error with method info
                raise MemberSearchError(
                    f"Error during eligibility check: {str(e)}",
                    method=method,
                ) from e

        return wrapper

    return decorator
