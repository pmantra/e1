from mmlib.ops import log

logger = log.getLogger(__name__)


def grpc_to_http_status_code(grpc_status_code):
    try:
        status_code = str(grpc_status_code).split(".")[-1]
        code_to_http_status_map = {
            "OK": 200,
            "CANCELLED": 499,
            "UNKNOWN": 500,
            "INVALID_ARGUMENT": 400,
            "DEADLINE_EXCEEDED": 504,
            "NOT_FOUND": 404,
            "ALREADY_EXISTS": 409,
            "PERMISSION_DENIED": 403,
            "RESOURCE_EXHAUSTED": 429,
            "FAILED_PRECONDITION": 400,
            "ABORTED": 409,
            "OUT_OF_RANGE": 400,
            "UNIMPLEMENTED": 501,
            "INTERNAL": 500,
            "UNAVAILABLE": 503,
            "DATA_LOSS": 500,
            "UNAUTHENTICATED": 401,
        }
        return code_to_http_status_map.get(status_code, 500)
    except Exception as e:
        logger.error("status code error occurred: %s", e)
        return 500
