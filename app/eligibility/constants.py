from __future__ import annotations

import enum
from typing import Final

STATS_PREFIX: Final[str] = "eligibility.app.service"

# This is not ideal to hardcode organization_ids here
# however we only have handful of them and is not worth moving these to db
# refer to https://mavenclinic.slack.com/archives/C04J215TG4R/p1713292978825119
# {morganstanley=620, morganstanleycanada=685, morganstanleyemea=686, morganstanleyinternational=2049,
#  publicis=601, mission support=484, Kraft-Heinz Canada=2475}
ORGANIZATIONS_NOT_SENDING_DOB = frozenset(
    {620, 685, 686, 696, 2031, 2049, 601, 2475, 484}
)


class EligibilityMethod(str, enum.Enum):
    STANDARD = "standard"
    ALTERNATE = "alternate"
    OVERELIGIBILITY = "overeligibility"
    CLIENT_SPECIFIC = "client-specific"
    NO_DOB = "no-dob"
    GET_BY_ID = "get-by-id"
    GET_BY_ORG_IDENTITY = "get-by-org-identity"
    CREATE_VERIFICATION_FOR_USER = "create-verification-for-user"
    DEACTIVATE_VERIFICATION_FOR_USER = "deactivate-verification-for-user"
    CREATE_TEST_MEMBER_RECORDS_FOR_ORGANIZATION = "create-test-member-records"
    BASIC = "basic"
    EMPLOYER = "employer"
    HEALTH_PLAN = "healthplan"


class ProcessingResult(enum.IntEnum):
    NO_RECORDS_FOUND = 0
    ERROR_DURING_PROCESSING = -1
    PROCESSING_SUCCESSFUL = 1
    FILE_MISSING = 2
    BAD_FILE_ENCODING = 3


class ProcessingTag(str, enum.Enum):
    ERROR = "error"
    PERSIST = "persist"
    DELETE = "delete"


class MatchType(enum.IntEnum):
    """Enumerate MatchTypes that indicate whether a user has potential or existing eligibility records"""

    POTENTIAL = 0
    """Given user's first, last names and date of birth, eligibility record(s) exist for this user
    user_id is not used searching for eligibility records"""

    POTENTIAL_CURRENT_ORGANIZATION = 1
    """Given user's first, last names, date of birth and user_id, additional eligibility record(s) are available 
    for this user that match the user's organization"""

    POTENTIAL_OTHER_ORGANIZATION = 2
    """Given user's first, last names, date of birth and user_id, eligibility record(s) are available for this user
    that are different from the user's organization"""

    EXISTING_ELIGIBILITY = 3
    """Given user's first, last names, date of birth and user_id, only the existing eligibility record is found
    for this user"""

    UNKNOWN_ELIGIBILITY = 4
    """Given user's first, last names, date of birth and/or user_id, no eligibility record is found for this user"""

    NONE = 5
    """Default match type used for handling errors"""


class E9yFeatureFlag(str, enum.Enum):
    RELEASE_ELIGIBILITY_DATABASE_INSTANCE_SWITCH = (
        "release-eligibility-database-instance-switch"
    )
    RELEASE_MICROSOFT_CERT_BASED_AUTH = "release-msft-cert-based-auth"
    RELEASE_ELIGIBILITY_2_ENABLED_ORGS_READ = (
        "release-eligibility-2-enabled-orgs-for-read"
    )
    RELEASE_ELIGIBILITY_2_ENABLED_ORGS_WRITE = (
        "release-eligibility-2-enabled-orgs-for-write"
    )
    RELEASE_OVER_ELIGIBILITY = "eligibility_enrollments_overeligibility_v1"
    RELEASE_OVER_ELIGIBILITY_ENABLED_ORGS = (
        "release-eligibility-overeligibility-enabled-orgs"
    )
    E9Y_DISABLE_WRITE = "e9y-disable-write"
    RELEASE_OPTUM_FILE_LOGGING_SWITCH = "release-optum-file-logging-switch"
