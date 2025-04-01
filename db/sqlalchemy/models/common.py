from enum import Enum


class MemberActivationReason(Enum):
    ORGANIZATION_INACTIVATED = "Member's organization not yet activated"
    ORGANIZATION_TERMINATED = "Member's organization has been terminated"
    NOT_YET_ELIGIBLE = "Member not yet eligible for Maven"
    ELIGIBILITY_EXPIRED = "Member's eligibility period has expired"
    ACTIVE = "Member is active"

    def __str__(self):
        return str(self.value)
