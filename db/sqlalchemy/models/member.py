import datetime

from db.sqlalchemy.models.common import MemberActivationReason
from db.sqlalchemy.models.configuration import Configurations
from db.sqlalchemy.models.file import File
from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import DATE, DATERANGE, JSONB, TEXT
from sqlalchemy.ext.hybrid import hybrid_property


class Members(Base):
    __tablename__ = "member"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column(
        "organization_id", Integer, ForeignKey(Configurations.organization_id)
    )
    first_name = Column("first_name", TEXT)
    last_name = Column("last_name", TEXT)
    date_of_birth = Column("date_of_birth", DATE)
    work_state = Column("work_state", TEXT)
    work_country = Column("work_country", TEXT)
    email = Column("email", TEXT)
    effective_range = Column("effective_range", DATERANGE)
    unique_corp_id = Column("unique_corp_id", TEXT)
    employer_assigned_id = Column("employer_assigned_id", TEXT)
    dependent_id = Column("dependent_id", TEXT)
    file_id = Column("file_id", Integer, ForeignKey(File.id))
    custom_attributes = Column("custom_attributes", JSONB)
    record = Column("record", JSONB)
    do_not_contact = Column("do_not_contact", TEXT)
    gender_code = Column("gender_code", TEXT)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)

    @hybrid_property
    def active(self) -> str:
        """Determine whether or not a member is considered 'active'
        A member is active if the current date is within the upper and lower bounds of our effective_range for that member
        i.e. they are eligible for Maven AND the member belongs to an organization that has been activated and is not terminated

        We return a string here, rather than bool, because flask admin will attempt to use a +/- icon for booleans- this can be
        difficult to read
        """

        effective_range_lower = self.effective_range.lower
        effective_range_upper = self.effective_range.upper
        current_date = datetime.date.today()

        # Check first the user belongs to an active org
        # Return false if the org isn't activated, or the activation date is in the future
        if (
            not self.configurations.activated_at
            or self.configurations.activated_at > current_date
        ):
            return "False"

        # Return false if our org has been terminated today or in the past
        if (
            self.configurations.terminated_at
            and self.configurations.terminated_at <= current_date
        ):
            return "False"

        # If either upper or lower effective_ranges are null, set them to the default values
        if not effective_range_lower:
            effective_range_lower = datetime.date.min
        if not effective_range_upper:
            effective_range_upper = datetime.date.max

        if not effective_range_lower <= current_date < effective_range_upper:
            return "False"
        return "True"

    @hybrid_property
    def reason_member_inactive(self) -> str:
        """When looking at a user, we should try to provide a hint to why they may be considered "inactive"
        The below will generate helpful text for use in quickly determining why a given member is not considered active
        or eligible for Maven
        """

        effective_range_lower = self.effective_range.lower
        effective_range_upper = self.effective_range.upper
        current_date = datetime.date.today()

        # Check first the member belongs to an active org
        # Return inactive if the org isn't activated, or the activation date is in the future
        if (
            not self.configurations.activated_at
            or self.configurations.activated_at > current_date
        ):
            return MemberActivationReason.ORGANIZATION_INACTIVATED.value

        # Return false if our org has been terminated today or in the past
        if (
            self.configurations.terminated_at
            and self.configurations.terminated_at <= current_date
        ):
            return MemberActivationReason.ORGANIZATION_TERMINATED.value

        # If either upper or lower effective_ranges are null, set them to the default values
        if not effective_range_lower:
            effective_range_lower = datetime.date.min
        if not effective_range_upper:
            effective_range_upper = datetime.date.max

        if effective_range_lower > current_date:
            return MemberActivationReason.NOT_YET_ELIGIBLE.value

        if effective_range_upper < current_date:
            return MemberActivationReason.ELIGIBILITY_EXPIRED.value

        return MemberActivationReason.ACTIVE.value
