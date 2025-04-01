from db.sqlalchemy.models.configuration import Configurations
from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import BOOLEAN, DATE, JSONB, TEXT


class VerificationAttempts(Base):
    __tablename__ = "verification_attempt"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column(
        "organization_id", Integer, ForeignKey(Configurations.organization_id)
    )
    verification_type = Column("verification_type", TEXT)
    date_of_birth = Column("date_of_birth", DATE)
    verification_id = Column("verification_id", Integer)
    first_name = Column("first_name", TEXT)
    last_name = Column("last_name", TEXT)
    user_id = Column("user_id", Integer)
    email = Column("email", TEXT)
    unique_corp_id = Column("unique_corp_id", TEXT)
    dependent_id = Column("dependent_id", TEXT)
    work_state = Column("work_state", TEXT)
    policy_used = Column("policy_used", JSONB)
    successful_verification = Column("successful_verification", BOOLEAN)
    verified_at = Column("verified_at", TIMESTAMP)
    additional_fields = Column("additional_fields", JSONB)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
