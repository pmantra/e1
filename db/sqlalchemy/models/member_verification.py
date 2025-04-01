from db.sqlalchemy.models.member_versioned import MemberVersioned
from db.sqlalchemy.models.verification import Verifications
from db.sqlalchemy.models.verification_attempt import VerificationAttempts
from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer


class MemberVerifications(Base):
    __tablename__ = "member_verification"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    member_id = Column("member_id", Integer, ForeignKey(MemberVersioned.id))
    verification_attempt_id = Column(
        "verification_attempt_id", Integer, ForeignKey(VerificationAttempts.id)
    )
    verification_id = Column("verification_id", Integer, ForeignKey(Verifications.id))
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
