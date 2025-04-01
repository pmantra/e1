from db.sqlalchemy.models.configuration import Configurations
from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import DATE, JSONB, TEXT, UUID


class Verifications2(Base):
    __tablename__ = "verification_2"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column(
        "organization_id", Integer, ForeignKey(Configurations.organization_id)
    )
    user_id = Column("user_id", Integer)

    unique_corp_id = Column("unique_corp_id", TEXT)
    dependent_id = Column("dependent_id", TEXT)
    first_name = Column("first_name", TEXT)
    last_name = Column("last_name", TEXT)
    email = Column("email", TEXT)
    date_of_birth = Column("date_of_birth", DATE)
    work_state = Column("work_state", TEXT)
    verification_type = Column("verification_type", TEXT)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
    deactivated_at = Column("deactivated_at", TIMESTAMP)
    verified_at = Column("verified_at", TIMESTAMP)
    additional_fields = Column("additional_fields", JSONB)
    verification_session = Column("verification_session", UUID)
    member_id = Column("member_id", Integer)
    member_version = Column("member_version", Integer)
