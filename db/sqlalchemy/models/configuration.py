from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import BOOLEAN, TIMESTAMP, Column, Integer
from sqlalchemy.dialects.postgresql import TEXT


class Configurations(Base):
    __tablename__ = "configuration"
    __table_args__ = {"schema": "eligibility"}

    organization_id = Column("organization_id", Integer, primary_key=True)
    directory_name = Column("directory_name", TEXT)
    email_domains = Column("email_domains", TEXT)
    implementation = Column("implementation", TEXT)
    data_provider = Column("data_provider", BOOLEAN)
    activated_at = Column("activated_at", TIMESTAMP)
    terminated_at = Column("terminated_at", TIMESTAMP)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
    eligibility_type = Column("eligibility_type", TEXT)
