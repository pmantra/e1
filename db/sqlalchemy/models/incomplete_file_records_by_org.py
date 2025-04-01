from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import Column, Integer
from sqlalchemy.dialects.postgresql import JSONB


class IncompleteFilesByOrg(Base):
    __tablename__ = "incomplete_files_by_org"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    total_members = Column("total_members", Integer)
    config = Column("config", JSONB)
    incomplete = Column("incomplete", JSONB)
