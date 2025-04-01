from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, Integer
from sqlalchemy.dialects.postgresql import TEXT

### THIS FILE IS CURRENTLY NOT USED, BUT MAY HAVE USE IN THE FUTURE- SAVING IT HERE JUST FOR POSTERITY


class File(Base):
    __tablename__ = "file"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column("organization_id", Integer)
    name = Column("name", TEXT)
    encoding = Column("encoding", TEXT)
    started_at = Column("started_at", TIMESTAMP)
    completed_at = Column("completed_at", TIMESTAMP)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
