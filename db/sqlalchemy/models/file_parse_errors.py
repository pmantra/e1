from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, Integer
from sqlalchemy.dialects.postgresql import JSONB, TEXT


class FileParseErrors(Base):
    __tablename__ = "file_parse_errors"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    file_id = Column("file_id", Integer)
    organization_id = Column("organization_id", Integer)
    record = Column("record", JSONB)
    errors = Column("errors", TEXT)
    warnings = Column("warnings", TEXT)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
