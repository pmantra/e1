from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, Date, Integer
from sqlalchemy.dialects.postgresql import DATERANGE, JSONB, TEXT


class FileParseResults(Base):
    __tablename__ = "file_parse_results"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column("organization_id", Integer)
    file_id = Column("file_id", Integer)
    first_name = Column("first_name", TEXT)
    last_name = Column("last_name", TEXT)
    email = Column("email", TEXT)
    unique_corp_id = Column("unique_corp_id", TEXT)
    dependent_id = Column("dependent_id", TEXT)
    date_of_birth = Column("date_of_birth", Date)
    work_state = Column("work_state", TEXT)
    record = Column("record", JSONB)
    errors = Column("errors", TEXT)
    warnings = Column("warnings", TEXT)
    effective_range = Column("effective_range", DATERANGE)
    do_not_contact = Column("do_not_contact", TEXT)
    gender_code = Column("gender_code", TEXT)
    employer_assigned_id = Column("employer_assigned_id", TEXT)
    created_at = Column("created_at", TIMESTAMP)
    updated_at = Column("updated_at", TIMESTAMP)
