import datetime

from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, ForeignKey, Integer
from sqlalchemy.dialects.postgresql import JSONB, TEXT
from sqlalchemy.orm import relationship


class SubPopulations(Base):
    """
    This class contains the SQLAlchemy model required for our E9y Flask implementation
    """

    __tablename__ = "sub_population"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    population_id = Column("population_id", ForeignKey("eligibility.population.id"))
    population = relationship("Populations", back_populates="sub_populations")
    feature_set_name = Column("feature_set_name", TEXT)
    feature_set_details_json = Column("feature_set_details_json", JSONB)
    created_at = Column(
        "created_at",
        TIMESTAMP,
        default=lambda: datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        ),
    )
    updated_at = Column(
        "updated_at",
        TIMESTAMP,
        default=lambda: datetime.datetime.now(datetime.timezone.utc).replace(
            microsecond=0
        ),
    )
