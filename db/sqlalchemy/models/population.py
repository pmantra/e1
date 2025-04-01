import datetime
from typing import Optional

from db.sqlalchemy.sqlalchemy_config import Base
from sqlalchemy import TIMESTAMP, Column, Integer
from sqlalchemy.dialects.postgresql import BOOLEAN, JSONB, TEXT
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.sql import and_, or_


class Populations(Base):
    """
    This class contains the SQLAlchemy model required for our E9y Flask implementation
    """

    __tablename__ = "population"
    __table_args__ = {"schema": "eligibility"}

    id = Column("id", Integer, primary_key=True)
    organization_id = Column("organization_id", Integer)
    activated_at = Column("activated_at", TIMESTAMP)
    deactivated_at = Column("deactivated_at", TIMESTAMP)
    sub_pop_lookup_keys_csv = Column("sub_pop_lookup_keys_csv", TEXT)
    sub_pop_lookup_map_json = Column("sub_pop_lookup_map_json", JSONB)
    advanced = Column("advanced", BOOLEAN)
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
    sub_populations = relationship(
        "SubPopulations",
        back_populates="population",
        order_by="SubPopulations.feature_set_name.asc()",
    )

    @hybrid_property
    def active(self) -> bool:
        """
        Determine whether or not a population is considered 'active'

        A population is active if deactivated_at is None or set in the future
        """
        activation_datetime: Optional[datetime.datetime] = self.activated_at
        deactivation_datetime: Optional[datetime.datetime] = self.deactivated_at
        if activation_datetime is None:
            return False
        if deactivation_datetime is None:
            return True
        return (
            activation_datetime
            <= datetime.datetime.now(tz=datetime.timezone.utc)
            < deactivation_datetime
        )

    @active.expression
    def active(cls):
        """
        Enabled filtering on the hybrid property. While the hybrid_property works on the instance
        level, this expression function works on the class level and uses SQLAlchemy's framework
        for filtering.
        """
        return and_(
            and_(
                cls.activated_at != None,  # noqa: E711
                cls.activated_at <= datetime.datetime.now(tz=datetime.timezone.utc),
            ),
            or_(
                cls.deactivated_at == None,  # noqa: E711
                cls.deactivated_at > datetime.datetime.now(tz=datetime.timezone.utc),
            ),
        )
