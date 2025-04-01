from mmlib.models import base

from sqlalchemy import Column, Integer, Text


class Demo(base.TimeLoggedModelBase):
    __tablename__ = "demo"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False, default="")
