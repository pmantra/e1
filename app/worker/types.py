import dataclasses
import datetime
from typing import Mapping, Optional, TypedDict

import typic

from db import model


@typic.slotted(dict=False)
@dataclasses.dataclass
class PendingFileNotification:
    file_id: int


@typic.slotted(dict=False)
@dataclasses.dataclass
class PersistFileMessage:
    id: int
    action: str


@typic.slotted(dict=False)
@dataclasses.dataclass
class PersistRecordMessage:
    key: str


@typic.slotted(dict=False)
@dataclasses.dataclass
class FileUploadNotification:
    name: str


@typic.slotted(dict=False)
@dataclasses.dataclass
class ExternalMemberAddress:
    address_1: str
    city: str
    state: str
    postal_code: str
    address_2: str = None
    postal_code_suffix: str = None
    country_code: str = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class ExternalMemberRecord:
    first_name: str
    last_name: str
    unique_corp_id: str
    date_of_birth: datetime.date
    client_id: str
    customer_id: str
    work_state: Optional[str] = None
    email: str = ""
    dependent_id: str = ""
    record: Mapping = dataclasses.field(default_factory=dict)
    effective_range: Optional[model.DateRange] = None
    gender_code: str = ""
    do_not_contact: str = ""
    address: Optional[ExternalMemberAddress] = None
    employer_assigned_id: str = ""
    custom_attributes: Mapping = dataclasses.field(default_factory=dict)


class ExternalMessageAttributes(TypedDict):
    source: str
    external_id: str
    external_name: str
    received_ts: int
