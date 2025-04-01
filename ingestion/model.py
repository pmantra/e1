from __future__ import annotations

import dataclasses
import datetime
from typing import Dict, List, Optional, Sequence, TypedDict

from db import model as db_model

__all__ = (
    "FileUploadNotification",
    "UnprocessedNotification",
    "OptumEligibilityRecord",
    "OptumPolicy",
    "OptumEmail",
    "OptumAddress",
    "OptumAttribute",
    "OptumAuthorativeRepresentative",
)


class OptumEligibilityRecord(TypedDict):
    """Optum's representation of a single member which is eligible for Maven.

    See Also:
        optum_record_to_member(...)
    """

    uniqueId: str
    """A unique identifier internal to Optum."""
    firstName: str
    """The first name of the member."""
    lastName: str
    """The last name of the member."""
    dateOfBirth: str
    """The member's date of birth."""
    primaryMemberId: str
    """A unique identifier internal to Optum."""
    dependentTypeId: str | None
    """If present, indicates that the record is for a dependent."""
    dependentTypeDesc: str | None
    """A description of the dependent type, internal to Optum."""
    subscriberId: str | None
    """If present, an identifier for the record."""
    memberId: str | None
    """If present, an identifier for the record."""
    altId: str | None
    """If present, an identifier for the record."""
    employerAssignedId: str | None
    """If present, an identifier for the record"""
    clientId: str
    """An ID for the client purchasing through Optum."""
    clientName: str
    """The name of the client purchasing through Optum."""
    customerId: str | None
    """If present, the ID of the company the client represents."""
    customerName: str | None
    """If present, the name of the company the client represents."""
    genderCode: str | None
    """If present, code indiciating the gender of a member """
    genderDescription: str | None
    """If present, text description corresponding to the gender code a of a member"""
    doNotContact: str | None
    """If present, flag indicating that outreach should not be sent to a member"""
    policies: Sequence[OptumPolicy]
    """The policies which provide this member with access to Maven."""
    emails: Sequence[OptumEmail]
    """The emails associated to this member."""
    postalAddresses: Sequence[OptumAddress]
    """ The addresses associated to this member"""
    attributes: Sequence[OptumAttribute]
    """ Who knows"""
    authoritativeRepresentatives: Sequence[OptumAuthorativeRepresentative]


class OptumPolicy(TypedDict):
    customerAccountId: str
    effectiveDate: str
    terminationDate: str
    planVariationCode: str
    reportingCode: str


class OptumEmail(TypedDict):
    emailId: str


class Address(TypedDict, total=False):
    address_1: str
    city: str
    state: str
    postal_code: str
    address_2: str = None
    postal_code_suffix: str = None
    country_code: str = None


class OptumAddress(TypedDict):
    addressTypeCode: str
    addressTypeDesc: str
    addressLine1: str
    addressLine2: str
    postalCode: str
    postalSuffixCode: str
    state: str
    city: str


class OptumAttribute(TypedDict):
    name: str
    value: str


class OptumAuthorativeRepresentative(TypedDict):
    name: str
    value: str


class EffectiveRange(TypedDict, total=False):
    lower: Optional[datetime.date]
    upper: Optional[datetime.date]
    lower_inc: bool
    upper_inc: bool


@dataclasses.dataclass
class FileUploadNotification:
    name: str


@dataclasses.dataclass
class UnprocessedNotification:
    metadata: Metadata
    record: Dict


@dataclasses.dataclass
class ParsedRecord:
    record: Dict
    errors: List = dataclasses.field(default_factory=list)
    warnings: List = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ProcessedNotification:
    metadata: Metadata
    record: ProcessedMember
    address: db_model.Address | None = None


@dataclasses.dataclass
class ProcessedMember:
    date_of_birth: datetime.date
    unique_corp_id: str
    first_name: str = ""
    last_name: str = ""
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    employer_assigned_id: str | None = ""
    dependent_id: str = ""
    effective_range: EffectiveRange | None = None
    record: dict = dataclasses.field(default_factory=dict)
    custom_attributes: dict = dataclasses.field(default_factory=dict)
    file_id: int | None = None
    organization_id: int | None = None
    do_not_contact: str | None = None
    gender_code: str | None = None
    errors: list[str] = None
    warnings: list[str] = None
    created_at: datetime.date | None = None
    updated_at: datetime.date | None = None


@dataclasses.dataclass
class Metadata:
    type: str
    identifier: str
    index: int
    ingestion_ts: datetime.datetime
    transformation_ts: datetime.datetime | None = None
    file_id: int | None = None
    organization_id: int | None = None
    data_provider: bool | None = None
