from __future__ import annotations

import dataclasses
import enum
import uuid
from datetime import date, datetime
from typing import Mapping, NamedTuple, Optional, Set, TypedDict, Union

import asyncpg
import typic

from app.eligibility.constants import MatchType

__all__ = (
    "Configuration",
    "HeaderAlias",
    "HeaderMapping",
    "File",
    "FileError",
    "Member",
    "OrgIdentity",
    "FileParseResult",
    "FileParseError",
    "ExternalMavenOrgInfo",
    "DateRange",
    "EligibilityVerificationForUser",
    "MemberResponse",
    "WalletEnablementResponse",
)


class ClientSpecificImplementation(str, enum.Enum):
    MICROSOFT = "MICROSOFT"


@typic.slotted(dict=False)
@dataclasses.dataclass
class Configuration:
    organization_id: int
    directory_name: str
    eligibility_type: str | None = None
    email_domains: Set[str] | None = None
    implementation: ClientSpecificImplementation | None = None
    data_provider: bool | None = False
    created_at: datetime | None = None
    updated_at: datetime | None = None
    activated_at: datetime | None = None
    terminated_at: datetime | None = None
    employee_only: bool | None = False
    medical_plan_only: bool | None = False


class ClientSpecificMode(str, enum.Enum):
    ONLY_CENSUS = "ONLY_CENSUS"
    FALLBACK_TO_CENSUS = "FALLBACK_TO_CENSUS"
    ONLY_CLIENT_CHECK = "ONLY_CLIENT_CHECK"


class VerificationTypes(str, enum.Enum):
    PRIMARY = "PRIMARY"
    ALTERNATE = "ALTERNATE"
    CLIENT_SPECIFIC = "CLIENT_SPECIFIC"
    FILELESS = "FILELESS"
    MANUAL = "MANUAL"
    PRE_VERIFY = "PRE_VERIFY"
    MULTISTEP = "MULTISTEP"
    SSO = "SSO"
    STANDARD = "STANDARD"
    LOOKUP = "LOOKUP"


class HeaderMapping(dict):

    _DEFAULT_HEADERS = {
        "date_of_birth": "date_of_birth",
        "email": "email",
        "unique_corp_id": "employee_id",
        "employer_assigned_id": "employer_assigned_id",
        "dependent_id": "dependent_id",
        "gender": "gender",
        "beneficiaries_enabled": "beneficiaries_enabled",
        "wallet_enabled": "wallet_enabled",
        "office_id": "office_id",
        "work_state": "work_state",
        "work_country": "work_country",
        "first_name": "employee_first_name",
        "last_name": "employee_last_name",
        "dependent_relationship_code": "dependent_relationship_code",
        "lob": "lob",
        "salary_tier": "salary_tier",
        "plan_carrier": "plan_carrier",
        "cobra_coverage": "cobra_coverage",
        "company_couple": "company_couple",
        "address_1": "address_1",
        "address_2": "address_2",
        "city": "city",
        "state": "state",
        "zip_code": "zip_code",
        "country": "country",
    }

    _OPTIONAL_HEADERS = {"client_id": "client_id"}
    _HEALTH_PLAN_HEADERS = {
        "maternity_indicator_date": "maternity_indicator_date",
        "maternity_indicator": "maternity_indicator",
        "delivery_indicator_date": "delivery_indicator_date",
        "delivery_indicator": "delivery_indicator",
        "fertility_indicator_date": "fertility_indicator_date",
        "fertility_indicator": "fertility_indicator",
        "p_and_p_indicator": "p_and_p_indicator",
        "client_name": "client_name",
    }

    def __getitem__(self, item):
        if item in self:
            return self[item]
        if item in self._DEFAULT_HEADERS:
            return self._DEFAULT_HEADERS[item]
        raise KeyError(repr(item))

    def with_defaults(self) -> dict:
        return {**self._DEFAULT_HEADERS, **self}

    def optional_headers(self) -> dict:
        return {**self._OPTIONAL_HEADERS, **self}

    def health_plan_headers(self) -> dict:
        return {**self._HEALTH_PLAN_HEADERS, **self}

    def with_all_headers(self) -> dict:
        return {
            **self._DEFAULT_HEADERS,
            **self._OPTIONAL_HEADERS,
            **self._HEALTH_PLAN_HEADERS,
            **self,
        }


@typic.slotted(dict=False)
@dataclasses.dataclass
class HeaderAlias:
    organization_id: int
    header: str
    alias: str
    id: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class FileError(str, enum.Enum):
    MISSING = "missing"
    DELIMITER = "delimiter"
    UNKNOWN = "unknown"


@typic.slotted(dict=False)
@dataclasses.dataclass
class File:
    organization_id: int
    name: str
    encoding: str = "utf8"
    error: FileError | None = None
    id: int | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    success_count: int | None = None
    failure_count: int | None = None
    raw_count: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class Member:
    organization_id: int
    first_name: str
    last_name: str
    date_of_birth: date
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    unique_corp_id: str = ""
    employer_assigned_id: str | None = ""
    dependent_id: str = ""
    effective_range: DateRange | None = None
    record: dict = dataclasses.field(default_factory=dict)
    custom_attributes: dict | None = dataclasses.field(default_factory=dict)
    id: int | None = None
    file_id: int | None = None
    do_not_contact: str | None = None
    gender_code: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def identity(self) -> OrgIdentity:
        return OrgIdentity(
            self.organization_id,
            self.unique_corp_id,
            self.dependent_id,
        )


@dataclasses.dataclass
class MemberVersioned:
    organization_id: int
    first_name: str
    last_name: str
    date_of_birth: date
    pre_verified: bool = False
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    unique_corp_id: str = ""
    employer_assigned_id: str | None = ""
    dependent_id: str = ""
    effective_range: DateRange | None = None
    record: dict = dataclasses.field(default_factory=dict)
    custom_attributes: dict | None = dataclasses.field(default_factory=dict)
    id: int | None = None
    file_id: int | None = None
    do_not_contact: str | None = None
    gender_code: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    hash_value: str | None = None
    hash_version: int | None = None

    def identity(self) -> OrgIdentity:
        return OrgIdentity(
            self.organization_id,
            self.unique_corp_id,
            self.dependent_id,
        )


@dataclasses.dataclass
class MemberAddress:
    address_1: str
    city: str
    state: str
    postal_code: str
    address_2: str = None
    postal_code_suffix: str = None
    address_type: str = None
    country_code: str = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    id: int | None = None
    member_id: int | None = None


@dataclasses.dataclass
class MemberAddressVersioned:
    address_1: str
    city: str
    state: str
    postal_code: str
    address_2: str = None
    postal_code_suffix: str = None
    address_type: str = None
    country_code: str = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    id: int | None = None
    member_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class FileParseResult:
    file_id: int
    organization_id: int
    date_of_birth: date
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    unique_corp_id: str = ""
    dependent_id: str = ""
    employer_assigned_id: str = ""
    do_not_contact: str = ""
    gender_code: str = ""
    work_state: str | None = None
    work_country: str | None = None
    record: dict | None = None
    custom_attributes: dict | None = None
    errors: list[str] = None
    warnings: list[str] = None
    effective_range: DateRange | None = None
    hash_value: str | None = ""
    hash_version: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class FileParseError:
    file_id: int
    organization_id: int
    id: int | None = None
    record: dict | None = None
    errors: list[str] = None
    warnings: list[str] = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class IncompleteFilesByOrg:
    id: int
    total_members: int
    config: dict
    incomplete: list[dict]


@typic.slotted(dict=False)
@dataclasses.dataclass
class WalletEnablement:
    member_id: int
    organization_id: int
    unique_corp_id: str
    dependent_id: str
    enabled: bool
    insurance_plan: str | None = None
    start_date: date | None = None
    eligibility_date: date | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    effective_range: DateRange | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class EligibilityVerificationForUser:
    verification_id: int
    user_id: int
    organization_id: int
    eligibility_member_id: int
    first_name: str
    last_name: str
    date_of_birth: date | None = None
    unique_corp_id: str = ""
    dependent_id: str = ""
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    record: dict | None = None
    custom_attributes: dict | None = None
    verification_type: str | None = None
    employer_assigned_id: str | None = None
    effective_range: DateRange | None = None
    verification_created_at: date | None = None
    verification_updated_at: date | None = None
    verification_deactivated_at: date | None = None
    verified_at: date | None = None
    additional_fields: dict | None = None
    gender_code: str | None = None
    do_not_contact: str | None = None
    verification_session: uuid | None = None
    eligibility_member_version: int | None = None
    is_v2: bool | None = False
    verification_1_id: int | None = None
    verification_2_id: int | None = None
    eligibility_member_2_id: int | None = None
    eligibility_member_2_version: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class ExternalMavenOrgInfo:
    organization_id: int
    directory_name: str | None = None
    activated_at: datetime | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class PreEligibilityResponse:
    match_type: MatchType
    pre_eligibility_organizations: [PreEligibilityOrganization]


@typic.slotted(dict=False)
@dataclasses.dataclass
class GetEligibleFeaturesForUser:
    features: list[int]


@typic.slotted(dict=False)
@dataclasses.dataclass
class DeactivateVerificationRecordForUser:
    verification_id: int
    deactivated_at: datetime


@typic.slotted(dict=False)
@dataclasses.dataclass
class PreEligibilityOrganization:
    organization_id: int
    eligibility_end_date: date | None = None


@dataclasses.dataclass
class Verification:
    user_id: int
    organization_id: int
    verification_type: VerificationTypes
    date_of_birth: date | None = None
    first_name: str | None = None
    last_name: str | None = None
    id: int | None = None
    email: str | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None
    work_state: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deactivated_at: datetime | None = None
    verified_at: datetime | None = None
    additional_fields: dict | None = None
    verification_session: uuid.UUID | None = None
    verification_2_id: int | None = None


@dataclasses.dataclass
class VerificationAttempt:
    verification_type: VerificationTypes
    date_of_birth: date | None = None
    verification_id: int | None = None
    organization_id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    id: int | None = None
    user_id: int | None = None
    email: str | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None
    work_state: str | None = None
    # TODO: Implement this as a mapping when we start looking at multiple effective ranges for a user
    policy_used: dict | None = None
    successful_verification: bool = False
    verified_at: datetime | None = None
    additional_fields: dict | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclasses.dataclass
class VerificationAttemptResponse:
    verification_type: VerificationTypes
    date_of_birth: date | None = None
    verification_id: int | None = None
    organization_id: int | None = None
    first_name: str | None = None
    last_name: str | None = None
    id: int | None = None
    user_id: int | None = None
    email: str | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None
    work_state: str | None = None
    # TODO: Implement this as a mapping when we start looking at multiple effective ranges for a user
    policy_used: dict | None = None
    successful_verification: bool = False
    verified_at: datetime | None = None
    additional_fields: dict | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_v2: bool | None = False
    verification_attempt_1_id: int | None = None
    verification_attempt_2_id: int | None = None
    eligibility_member_id: int | None = None
    eligibility_member_2_id: int | None = None


@dataclasses.dataclass
class MemberVerification:
    id: int | None = None
    member_id: int | None = None
    verification_id: int | None = None
    verification_attempt_id: int | None = None


class DateRange(asyncpg.Range):
    lower: date | None
    upper: date | None
    lower_inc: bool
    upper_inc: bool


class Policy:
    effective_range: DateRange | None
    policyID: str = None


class Address(TypedDict, total=False):
    address_1: str
    city: str
    state: str
    postal_code: str
    id: int | None = None
    member_id: int | None = None
    address_2: str = None
    postal_code_suffix: str = None
    address_type: str = None
    country_code: str = None


class OrgIdentity(NamedTuple):
    organization_id: int
    unique_corp_id: str = ""
    dependent_id: str = ""


class ExternalRecord(TypedDict):
    first_name: str
    last_name: str
    organization_id: int
    unique_corp_id: str
    date_of_birth: date
    work_state: str | None
    work_country: str | None
    email: str
    dependent_id: str
    record: Mapping
    custom_attributes: Mapping
    effective_range: DateRange | None
    do_not_contact: str
    gender_code: str
    employer_assigned_id: str | None
    source: str
    external_id: str
    external_name: str
    received_ts: int


class MemberTestRecord(TypedDict):
    organization_id: int
    first_name: str
    last_name: str
    date_of_birth: date
    work_state: str
    email: str
    unique_corp_id: str
    dependent_id: int
    effective_range: DateRange
    work_country: str


class ExternalRecordAndAddress(TypedDict):
    external_record: ExternalRecord
    record_address: Address | None = None


@dataclasses.dataclass
class VerificationData:
    verification_id: Optional[int]
    verification_attempt_id: Optional[int]
    eligibility_member_id: int
    organization_id: int
    unique_corp_id: str
    dependent_id: str
    email: str
    work_state: str
    additional_fields: dict
    member_1_id: Optional[int]
    member_2_id: Optional[int]
    member_2_version: Optional[int]


def _patch_asyncpg_range():
    # N.B.: Some hacking ahead.
    # asyncpg's builtin Range object has some limitations which make ser/des finnicky.
    #   It takes the parameter `empty`, but exposes that value as the property
    #   `isempty`, hides all its attributes behind properties, and has no annotations.
    #
    # I'd rather just update the db client to return our own type for pg Ranges, but that
    #   would require writing a custom encoder/decoder for postgres :(
    #   See: https://magicstack.github.io/asyncpg/current/usage.html#custom-type-conversions
    #
    # Instead, I'm just flagging to typical that we should use `isempty` as the value for
    #   `empty` when we serialize a range to JSON, etc.

    asyncpg.Range.__serde_flags__ = typic.flags(
        # Don't try to get the attribute `empty`.
        exclude=("empty",),
        # Alias `isempty` to `empty` when serializing.
        fields={"isempty": "empty"},
    )
    # We're also adding in annotations so we preserve these fields when serializing.
    asyncpg.Range.__annotations__ = {
        "lower": Union[int, date, datetime, None],
        "upper": Union[int, date, datetime, None],
        "lower_inc": bool,
        "upper_inc": bool,
        "isempty": bool,
    }


_patch_asyncpg_range()


@dataclasses.dataclass
class Member2:
    id: int
    version: int
    organization_id: int
    first_name: str
    last_name: str
    date_of_birth: date
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    unique_corp_id: str = ""
    employer_assigned_id: str | None = ""
    dependent_id: str = ""
    effective_range: DateRange | None = None
    record: dict = dataclasses.field(default_factory=dict)
    custom_attributes: dict | None = dataclasses.field(default_factory=dict)
    do_not_contact: str | None = None
    gender_code: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def identity(self) -> OrgIdentity:
        return OrgIdentity(
            self.organization_id,
            self.unique_corp_id,
            self.dependent_id,
        )


@dataclasses.dataclass
class Verification2:
    user_id: int
    organization_id: int
    verification_type: VerificationTypes
    date_of_birth: date | None = None
    first_name: str | None = None
    last_name: str | None = None
    id: int | None = None
    email: str | None = None
    unique_corp_id: str | None = None
    dependent_id: str | None = None
    work_state: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    deactivated_at: datetime | None = None
    verified_at: datetime | None = None
    additional_fields: dict | None = None
    verification_session: uuid.UUID | None = None
    member_id: int | None = None
    member_version: int | None = None


@dataclasses.dataclass
class VerificationKey:
    organization_id: int
    member_id: int | None = None
    created_at: datetime | None = None
    is_v2: bool | None = False
    verification_1_id: int | None = None
    verification_2_id: int | None = None
    member_2_id: int | None = None
    member_2_version: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class MemberResponse:
    id: int
    version: int
    organization_id: int
    first_name: str
    last_name: str
    date_of_birth: date
    file_id: int | None = None
    work_state: str | None = None
    work_country: str | None = None
    email: str = ""
    unique_corp_id: str = ""
    employer_assigned_id: str | None = ""
    dependent_id: str = ""
    effective_range: DateRange | None = None
    record: dict = dataclasses.field(default_factory=dict)
    custom_attributes: dict | None = dataclasses.field(default_factory=dict)
    do_not_contact: str | None = None
    gender_code: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    is_v2: bool | None = False
    member_1_id: int | None = None
    member_2_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class WalletEnablementResponse:
    member_id: int
    organization_id: int
    unique_corp_id: str
    dependent_id: str
    enabled: bool
    insurance_plan: str | None = None
    start_date: date | None = None
    eligibility_date: date | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    effective_range: DateRange | None = None
    is_v2: bool | None = False
    member_1_id: int | None = None
    member_2_id: int | None = None


@typic.slotted(dict=False)
@dataclasses.dataclass
class BackfillMemberTrackEligibilityData:
    user_id: int
    verification_id: int
    verification_organization_id: int
    verification_created_at: datetime
    member_id: int | None = None
    member_organization_id: int | None = None
    member_created_at: datetime | None = None
