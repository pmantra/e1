from __future__ import annotations

import random
import uuid
from datetime import date, datetime, time, timedelta, timezone
from typing import Optional, Sequence, Union

import asyncpg
import factory
from factory import fuzzy
from mmstream import redis
from split.model import AffiliationsHeader, ParentFileInfo
from tests.factories import unique_faker

from app.eligibility.client_specific import base as ccbase
from app.eligibility.client_specific import microsoft
from app.eligibility.constants import MatchType
from app.eligibility.populations import model as pop_model
from db import model
from db.model import Member2, VerificationTypes
from db.mono import client as mclient


def postiveint(integer: int):
    return integer + 1


class SubFactoryList(factory.SubFactory):
    """Copied from factory.RelatedFactoryList, adapted for SubFactory"""

    def __init__(self, factory, type=list, size=2, **defaults):
        self.size = size
        self.type = type
        super().__init__(factory, **defaults)

    def evaluate(self, instance, step, extra):
        parent = super()
        return self.type(
            parent.evaluate(instance, step, extra)
            for i in range(self.size if isinstance(self.size, int) else self.size())
        )


class FakerList(factory.Faker):
    def __init__(self, provider, type=list, size=2, **kwargs):
        self.size = size
        self.type = type
        super().__init__(provider, **kwargs)

    def evaluate(self, instance, step, extra):
        parent = super()
        return self.type(
            parent.evaluate(instance, step, extra.copy())
            for i in range(self.size if isinstance(self.size, int) else self.size())
        )


class _MemberFactory(factory.Factory):
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    work_state = factory.Faker("state_abbr", locale="en_US")
    work_country = factory.Faker("country_code")
    email = factory.Faker("email")
    unique_corp_id = factory.Faker("swift11")
    custom_attributes = factory.Faker("pydict", value_types=["str"])

    @factory.post_generation
    def effective_range(instance: model.Member, create, extracted, **kwargs):
        if kwargs:
            instance.effective_range = asyncpg.Range(**kwargs)


class DateRangeFactory(factory.Factory):
    class Meta:
        model = model.DateRange

    lower = date(2000, 1, 1)
    upper = None
    lower_inc = True
    upper_inc = False


class ExpiredDateRangeFactory(factory.Factory):
    class Meta:
        model = model.DateRange

    lower = date.today() - timedelta(days=30)
    upper = lower + timedelta(days=1)
    lower_inc = factory.Faker("boolean")
    upper_inc = factory.Faker("boolean")


class PolicyFactory(factory.Factory):
    class Meta:
        model = model.Policy

    policy_id = factory.Faker("swift11")
    effective_range = factory.SubFactory(DateRangeFactory)


class MemberFactory(_MemberFactory):
    class Meta:
        model = model.Member

    file_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    effective_range = factory.SubFactory(DateRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")


class MemberVersionedFactory(_MemberFactory):
    class Meta:
        model = model.MemberVersioned

    file_id = None
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    effective_range = factory.SubFactory(DateRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")
    work_state = factory.Faker("state_abbr", locale="en_US")


class MemberVersionedFactoryWithHash(_MemberFactory):
    class Meta:
        model = model.MemberVersioned

    file_id = None
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    effective_range = factory.SubFactory(DateRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")
    hash_value = factory.fuzzy.FuzzyText(length=16)
    hash_version = factory.Sequence(postiveint)


class AddressFactory(factory.Factory):
    class Meta:
        model = model.Address

    address_1 = factory.Faker("street_address")
    city = factory.Faker("city")
    state = factory.Faker("state")
    postal_code = factory.Faker("postcode")
    address_2 = factory.Faker("street_address")
    postal_code_suffix = factory.Faker("postcode")
    country_code = ""


class FileFactory(factory.Factory):
    class Meta:
        model = model.File

    id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    name = factory.Faker("file_path", depth=2, category="text")
    encoding = "utf8"
    error = None
    success_count = 0
    failure_count = 0
    raw_count = 0


class ConfigurationFactory(factory.Factory):
    class Meta:
        model = model.Configuration

    organization_id = factory.Sequence(postiveint)
    directory_name = factory.Faker("swift8")
    email_domains = factory.LazyFunction(set)
    data_provider = False

    # Get current date at midnight
    activated_at = datetime.combine(date.today(), datetime.min.time()) - timedelta(
        days=365
    )
    terminated_at = datetime.combine(date.today(), datetime.min.time()) + timedelta(
        days=365
    )
    employee_only = False
    medical_plan_only = False
    eligibility_type = random.choice(
        [
            None,
            "STANDARD",
            "ALTERNATE",
            "FILELESS",
            "CLIENT_SPECIFIC",
            "SAML",
            "HEALTHPLAN",
            "UNKNOWN",
        ]
    )


class HeaderAliasFactory(factory.Factory):
    class Meta:
        model = model.HeaderAlias

    organization_id = factory.Sequence(postiveint)
    header = factory.Faker("swift11")
    alias = factory.Faker("domain_word")


class RecordFactory(MemberFactory):
    class Meta:
        model = dict

    id = factory.Sequence(postiveint)
    key = factory.Faker("swift11")


class ExternalRecordFactory(_MemberFactory):
    class Meta:
        model = dict

    external_id = factory.Faker("swift11")
    # record["external_id"] should equal external_id.
    record = factory.LazyAttribute(lambda o: {"external_id": o.external_id})
    external_name = factory.Faker("company")
    dependent_id = factory.Faker("swift11")
    source = factory.Faker("domain_word")
    organization_id = factory.Sequence(postiveint)
    received_ts = factory.Sequence(postiveint)
    effective_range = factory.SubFactory(DateRangeFactory)
    hash_value = None
    hash_version = None


class ExternalRecordFactoryWithHash(_MemberFactory):
    class Meta:
        model = dict

    external_id = factory.Faker("swift11")
    # record["external_id"] should equal external_id.
    record = factory.LazyAttribute(lambda o: {"external_id": o.external_id})
    external_name = factory.Faker("company")
    dependent_id = factory.Faker("swift11")
    source = factory.Faker("domain_word")
    organization_id = factory.Sequence(postiveint)
    received_ts = factory.Sequence(postiveint)
    effective_range = factory.SubFactory(DateRangeFactory)
    hash_value = factory.fuzzy.FuzzyText(length=16)
    hash_version = factory.Sequence(postiveint)


class ExternalRecordAndAddressFactory(factory.Factory):
    class Meta:
        model = model.ExternalRecordAndAddress

    external_record = factory.SubFactory(ExternalRecordFactory)
    record_address = factory.SubFactory(AddressFactory)


class ExternalRecordAndAddressFactoryWithHash(factory.Factory):
    class Meta:
        model = model.ExternalRecordAndAddress

    external_record = factory.SubFactory(ExternalRecordFactoryWithHash)
    record_address = factory.SubFactory(AddressFactory)


class ExternalIDFactory(factory.Factory):
    class Meta:
        model = dict

    external_id = factory.Faker("swift11")
    source = factory.Faker("domain_word")
    organization_id = factory.Sequence(postiveint)
    data_provider_organization_id = None


class FileParseResultFactory(_MemberFactory):
    class Meta:
        model = model.FileParseResult

    file_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    effective_range = factory.SubFactory(DateRangeFactory)
    errors = factory.SubFactory(factory.ListFactory)
    warnings = factory.SubFactory(factory.ListFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")
    hash_value = None
    hash_version = None


class FileParseResultFactoryWithHash(_MemberFactory):
    class Meta:
        model = model.FileParseResult

    file_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    effective_range = factory.SubFactory(DateRangeFactory)
    errors = factory.SubFactory(factory.ListFactory)
    warnings = factory.SubFactory(factory.ListFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")
    hash_value = factory.fuzzy.FuzzyText(length=16)
    hash_version = factory.Sequence(postiveint)


class FileParseErrorFactory(factory.Factory):
    class Meta:
        model = model.FileParseError

    file_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    record = factory.SubFactory(factory.DictFactory)
    errors = factory.SubFactory(factory.ListFactory)
    warnings = factory.SubFactory(factory.ListFactory)


class VerificationFactory(factory.Factory):
    class Meta:
        model = model.Verification

    user_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    dependent_id = ""
    email = ""
    unique_corp_id = factory.Faker("swift11")
    verification_type = random.choice(list(VerificationTypes)).value
    verified_at = factory.Faker("date_time")
    additional_fields = factory.SubFactory(factory.DictFactory)
    verification_session = factory.Faker("uuid4")
    verification_2_id: Optional[int] = None


class DeactivatedVerificationFactory(VerificationFactory):
    class Meta:
        model = model.Verification

    id = factory.Sequence(postiveint)
    deactivated_at = factory.Faker("date_time")


class MemberVerificationFactory(factory.Factory):
    class Meta:
        model = model.MemberVerification

    member_id = factory.Sequence(postiveint)
    verification_id = factory.Sequence(postiveint)
    verification_attempt_id = None


class VerificationAttemptFactory(factory.Factory):
    class Meta:
        model = model.VerificationAttempt

    user_id = factory.Sequence(postiveint)
    verification_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    work_state = factory.Faker("state_abbr", locale="en_US")
    dependent_id = ""
    email = ""
    unique_corp_id = factory.Faker("swift11")
    verification_type = random.choice(list(VerificationTypes)).value
    # TODO: Implement this as a mapping when we start looking at multiple effective ranges for a user
    policy_used = ""
    # Using dob here, as we don't want timezones/detailed info attached here- causes issues with verification
    verified_at = factory.Faker("date_of_birth")
    additional_fields = factory.SubFactory(factory.DictFactory)


class VerificationDataFactory(factory.Factory):
    class Meta:
        model = model.VerificationData

    verification_id = factory.Sequence(postiveint)
    verification_attempt_id = factory.Sequence(postiveint)
    eligibility_member_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    unique_corp_id = factory.Faker("swift11")
    dependent_id = factory.Faker("swift11")
    email = factory.Faker("email")
    work_state = factory.Faker("state_abbr", locale="en_US")
    additional_fields = factory.SubFactory(factory.DictFactory)
    member_1_id = None
    member_2_id = None
    member_2_version = None


class _RawStreamEntryFactory(factory.Factory):
    class Meta:
        model = dict

    key = factory.Faker("word")
    id = factory.Sequence(str)
    headers = factory.SubFactory(factory.DictFactory)
    message = factory.SubFactory(factory.DictFactory)


class _PendingMessageFactory(factory.Factory):
    class Meta:
        model = dict

    msgid = factory.Sequence(str)
    consumer = factory.Faker("word")
    pendingms = factory.Faker("random_int")
    deliveries = factory.Faker("random_int")


def pending_message(*, size: int = 1, **kwargs):
    if size == 1:
        return tuple(_PendingMessageFactory.create(**kwargs).values())
    return [
        tuple(e.values()) for e in _PendingMessageFactory.create_batch(size, **kwargs)
    ]


def raw_stream_entry(
    *, size: int = 1, **kwargs
) -> Union[redis.RawStreamEntryT, Sequence[redis.RawStreamEntryT]]:
    if size == 1:
        return tuple(_RawStreamEntryFactory.create(**kwargs).values())
    return [
        tuple(e.values()) for e in _RawStreamEntryFactory.create_batch(size, **kwargs)
    ]


class RedisStreamEntryFactory(_RawStreamEntryFactory):
    class Meta:
        model = redis.RedisStreamEntry


class MavenOrgExternalIDFactory(factory.Factory):
    class Meta:
        model = mclient.MavenOrgExternalID

    source: str = factory.Faker("domain_word")
    external_id: str = factory.Faker("swift11")
    organization_id: int = factory.Sequence(postiveint)
    data_provider_organization_id: int = factory.Sequence(postiveint)


class MavenOrganizationFactory(factory.Factory):
    class Meta:
        model = mclient.MavenOrganization
        exclude = ("with_activated", "with_terminated")

    id: int = factory.Sequence(postiveint)
    name: str = factory.Faker("company")
    directory_name: str = factory.Faker("domain_word")
    json: dict = factory.SubFactory(factory.DictFactory)
    email_domains: set[str] = FakerList("dga", type=set)
    data_provider: int = fuzzy.FuzzyChoice([0, 1])
    with_activated = factory.Faker("pybool")
    with_terminated = factory.Faker("pybool")
    activated_at: datetime | None = factory.Maybe(
        "with_activated",
        datetime.combine(datetime.strptime("04-01-2020", "%m-%d-%Y"), time()),
        None,
    )
    terminated_at: datetime | None = factory.Maybe(
        "with_activated" and "with_terminated",
        datetime.combine(datetime.strptime("04-01-2023", "%m-%d-%Y"), time()),
        None,
    )


class MavenClientTrackFactory(factory.Factory):
    class Meta:
        model = mclient.BasicClientTrack

    id: int = factory.Sequence(postiveint)
    track: str = unique_faker.UniqueFaker("domain_word")
    organization_id: int = factory.Sequence(postiveint)
    active: bool = True
    launch_date: Optional[datetime] = datetime.now(tz=timezone.utc) - timedelta(days=1)
    length_in_days: int = 365
    ended_at: Optional[datetime] = None


class MavenReimbursementOrganizationSettingsFactory(factory.Factory):
    class Meta:
        model = mclient.BasicReimbursementOrganizationSettings

    id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    name: str = unique_faker.UniqueFaker("domain_word")
    benefit_faq_resource_id: int = 0
    survey_url: str = ""
    started_at: Optional[datetime] = datetime.now(tz=timezone.utc) - timedelta(days=1)
    ended_at: Optional[datetime] = None
    debit_card_enabled: bool = False
    cycles_enabled: bool = False
    direct_payment_enabled: bool = False
    rx_direct_payment_enabled: bool = False
    deductible_accumulation_enabled: bool = False
    closed_network: bool = True
    fertility_program_type: mclient.FertilityProgramType = (
        mclient.FertilityProgramType.CARVE_OUT
    )
    fertility_requires_diagnosis: bool = True
    fertility_allows_taxable: bool = False


class WalletEnablementFactory(factory.Factory):
    class Meta:
        model = model.WalletEnablement

    member_id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    enabled: bool = factory.Faker("boolean")
    insurance_plan: Optional[str] = factory.Faker("domain_word")
    start_date: Optional[date] = factory.Faker("past_date")
    eligibility_date: Optional[date] = factory.Faker("future_date")
    created_at: Optional[datetime] = factory.Faker("date_time")
    updated_at: Optional[datetime] = factory.Faker("date_time")
    effective_range: Optional[model.DateRange] = factory.SubFactory(DateRangeFactory)


class WalletEnablementResponseFactory(factory.Factory):
    class Meta:
        model = model.WalletEnablementResponse

    member_id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    enabled: bool = factory.Faker("boolean")
    insurance_plan: Optional[str] = factory.Faker("domain_word")
    start_date: Optional[date] = factory.Faker("past_date")
    eligibility_date: Optional[date] = factory.Faker("future_date")
    created_at: Optional[datetime] = factory.Faker("date_time")
    updated_at: Optional[datetime] = factory.Faker("date_time")
    effective_range: Optional[model.DateRange] = factory.SubFactory(DateRangeFactory)
    is_v2: bool = factory.Faker("boolean")
    member_1_id: Optional[int] = None
    member_2_id: Optional[int] = None


class EligibilityVerificationForUserFactory(factory.Factory):
    class Meta:
        model = model.EligibilityVerificationForUser

    verification_id: int = factory.Sequence(postiveint)
    user_id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    eligibility_member_id: int = factory.Sequence(postiveint)
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    work_state: str = factory.Faker("state_abbr", locale="en_US")
    email: str = factory.Faker("email")
    verification_type: str = random.choice(list(VerificationTypes)).value
    employer_assigned_id = factory.Faker("swift11")
    effective_range = factory.SubFactory(DateRangeFactory)
    verification_created_at: date = factory.Faker("date_time")
    verification_updated_at: date = factory.Faker("date_time")
    verification_deactivated_at: date = factory.Faker("date_time")
    gender_code: str = random.choice(["F", "M", "O", "U"])
    do_not_contact = random.choice(["", "True", "False"])
    verified_at: date = factory.Faker("date_time")
    additional_fields: factory.LazyAttribute(lambda o: {"external_id": o.external_id})
    verification_session: uuid = factory.Faker("uuid4")
    eligibility_member_version: int | None = None
    is_v2: bool = factory.Faker("boolean")
    verification_1_id: Optional[int] = None
    verification_2_id: Optional[int] = None
    eligibility_member_2_id: Optional[int] = None
    eligibility_member_2_version: Optional[int] = None


class EligibilityVerificationExpiredE9yForUserFactory(factory.Factory):
    class Meta:
        model = model.EligibilityVerificationForUser

    verification_id: int = factory.Sequence(postiveint)
    user_id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    eligibility_member_id: int = factory.Sequence(postiveint)
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    work_state: str = factory.Faker("state_abbr", locale="en_US")
    email: str = factory.Faker("email")
    verification_type: str = random.choice(list(VerificationTypes)).value
    employer_assigned_id = factory.Faker("swift11")
    effective_range = factory.SubFactory(ExpiredDateRangeFactory)
    verification_created_at: date = factory.Faker("date_time")
    verification_updated_at: date = factory.Faker("date_time")
    verification_deactivated_at: date = factory.Faker("date_time")
    gender_code: str = random.choice(["F", "M", "O", "U"])
    do_not_contact = random.choice(["", "True", "False"])
    verified_at: date = factory.Faker("date_time")
    additional_fields: factory.LazyAttribute(lambda o: {"external_id": o.external_id})


class ClientSpecificEmployeeRequestFactory(factory.Factory):
    class Meta:
        model = ccbase.ClientSpecificRequest

    is_employee = True
    unique_corp_id = factory.Faker("swift11")
    date_of_birth = factory.Faker("date_of_birth")
    dependent_date_of_birth = None


class ClientSpecificDependentRequestFactory(factory.Factory):
    class Meta:
        model = ccbase.ClientSpecificRequest

    is_employee = False
    unique_corp_id = factory.Faker("swift11")
    date_of_birth = factory.Faker("date_of_birth")
    dependent_date_of_birth = factory.Faker("date_of_birth")


class MicrosoftResponseFactory(factory.Factory):
    class Meta:
        model = microsoft.MicrosoftResponse

    insuranceType = factory.Faker("word")
    country = factory.Faker("country_code")
    state = factory.Faker("state_abbr")


class GetEligibleFeaturesForUserResponse(factory.Factory):
    class Meta:
        model = model.GetEligibleFeaturesForUser

    features = factory.Faker(
        "pylist", nb_elements=10, variable_nb_elements=True, value_types="int"
    )


class PreEligibilityOrganizationFactory(factory.Factory):
    class Meta:
        model = model.PreEligibilityOrganization

    organization_id = factory.Sequence(int)
    eligibility_end_date = factory.Faker("date_time")


class PreEligibilityResponseFactory(factory.Factory):
    class Meta:
        model = model.PreEligibilityResponse

    match_type = MatchType.UNKNOWN_ELIGIBILITY
    pre_eligibility_organizations = list()


class FailedVerificationAttemptFactory(factory.Factory):
    class Meta:
        model = model.VerificationAttempt

    organization_id: int = factory.Sequence(postiveint)
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    work_state: str = factory.Faker("state_abbr", locale="en_US")
    email: str = factory.Faker("email")
    verification_type: str = random.choice(list(VerificationTypes)).value
    created_at: date = factory.Faker("date_time")
    updated_at: date = factory.Faker("date_time")
    successful_verification = False
    policy_used = ""
    verified_at: date = factory.Faker("date_time")
    additional_fields: factory.LazyAttribute(lambda o: {"external_id": o.external_id})


class FailedVerificationAttemptResponseFactory(factory.Factory):
    class Meta:
        model = model.VerificationAttemptResponse

    organization_id: int = factory.Sequence(postiveint)
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    unique_corp_id: str = factory.Faker("swift11")
    dependent_id: str = factory.Faker("swift11")
    work_state: str = factory.Faker("state_abbr", locale="en_US")
    email: str = factory.Faker("email")
    verification_type: str = random.choice(list(VerificationTypes)).value
    created_at: date = factory.Faker("date_time")
    updated_at: date = factory.Faker("date_time")
    successful_verification = False
    policy_used = ""
    verified_at: date = factory.Faker("date_time")
    additional_fields: factory.LazyAttribute(lambda o: {"external_id": o.external_id})
    is_v2: bool = factory.Faker("boolean")
    verification_attempt_1_id: Optional[int] = None
    verification_attempt_2_id: Optional[int] = None
    eligibility_member_id: Optional[int] = None
    eligibility_member_2_id: Optional[int] = None


class PopulationFactory(factory.Factory):
    class Meta:
        model = pop_model.Population

    id: int = factory.Sequence(postiveint)
    organization_id: int = factory.Sequence(postiveint)
    activated_at: datetime | None = None
    deactivated_at: datetime | None = None
    # only a single level lookup
    sub_pop_lookup_keys_csv: str = factory.Faker("swift11")
    sub_pop_lookup_map_json: dict = factory.Faker("pydict", value_types=["str"])
    advanced: bool = False
    created_at: datetime = factory.Faker("date_time")
    updated_at: datetime = factory.Faker("date_time")


class SubPopulationFactory(factory.Factory):
    class Meta:
        model = pop_model.SubPopulation

    id: int = factory.Sequence(postiveint)
    population_id: int = factory.Sequence(postiveint)
    feature_set_name: str = factory.Faker("swift11")
    feature_set_details_json: dict = {
        f"{feature_type}": "1,2,3,4,5" for feature_type in pop_model.FeatureTypes
    }
    created_at: datetime = factory.Faker("date_time")
    updated_at: datetime = factory.Faker("date_time")


class MemberSubPopulationFactory(factory.Factory):
    class Meta:
        model = pop_model.MemberSubPopulation

    member_id: int = factory.Sequence(postiveint)
    sub_population_id: int = factory.Sequence(postiveint)


class ParentFileInfoFactory(factory.Factory):
    class Meta:
        model = ParentFileInfo

    file = FileFactory.create()
    affiliations_header = AffiliationsHeader(
        client_id_source="client_id", customer_id_source="customer_id"
    )


class Member2Factory(_MemberFactory):
    class Meta:
        model = Member2

    version = 1000
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    effective_range = factory.SubFactory(DateRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")


class MemberResponseFactory(_MemberFactory):
    class Meta:
        model = model.MemberResponse

    version = 1000
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    effective_range = factory.SubFactory(DateRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["F", "M", "O", "U"])
    employer_assigned_id = factory.Faker("swift11")
    is_v2: bool = factory.Faker("boolean")
    member_1_id: Optional[int] = None
    member_2_id: Optional[int] = None


class Verification2Factory(factory.Factory):
    class Meta:
        model = model.Verification2

    user_id = factory.Sequence(postiveint)
    organization_id = factory.Sequence(postiveint)
    created_at = factory.Faker("date_time")
    updated_at = factory.Faker("date_time")
    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    dependent_id = ""
    email = ""
    unique_corp_id = factory.Faker("swift11")
    verification_type = random.choice(list(VerificationTypes)).value
    verified_at = factory.Faker("date_time", tzinfo=timezone.utc)
    additional_fields = factory.SubFactory(factory.DictFactory)
    verification_session = factory.Faker("uuid4")
    member_version = 1000
