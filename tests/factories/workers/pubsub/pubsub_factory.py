import random

import asyncpg
import factory
from ingestion import model, repository
from mmstream.pubsub import PubSubEntry
from tests.factories import data_models

from app.worker import types


class EffectiveRangeFactory(factory.Factory):
    class Meta:
        model = asyncpg.Range

    lower = None
    upper = None
    lower_inc = True
    upper_inc = True


class ExternalMessageAttributesFactory(factory.Factory):
    class Meta:
        model = types.ExternalMessageAttributes

    source = factory.Faker("domain_word")
    external_id = factory.Faker("swift11")
    external_name = factory.Faker("company")
    received_ts = factory.Sequence(int)


class ExternalMemberAddressFactory(factory.Factory):
    class Meta:
        model = types.ExternalMemberAddress

    address_1 = factory.Faker("street_address")
    city = factory.Faker("city")
    state = factory.Faker("state")
    postal_code = factory.Faker("postcode")
    address_2 = factory.Faker("street_address")
    postal_code_suffix = factory.Faker("postcode")
    country_code = factory.Faker("country_code")


class ExternalRecordFactory(factory.Factory):
    class Meta:
        model = types.ExternalMemberRecord

    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    work_state = factory.Faker("state_abbr", locale="en_US")
    email = factory.Faker("email")
    unique_corp_id = factory.Faker("swift11")
    effective_range = factory.SubFactory(EffectiveRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["", "F", "M", "O", "U"])
    address = factory.SubFactory(ExternalMemberAddressFactory)
    employer_assigned_id = factory.Faker("swift11")
    client_id = factory.Faker("swift11")
    customer_id = factory.Faker("swift11")


class ExternalRecordFactoryNoAddress(factory.Factory):
    class Meta:
        model = types.ExternalMemberRecord

    first_name = factory.Faker("first_name")
    last_name = factory.Faker("last_name")
    date_of_birth = factory.Faker("date_of_birth")
    work_state = factory.Faker("state_abbr", locale="en_US")
    email = factory.Faker("email")
    unique_corp_id = factory.Faker("swift11")
    effective_range = factory.SubFactory(EffectiveRangeFactory)
    do_not_contact = random.choice(["", "True", "False"])
    gender_code = random.choice(["", "F", "M", "O", "U"])
    address = None
    employer_assigned_id = factory.Faker("swift11")
    client_id = factory.Faker("swift11")
    customer_id = factory.Faker("swift11")


class PubSubMessageFactory(factory.Factory):
    class Meta:
        model = PubSubEntry

    ordering_key = factory.Faker("pyint")
    ack_id = factory.Faker("swift11")
    attributes = factory.SubFactory(factory.DictFactory)
    data = factory.SubFactory(factory.DictFactory)


class FileUploadNotificationFactory(factory.Factory):
    class Meta:
        model = types.FileUploadNotification

    name = factory.Faker("file_path", depth=2, category="text")


class MetadataFactory(factory.Factory):
    class Meta:
        model = model.Metadata

    type = repository.IngestionType.FILE
    identifier = factory.Faker("file_path", depth=2, category="text")
    index = factory.Sequence(int)
    ingestion_ts = factory.Faker("date_time")


class ProcessedMemberFactory(factory.Factory):
    class Meta:
        model = model.ProcessedMember

    date_of_birth = factory.Faker("date_of_birth")
    unique_corp_id = factory.Faker("swift11")
    effective_range = {
        "lower": None,
        "upper": None,
        "lower_inc": True,
        "upper_inc": True,
    }
    record = {"external_id": "ext"}


class OptumAddressFactory(factory.Factory):
    class Meta:
        model = model.OptumAddress

    addressTypeCode = "180"
    addressTypeDesc = random.choice(
        [
            "Home Address",
            "Work Address",
            "Confidential Mailing Address",
            "",
            "Other",
            "Consumer Mailing Address",
        ]
    )
    addressLine1 = factory.Faker("street_address")
    addressLine2 = factory.Faker("street_address")
    postalCode = factory.Faker("postcode")
    postalSuffixCode = factory.Faker("postcode")
    state = factory.Faker("state")
    city = factory.Faker("city")
    isoCountryCode = factory.Faker("country_code")
    isoCountryDesc = factory.Faker("country")


class UnprocessedNotificationFactory(factory.Factory):
    class Meta:
        model = model.UnprocessedNotification

    metadata = factory.SubFactory(MetadataFactory)
    record = factory.SubFactory(factory.DictFactory)


class MemberAddressDictFactory(factory.Factory):
    class Meta:
        model = model.OptumAddress

    address_1 = factory.Faker("street_address")
    city = factory.Faker("city")
    state = factory.Faker("state")
    postal_code = factory.Faker("postcode")
    address_2 = factory.Faker("street_address")
    postal_code_suffix = factory.Faker("postcode")
    country_code = factory.Faker("country_code")


class ProcessedNotificationFactory(factory.Factory):
    class Meta:
        model = model.ProcessedNotification

    metadata = factory.SubFactory(MetadataFactory)
    record = factory.SubFactory(ProcessedMemberFactory)
    address = factory.SubFactory(data_models.AddressFactory)


class SubFactoryList(factory.SubFactory):
    """Basically copied from `factory.RelatedFactoryList`, tweaked for SubFactory."""

    def __init__(self, factory, size=2, **defaults):
        self.size = size
        super().__init__(factory, **defaults)

    def evaluate(self, instance, step, context):
        parent = super()
        return [
            parent.evaluate(instance, step, context)
            for i in range(self.size if isinstance(self.size, int) else self.size())
        ]


class OptumEmailFactory(factory.Factory):
    class Meta:
        model = model.OptumEmail

    emailId = factory.Faker("email")


class OptumPolicyFactory(factory.Factory):
    class Meta:
        model = model.OptumPolicy

    customerAccountId = factory.Faker("swift11")
    effectiveDate = factory.Faker("date_this_decade")
    terminationDate = factory.Faker(
        "date_this_decade", before_today=False, after_today=True
    )
    planVariationCode = factory.Faker("swift11")
    reportingCode = factory.Faker("swift11")

    @factory.post_generation
    def finalize(obj, *args, **kwargs):
        if obj["effectiveDate"] is None:
            obj["effectiveDate"] = ""
        elif not isinstance(obj["effectiveDate"], str):
            obj["effectiveDate"] = obj["effectiveDate"].isoformat()

        if obj["terminationDate"] is None:
            obj["terminationDate"] = ""
        elif not isinstance(obj["terminationDate"], str):
            obj["terminationDate"] = obj["terminationDate"].isoformat()


class OptumAuthorizedRepresentationFactory(factory.Factory):
    class Meta:
        model = model.OptumEligibilityRecord

    authoReprIsoCountryCode = factory.Faker("country_code")
    authoReprIsoCountryDesc = factory.Faker("country")


class OptumAttributeFactory(factory.Factory):
    class Meta:
        model = model.OptumAttribute

    name = factory.Faker("swift11")
    value = factory.Faker("swift11")


class OptumStreamValueFactory(factory.Factory):
    class Meta:
        model = model.OptumEligibilityRecord

    uniqueId = factory.Faker("swift11")
    employerAssignedId = factory.Faker("swift11")
    firstName = factory.Faker("first_name")
    lastName = factory.Faker("last_name")
    dateOfBirth = factory.Faker("date_this_century")
    primaryMemberId = factory.Faker("swift11")
    dependentTypeId = factory.Faker("swift11")
    dependentTypeDesc = factory.Faker("domain_word")
    subscriberId = factory.Faker("swift11")
    memberId = factory.Faker("swift11")
    altId = factory.Faker("swift11")
    clientId = "12345"  # factory.Faker("swift11")
    clientName = factory.Faker("company")
    customerId = factory.Faker("swift11")
    customerName = factory.Faker("company")
    policies = SubFactoryList(OptumPolicyFactory, size=1)
    emails = SubFactoryList(OptumEmailFactory, size=1)
    attributes = SubFactoryList(OptumAttributeFactory, size=1)
    genderCode = random.choice(["", "F", "M", "O", "U"])
    genderDesc = random.choice(["Female", "Male", "Other", "Unknown"])
    doNotContact = random.choice(["", "True", "False", "f", "F", "t", "T"])
    postaladdresses = SubFactoryList(
        OptumAddressFactory, size=1
    )  # optum left this lowercase :shrug:
    authoritativeRepresentatives = SubFactoryList(
        OptumAuthorizedRepresentationFactory, size=1
    )

    @factory.post_generation
    def finalize(obj, *args, **kwargs):
        obj["dateOfBirth"] = obj["dateOfBirth"].isoformat()
