from __future__ import annotations

import dataclasses
import datetime
from typing import Dict, List, Tuple

import ddtrace
import structlog
from datadog import statsd
from ingestion import model, repository, service
from ingestion.service.transform import validate
from mmstream import pubsub

import app.utils.utils
import constants
from config import settings
from db import model as db_model

__all__ = (
    "subscriptions",
    "TransformationService",
    "UnmappedOrganizationError",
    "consume_unprocessed",
)


APP_SETTINGS = settings.App()
GCP_SETTINGS = settings.GCP()
PUBSUB_SETTINGS = settings.Pubsub()

BATCH_SIZE = 1_000

MODULE = __name__
logger = structlog.getLogger(MODULE)

subscriptions = pubsub.PubSubStreams(constants.APP_NAME)


@subscriptions.consumer(
    GCP_SETTINGS.unprocessed_topic,
    group=GCP_SETTINGS.unprocessed_group,
    model=model.UnprocessedNotification,
    auto_create=APP_SETTINGS.dev_enabled,
)
async def consume_unprocessed(
    stream: pubsub.SubscriptionStream[model.UnprocessedNotification],
):
    async with pubsub.PubSubPublisher(
        project=GCP_SETTINGS.project,
        topic=GCP_SETTINGS.processed_topic,
        name=MODULE,
        emulator_host=PUBSUB_SETTINGS.emulator_host,
    ) as publisher:
        transformation_service = TransformationService()

        messages: List[pubsub.PubSubEntry[model.UnprocessedNotification]]

        async for messages in stream.next(count=BATCH_SIZE):
            await transformation_service.transform_batch(
                messages=messages, publisher=publisher
            )
            yield messages


class TransformationService:
    def __init__(
        self,
        ingest_config: repository.IngestConfigurationRepository | None = None,
        file_parser: service.EligibilityFileParser | None = None,
    ):
        self._ingest_config = (
            ingest_config or repository.IngestConfigurationRepository()
        )
        self._file_parser = file_parser or service.EligibilityFileParser()

    @ddtrace.tracer.wrap()
    async def transform_batch(
        self,
        *,
        messages: List[pubsub.PubSubEntry[model.UnprocessedNotification]],
        publisher: pubsub.PubSubPublisher,
    ) -> List[pubsub.PublisherMessage]:
        processed_batch: List[pubsub.PublisherMessage] = []
        for message in messages:
            try:
                # Attempt to transform the record
                processed: model.ProcessedNotification | None = await self._transform(
                    message=message.data
                )
            except Exception as e:
                logger.exception(
                    "Message transform error encountered",
                    metadata=message.data.metadata,
                    error=e,
                )
                continue

            if processed:
                # Add the transformation timestamp to the metadata
                processed.metadata.transformation_ts = datetime.datetime.utcnow()
                # Publish the record
                processed_batch.append(pubsub.PublisherMessage(message=processed))

                statsd.increment(
                    metric="eligibility.transform.count",
                    tags=[f"source:{MODULE}", f"type:{processed.metadata.type}"],
                )

        if processed_batch:
            await publisher.publish(*processed_batch)
            logger.info(
                "Published batch",
                count=len(processed_batch),
                module=MODULE,
            )

        return processed_batch

    @ddtrace.tracer.wrap()
    async def _transform(
        self,
        *,
        message: model.UnprocessedNotification,
    ) -> model.ProcessedNotification | None:
        """Transform a record and publish it to the processed topic"""
        processed: model.ProcessedMember | None = None
        address: model.Address | None = None
        # TODO: Unify the header mapping external ID logic between file/kafka
        if message.metadata.type == repository.IngestionType.FILE:
            processed = await self._transform_file(
                record=message.record, metadata=message.metadata
            )
        elif message.metadata.type == repository.IngestionType.STREAM:
            processed, address = await self._transform_optum(
                record=message.record, metadata=message.metadata
            )
        else:
            logger.info(
                "Message with unsupported ingestion type encountered, could not transform",
                metadata=message.metadata,
            )
            return processed

        return (
            model.ProcessedNotification(
                metadata=message.metadata, record=processed, address=address
            )
            if processed
            else None
        )

    @ddtrace.tracer.wrap()
    async def _transform_file(
        self, *, record: Dict, metadata: model.Metadata
    ) -> model.ProcessedMember:
        """Transform a file based record, update the metadata"""
        # Map and parse the data
        parsed: model.ParsedRecord = await self._map_and_parse(
            record=record, metadata=metadata
        )

        combined: Dict = {
            **record,  # Original record
            **parsed.record,  # mapped and transformed record
        }

        # Format the kwargs we will use to init the dataclass
        final: Dict = {
            f.name: combined[f.name]
            for f in dataclasses.fields(model.ProcessedMember)
            if f.name in combined
        }

        # create the "record" json
        final["record"] = combined

        try:
            org_info: db_model.ExternalMavenOrgInfo | None = (
                await self._ingest_config.get_external_org_info(
                    source=repository.IngestionType(metadata.type),
                    client_id=parsed.record.get("client_id"),
                    organization_id=metadata.organization_id,
                )
            )
            final["organization_id"] = org_info.organization_id
        except UnmappedOrganizationError:
            # Let other exceptions bubble up
            parsed.errors.append(service.ParseErrorMessage.CLIENT_ID_NO_MAPPING)
            # Assign the organization_id to the original data provider
            final["organization_id"] = metadata.organization_id

        # Pull the right fields to form a dict to unpack into kwargs
        return model.ProcessedMember(
            errors=parsed.errors,
            warnings=parsed.warnings,
            file_id=metadata.file_id,
            **final,
        )

    @ddtrace.tracer.wrap()
    async def _transform_optum(
        self, *, record: model.OptumEligibilityRecord, metadata: model.Metadata
    ) -> Tuple[model.ProcessedMember, model.Address] | Tuple[None, None]:
        """Transform an optum record, update the metadata"""
        org_info: db_model.ExternalMavenOrgInfo | None = (
            await self._ingest_config.get_external_org_info(
                source=repository.IngestionType(metadata.type),
                client_id=record["clientId"],
                customer_id=record["customerId"],
            )
        )

        if not org_info:
            logger.info(
                "Skipping record - missing org info",
                client_id=record["clientId"],
                customer_id=record["customerId"],
            )
            return None, None

        # extract effective range for policies
        effective_range: model.EffectiveRange | None = service.resolve_effective_range(
            record["policies"], datetime.date.today()
        )

        if not validate.is_effective_range_activated(
            org_info.activated_at, effective_range
        ):
            logger.warning(
                "Got an external record with an effective range before configuration/organization activated.",
            )
            return None, None

        # extract email
        email: str = ""
        if emails := record["emails"]:
            email = emails[0]["emailId"]

        # optum waterfall logic
        # extract corp id
        corp_id: str = (
            # Finest-grained identifier for health plans
            record["memberId"]
            # Group identifier for a family (may be the same as member ID for plan owner)
            or record["subscriberId"]
            # Provided by non-health plans (should be always)
            or record["altId"]
            # Worst-case, use primaryMemberId (Optum-specific, guaranteed unique)
            or record["primaryMemberId"]
        )

        # extract address
        address: model.Address = service.resolve_member_address(
            record.get("postaladdresses")
        )
        # extract do not contact information
        do_not_contact: str = service.resolve_do_not_contact(
            record.get("doNotContact", "")
        )
        # extract gender information
        gender: str = app.utils.utils.resolve_gender_code(record.get("genderCode", ""))

        # extract date of birth
        date_of_birth: datetime.date = service.to_date(record["dateOfBirth"])

        # Add external_id - this is used in persistence
        record["external_id"] = record["clientId"]
        # Mark the source
        record["source"] = "optum"

        # abfe: extract attributes from record into custom_attributes
        custom_attributes = service.parse_custom_attributes(record["attributes"])

        return (
            model.ProcessedMember(
                errors=[],
                warnings=[],
                first_name=record["firstName"],
                last_name=record["lastName"],
                date_of_birth=date_of_birth,
                email=email,
                unique_corp_id=corp_id,
                dependent_id=record["primaryMemberId"],
                employer_assigned_id=record["employerAssignedId"],
                effective_range=effective_range,
                record=record,
                gender_code=gender,
                do_not_contact=do_not_contact,
                organization_id=org_info.organization_id,
                custom_attributes=custom_attributes,
            ),
            address,
        )

    @ddtrace.tracer.wrap()
    async def _map_and_parse(
        self, *, record: Dict, metadata: model.Metadata
    ) -> model.ParsedRecord:
        """Take a record and map it to internal fields and parse it"""
        # Pull the header alias mappings
        header_mapping: Dict = await self._ingest_config.get_header_mapping(
            source=repository.IngestionType(metadata.type),
            organization_id=metadata.organization_id,
        )

        # Transform into dict with internal fields
        mapped: Dict = TransformationService._map_fields(
            record=record, mapping=header_mapping
        )

        # Parse the data
        return self._file_parser.parse(row=mapped)

    @staticmethod
    @ddtrace.tracer.wrap()
    def _map_fields(*, record: Dict, mapping: Dict) -> Dict:
        """
        With the mapping provided, return a new record containing additional mapped fields
        if the field is contained in the provided mapping or is already the mapped value
        """
        mapped: Dict = {}
        for key, value in record.items():
            if key in mapping:
                # the external field needs to be mapped to an internal field
                mapped[mapping[key]] = value
            elif key in mapping.values():
                # the external field is already an internal field
                mapped[key] = value

        return mapped


class UnmappedOrganizationError(Exception):
    """An exception class to throw when attempting to process an unmapped organization"""

    pass
