from __future__ import annotations

import datetime
from unittest import mock

import pytest
from tests.factories import data_models as factory
from tests.factories.workers.pubsub import pubsub_factory

from app.eligibility.convert import COUNTRY_UNKNOWN
from app.worker import pubsub, types
from db import model
from db.clients import configuration_client
from db.mono import client as mono_client

pytestmark = pytest.mark.asyncio


@pytest.fixture
def org() -> mono_client.MavenOrganization:
    return factory.MavenOrganizationFactory.create()


# region file handling


class TestFileNotifications:
    """Tests for our file-notification handling logic."""

    @staticmethod
    async def test_handler(
        files,
        configs,
        maven,
        publisher,
        subscription,
        file: model.File,
        config: model.Configuration,
        org: mono_client.MavenOrganization,
    ):
        # Given
        notification = pubsub_factory.FileUploadNotificationFactory.create(
            name=file.name
        )
        message = pubsub_factory.PubSubMessageFactory.create(data=notification)
        subscription.__aiter__.return_value = [message]
        maven.get_org_from_directory.return_value = org
        file.id = 1
        files.persist.return_value = file
        with mock.patch(
            "split.utils.helper.is_parent_org",
            return_value=False,
        ):
            # When
            files = [f async for f in pubsub.file_notification_handler(subscription)]
            # Then
            assert files
            assert publisher.publish.call_args == mock.call({"file_id": file.id})

    @staticmethod
    async def test_handler_parent_file(
        files,
        configs,
        maven,
        publisher,
        subscription,
        file: model.File,
        config: model.Configuration,
        org: mono_client.MavenOrganization,
    ):
        # Given
        notification = pubsub_factory.FileUploadNotificationFactory.create(
            name=file.name
        )
        message = pubsub_factory.PubSubMessageFactory.create(data=notification)
        subscription.__aiter__.return_value = [message]
        maven.get_org_from_directory.return_value = org
        file.id = 1
        files.persist.return_value = file
        with mock.patch(
            "split.utils.helper.is_parent_org",
            return_value=True,
        ):
            # When
            files = [f async for f in pubsub.file_notification_handler(subscription)]
            # Then
            assert not files
            assert not publisher.publish.called

    @staticmethod
    async def test_handler_got_directory(
        subscription,
        publisher,
    ):
        # Given
        notification = pubsub_factory.FileUploadNotificationFactory.create(name="foo/")
        message = pubsub_factory.PubSubMessageFactory.create(data=notification)
        subscription.__aiter__.return_value = [message]
        # When
        files = [f async for f in pubsub.file_notification_handler(subscription)]
        # Then
        assert not files
        assert not publisher.publish.called

    @staticmethod
    async def test_handler_no_org(
        subscription,
        maven,
        publisher,
    ):
        # Given
        notification = pubsub_factory.FileUploadNotificationFactory.create()
        message = pubsub_factory.PubSubMessageFactory.create(data=notification)
        maven.get_org_from_directory.return_value = None
        subscription.__aiter__.return_value = [message]
        # When
        files = [f async for f in pubsub.file_notification_handler(subscription)]
        # Then
        assert not files
        assert not publisher.publish.called


# endregion


# region record/message handling


class TestExternalRecordNotifications:
    @staticmethod
    async def test_handler_bad_attributes(
        subscription,
        members,
    ):
        # Given
        notification = pubsub_factory.ExternalRecordFactory.create()
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes={}, data=notification
        )
        subscription.next.return_value.__aiter__.return_value = [[message]]
        # When
        results = [
            m async for m in pubsub.external_record_notification_handler(subscription)
        ]
        # Then
        assert not results
        assert not members.bulk_persist_external_records.called

    # region extract records
    async def test_extract_records_correct_translation(self):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create()
        expected_member = factory.MemberFactory.create(effective_range=None)
        notification = types.ExternalMemberRecord(
            first_name=expected_member.first_name,
            last_name=expected_member.last_name,
            date_of_birth=expected_member.date_of_birth,
            work_state=expected_member.work_state,
            email=expected_member.email,
            unique_corp_id=expected_member.unique_corp_id,
            effective_range=None,
            do_not_contact=expected_member.do_not_contact,
            gender_code=expected_member.gender_code,
            client_id="test123",
            customer_id="test",
        )
        # When
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        values_to_check = {
            "first_name": expected_member.first_name,
            "last_name": expected_member.last_name,
            "date_of_birth": expected_member.date_of_birth,
            "unique_corp_id": expected_member.unique_corp_id,
            "source": message.attributes["source"],
            "external_id": message.attributes["external_id"],
            "external_name": message.attributes["external_name"],
            "received_ts": mock.ANY,
            "record": message.attributes,
        }
        for key, value in values_to_check.items():
            assert returned_record[0]["external_record"][key] == value

    @pytest.mark.parametrize(
        "input_country_code, expected_country_code",
        [("US", "USA"), ("USA", "USA"), (None, ""), ("", ""), ("NYC", COUNTRY_UNKNOWN)],
    )
    async def test_extract_records_address(
        self, input_country_code, expected_country_code
    ):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create(source=None)
        notification = pubsub_factory.ExternalRecordFactory.create()
        notification.address.country_code = input_country_code

        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )

        # When
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        assert (
            returned_record[0]["record_address"]["country_code"]
            == expected_country_code
        )

    async def test_extract_records_missing_attributes(self):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create(source=None)
        notification = pubsub_factory.ExternalRecordFactory.create()

        # When
        attributes.pop("source")
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        result = await pubsub._extract_records([message], configs)

        # Then
        assert result == []

    async def test_extract_records_effective_before_activated(self):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create()
        expected_member = factory.MemberFactory.create(
            effective_range=model.DateRange(
                lower=None, upper=datetime.date(2022, 12, 31)
            )
        )
        notification = types.ExternalMemberRecord(
            first_name=expected_member.first_name,
            last_name=expected_member.last_name,
            date_of_birth=expected_member.date_of_birth,
            work_state=expected_member.work_state,
            email=expected_member.email,
            unique_corp_id=expected_member.unique_corp_id,
            effective_range=expected_member.effective_range,
            do_not_contact=expected_member.do_not_contact,
            gender_code=expected_member.gender_code,
            client_id="test123",
            customer_id="test",
        )
        configs.get_external_org_infos_by_value_and_source.return_value = [
            model.ExternalMavenOrgInfo(
                organization_id=1, activated_at=datetime.datetime(2023, 1, 1)
            )
        ]

        # When
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        assert returned_record == []

    async def test_extract_records_for_inactive_org(self):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create()
        expected_member = factory.MemberFactory.create(
            effective_range=model.DateRange(
                lower=None, upper=datetime.date(2022, 12, 31)
            )
        )
        notification = types.ExternalMemberRecord(
            first_name=expected_member.first_name,
            last_name=expected_member.last_name,
            date_of_birth=expected_member.date_of_birth,
            work_state=expected_member.work_state,
            email=expected_member.email,
            unique_corp_id=expected_member.unique_corp_id,
            effective_range=expected_member.effective_range,
            do_not_contact=expected_member.do_not_contact,
            gender_code=expected_member.gender_code,
            client_id="test123",
            customer_id="test",
        )
        configs.get_external_org_infos_by_value_and_source.return_value = [
            model.ExternalMavenOrgInfo(organization_id=1, activated_at=None)
        ]

        # When
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        assert returned_record == []

    # endregion extract records

    # region extract records hash
    async def test_extract_records_generate_hash_value(self):
        # Given
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create()
        notification = pubsub_factory.ExternalRecordFactory.create()
        configs.get_external_org_infos_by_value_and_source.return_value = [
            model.ExternalMavenOrgInfo(
                organization_id=1, activated_at=datetime.datetime(2023, 1, 1)
            )
        ]

        # When
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        assert returned_record[0]["external_record"]["hash_value"] is not None
        assert returned_record[0]["external_record"]["hash_version"] is not None

    async def test_extract_records_handle_ssn(self):
        # Given
        test_ssn = "123-45-6789"
        configs = configuration_client.Configurations()
        attributes = pubsub_factory.ExternalMessageAttributesFactory.create()
        notification = pubsub_factory.ExternalRecordFactory.create(
            unique_corp_id=test_ssn
        )
        configs.get_external_org_infos_by_value_and_source.return_value = [
            model.ExternalMavenOrgInfo(
                organization_id=1, activated_at=datetime.datetime(2023, 1, 1)
            )
        ]

        # When
        message = pubsub_factory.PubSubMessageFactory.create(
            attributes=attributes, data=notification
        )
        returned_record = await pubsub._extract_records([message], configs)

        # Then
        assert returned_record[0]["external_record"]["unique_corp_id"] != test_ssn
        assert (
            returned_record[0]["external_record"]["record"]["unique_corp_id"]
            == returned_record[0]["external_record"]["unique_corp_id"]
        )
        assert (
            returned_record[0]["external_record"]["record"][
                "id-resembling-hyphenated-ssn"
            ]
            is True
        )

    # endregion extract records hash


# endregion
