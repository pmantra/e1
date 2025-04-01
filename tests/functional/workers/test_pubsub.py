import datetime
from unittest import mock

import pytest
from tests.factories import data_models as factory
from tests.factories.workers.pubsub.pubsub_factory import (
    ExternalMessageAttributesFactory,
    ExternalRecordFactory,
    ExternalRecordFactoryNoAddress,
    PubSubMessageFactory,
)

from app.worker import pubsub
from app.worker.pubsub import (
    get_cached_external_org_infos_by_value,
    retrieve_external_org_info,
)
from db.mono.client import MavenOrgExternalID

# Needed to run async tests, otherwise they are skipped
pytestmark = pytest.mark.asyncio


class TestExternalRecordNotificationsHandler:
    @staticmethod
    async def test_process_publish_external_record(
        member_test_client,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        subscription,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        attributes = ExternalMessageAttributesFactory.create(
            external_id=external_id["external_id"], source=external_id["source"]
        )
        notification = ExternalRecordFactory.create(
            client_id=external_id["external_id"],
            record={"client_id": external_id["external_id"]},
        )
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)
        subscription.next.return_value.__aiter__.return_value = [[message]]

        # When
        # TODO: we really should not be needing to patch this to have the same connection between our test setup and client
        # to be investigated in https://app.shortcut.com/maven-clinic/story/89130/investigate-why-we-need-to-patch-member-client
        with mock.patch(
            "db.clients.member_client.Members", return_value=member_test_client
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned",
            return_value=member_versioned_test_client,
        ), mock.patch(
            "db.clients.configuration_client.Configurations",
            return_value=configuration_test_client,
        ):
            result = [
                m
                async for m in pubsub.external_record_notification_handler(subscription)
            ][0]

        # Then
        assert len(result[0]) == 1  # assert we have one saved member
        assert len(result[1]) == 1  # assert we have one saved address
        assert len(result[2]) == 1  # assert we have one saved member_versioned
        assert len(result[3]) == 1  # assert we have one saved member_address_versioned

    @staticmethod
    async def test_process_publish_external_record_dual_write(
        member_test_client,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        subscription,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        attributes = ExternalMessageAttributesFactory.create(
            external_id=external_id["external_id"], source=external_id["source"]
        )
        notification = ExternalRecordFactory.create(
            client_id=external_id["external_id"],
            record={"client_id": external_id["external_id"]},
        )
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)
        subscription.next.return_value.__aiter__.return_value = [[message]]

        # When
        # TODO: we really should not be needing to patch this to have the same connection between our test setup and client
        # to be investigated in https://app.shortcut.com/maven-clinic/story/89130/investigate-why-we-need-to-patch-member-client
        with mock.patch(
            "db.clients.member_client.Members", return_value=member_test_client
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned",
            return_value=member_versioned_test_client,
        ), mock.patch(
            "db.clients.configuration_client.Configurations",
            return_value=configuration_test_client,
        ):
            result = [
                m
                async for m in pubsub.external_record_notification_handler(subscription)
            ][0]

        # Then
        assert len(result[0]) == 1  # assert we have saved one member
        assert len(result[1]) == 1  # assert we have one saved address
        assert len(result[2]) == 1  # assert we have saved one member_versioned
        assert len(result[3]) == 1  # assert we have saved one member_address_versioned

    @staticmethod
    async def test_process_message_for_inactive_org(
        member_test_client,
        member_versioned_test_client,
        configuration_test_client,
        test_inactive_config,
        subscription,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_inactive_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        attributes = ExternalMessageAttributesFactory.create(
            external_id=external_id["external_id"], source=external_id["source"]
        )
        notification = ExternalRecordFactory.create(
            client_id=external_id["external_id"],
            record={"client_id": external_id["external_id"]},
        )
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)
        subscription.next.return_value.__aiter__.return_value = [[message]]

        # When
        # TODO: we really should not be needing to patch this to have the same connection between our test setup and client
        # to be investigated in https://app.shortcut.com/maven-clinic/story/89130/investigate-why-we-need-to-patch-member-client
        with mock.patch(
            "db.clients.member_client.Members", return_value=member_test_client
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned",
            return_value=member_versioned_test_client,
        ), mock.patch(
            "db.clients.configuration_client.Configurations",
            return_value=configuration_test_client,
        ):
            result = [
                m
                async for m in pubsub.external_record_notification_handler(subscription)
            ]

        # Then
        assert len(result) == 0  # assert nothing is persisted.

    @staticmethod
    async def test_handler_multiple_records(
        member_test_client,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        test_organization_external_id: MavenOrgExternalID,
        subscription,
    ):
        # Given
        attributes = ExternalMessageAttributesFactory.create(
            external_id=test_organization_external_id.external_id,
            source=test_organization_external_id.source,
        )
        num_records = 10
        messages = []

        # Have half of our records have addresses, half without
        for i in range(num_records // 2):
            notification = ExternalRecordFactory.create(
                client_id=test_organization_external_id.external_id,
                record={"external_id": test_organization_external_id.external_id},
            )
            messages.append(
                PubSubMessageFactory.create(attributes=attributes, data=notification)
            )

        for i in range(num_records // 2):
            notification = ExternalRecordFactoryNoAddress.create(
                client_id=test_organization_external_id.external_id,
                record={
                    "external_id": test_organization_external_id.external_id,
                    "address": None,
                },
            )
            messages.append(
                PubSubMessageFactory.create(attributes=attributes, data=notification)
            )

        subscription.next.return_value.__aiter__.return_value = [messages]

        # When
        # TODO: we really should not be needing to patch this to have the same connection between our test setup and client
        # to be investigated in https://app.shortcut.com/maven-clinic/story/89130/investigate-why-we-need-to-patch-member-client
        with mock.patch(
            "db.clients.member_client.Members", return_value=member_test_client
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned",
            return_value=member_versioned_test_client,
        ), mock.patch(
            "db.clients.configuration_client.Configurations",
            return_value=configuration_test_client,
        ):
            results = [
                m
                async for m in pubsub.external_record_notification_handler(subscription)
            ][0]

        # Then
        assert (
            len(results[2]) == num_records
        )  # assert we saved correct number of member_versioned
        assert (
            len(results[3]) == num_records // 2
        )  # assert we saved correct number of member_address_versioned

    @pytest.mark.parametrize(
        "given_external_id,client_id,customer_id",
        [("123:foo", "123", "foo"), ("123", "123", "foobar")],
        ids=["exact_match_compound_key", "no_customer_id_configured"],
    )
    async def test_retrieve_external_info_success(
        self,
        test_config,
        configuration_test_client,
        given_external_id,
        client_id,
        customer_id,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id,
            external_id=given_external_id,
        )
        await configuration_test_client.add_external_id(**external_id)

        source = external_id["source"]
        attributes = ExternalMessageAttributesFactory.create(source=source)
        notification = ExternalRecordFactoryNoAddress.create(
            client_id=client_id,
            customer_id=customer_id,
        )
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)

        # When
        result = await retrieve_external_org_info(
            client_id=message.data.client_id,
            customer_id=message.data.customer_id,
            source=source,
            configs=configuration_test_client,
        )

        # Then
        assert (result.organization_id, result.activated_at) == (
            test_config.organization_id,
            test_config.activated_at,
        )

    async def test_retrieve_external_info_no_match(
        self, test_config, configuration_test_client
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        source = external_id["source"]
        attributes = ExternalMessageAttributesFactory.create(source=source)
        notification = ExternalRecordFactoryNoAddress.create()
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)

        # When
        result = await retrieve_external_org_info(
            client_id=message.data.client_id,
            customer_id=message.data.customer_id,
            source=source,
            configs=configuration_test_client,
        )

        # Then
        assert result is None

    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_cached_external_infos_by_value(
        self,
        configuration_test_client,
    ):
        with mock.patch(
            "db.clients.configuration_client.Configurations.get_external_org_infos_by_value_and_source",
            return_value="mock_value",
        ) as get_external_ids_mock:
            # When
            # get_cached_external_ids_by_value called once
            await get_cached_external_org_infos_by_value(
                source="foo",
                external_id="bar",
                configs=configuration_test_client,
            )
        # Then
        # Underlying async coroutine awaited once
        assert get_external_ids_mock.await_count == 1

    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_cached_external_infos_by_value_use_cache(
        self,
        configuration_test_client,
    ):
        with mock.patch(
            "db.clients.configuration_client.Configurations.get_external_org_infos_by_value_and_source",
            return_value="mock_value",
        ) as get_external_ids_mock:
            # Given
            # get_cached_external_ids_by_value called
            await get_cached_external_org_infos_by_value(
                source="foo",
                external_id="bar",
                configs=configuration_test_client,
            )
            # When
            # get_cached_external_ids_by_value called a second time
            await get_cached_external_org_infos_by_value(
                source="foo",
                external_id="bar",
                configs=configuration_test_client,
            )
        # Then
        # Underlying async coroutine only awaited once
        assert get_external_ids_mock.await_count == 1

    @pytest.mark.usefixtures("reset_external_id_cache")
    async def test_get_cached_external_infos_by_value_time_out(
        self,
        configuration_test_client,
    ):
        with mock.patch(
            "db.clients.configuration_client.Configurations.get_external_org_infos_by_value_and_source",
            return_value="mock_value",
        ) as get_external_ids_mock, mock.patch(
            "app.utils.async_ttl_cache.AsyncTTLCache._InnerCache._get_time_to_live_value"
        ) as ttl_func_mock:
            # Given
            # Set TTL to be 0:30:01 ago so that a second call will already be considered expired
            # The TTL value is set when the initial call is made, so the mocked TTL value must
            # be set before the first call to get_cached_external_ids_by_value.
            ttl_func_mock.return_value = datetime.datetime.now() - datetime.timedelta(
                minutes=30, seconds=1
            )
            await get_cached_external_org_infos_by_value(
                source="foo",
                external_id="bar",
                configs=configuration_test_client,
            )
            # get_cached_external_ids_by_value called once
            assert get_external_ids_mock.await_count == 1

            # When
            # Call get_cached_external_ids_by_value again
            await get_cached_external_org_infos_by_value(
                source="foo",
                external_id="bar",
                configs=configuration_test_client,
            )
        # Then
        # Underlying async coroutine awaited twice
        assert get_external_ids_mock.await_count == 2


class TestExternalRecordNotificationsHandlerHash:
    @staticmethod
    async def test_process_publish_external_record(
        member_test_client,
        member_versioned_test_client,
        configuration_test_client,
        test_config,
        subscription,
    ):
        # Given
        external_id = factory.ExternalIDFactory.create(
            organization_id=test_config.organization_id
        )
        await configuration_test_client.add_external_id(**external_id)

        attributes = ExternalMessageAttributesFactory.create(
            external_id=external_id["external_id"], source=external_id["source"]
        )
        notification = ExternalRecordFactory.create(
            client_id=external_id["external_id"],
            record={"client_id": external_id["external_id"]},
        )
        message = PubSubMessageFactory.create(attributes=attributes, data=notification)
        subscription.next.return_value.__aiter__.return_value = [[message]]

        # When
        # TODO: we really should not be needing to patch this to have the same connection between our test setup and client
        # to be investigated in https://app.shortcut.com/maven-clinic/story/89130/investigate-why-we-need-to-patch-member-client
        with mock.patch(
            "db.clients.member_client.Members", return_value=member_test_client
        ), mock.patch(
            "db.clients.member_versioned_client.MembersVersioned",
            return_value=member_versioned_test_client,
        ), mock.patch(
            "db.clients.configuration_client.Configurations",
            return_value=configuration_test_client,
        ):
            [
                m
                async for m in pubsub.external_record_notification_handler(subscription)
            ][0]

        # Then
        saved_member = await member_test_client.all()
        saved_member_address = await member_test_client.get_address_by_member_id(
            member_id=saved_member[0].id
        )
        assert len(saved_member) == 1
        assert saved_member_address is not None

        saved_member_version = await member_versioned_test_client.all()
        saved_member_version_address = (
            await member_versioned_test_client.get_address_by_member_id(
                member_id=saved_member_version[0].id
            )
        )
        assert len(saved_member_version) == 1
        assert saved_member_version_address is not None
        assert saved_member_version[0].hash_value is not None
