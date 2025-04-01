import pytest
from ingestion import repository


@pytest.fixture
def ingest_config(
    file_test_client, configuration_test_client, header_test_client, maven
):
    return repository.IngestConfigurationRepository(
        file_client=file_test_client,
        config_client=configuration_test_client,
        header_client=header_test_client,
        mono_client=maven,
    )


@pytest.fixture
def member_repo(member_test_client):
    return repository.MemberRepository(member_client=member_test_client)
