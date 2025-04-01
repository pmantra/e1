import pytest
from verification import repository


@pytest.fixture
def member_versioned_repo(member_versioned_test_client):
    return repository.MemberVersionedRepository(
        member_versioned_client=member_versioned_test_client
    )


@pytest.fixture
def verification_repo(
    verification_test_client,
    verification_attempt_test_client,
    member_verification_test_client,
):
    return repository.VerificationRepository(
        verification_client=verification_test_client,
        verification_attempt_client=verification_attempt_test_client,
        member_verification_client=member_verification_test_client,
    )
