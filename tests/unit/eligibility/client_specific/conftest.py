import pytest
from maven import feature_flags

from app.eligibility import constants as e9y_constants


@pytest.fixture
def ff_test_data():
    with feature_flags.test_data() as td:
        yield td


@pytest.fixture
def mock_release_microsoft_cert_based_auth_enabled(ff_test_data):
    def _mock(is_on: bool = True):
        ff_test_data.update(
            ff_test_data.flag(
                e9y_constants.E9yFeatureFlag.RELEASE_MICROSOFT_CERT_BASED_AUTH,
            ).variation_for_all(is_on)
        )

    return _mock
