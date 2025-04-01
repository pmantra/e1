from tests.conftest import _mock_namespace

from config import settings
from db.mono.client import get_mono_db


class TestGetMonoDb:
    def test_without_namespace(self):
        """
        The default un-namespaced database name for dev / prod based off the yml config files
        """
        # Given
        # When
        # Then
        assert get_mono_db(settings.MonoDB().db) == "maven"

    def test_with_namespace(self, patch_namespace):
        """
        The namespaced database name for multi-tenant QA based off the APP_ENVIRONMENT_NAMESPACE env variable and yml config files
        """
        # Given
        # When
        # Then
        assert get_mono_db(settings.MonoDB().db) == f"{_mock_namespace}__maven"

    def test_with_blank_db(self, patch_namespace):
        # Given
        # When
        # Then
        assert get_mono_db(None) is None
