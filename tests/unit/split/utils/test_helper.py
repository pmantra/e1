import pytest
from split.utils import helper
from tests.factories.data_models import ConfigurationFactory


class TestHelper:
    @staticmethod
    @pytest.mark.parametrize(
        argnames="is_data_provider, expected",
        argvalues=[
            (True, False),
            (False, False),
        ],
    )
    def test_is_parent_org(is_data_provider, expected):
        # Given
        config = ConfigurationFactory.create()
        config.data_provider = is_data_provider

        # Then
        assert helper.is_parent_org(config) == expected
