import os
from unittest.mock import patch

import pytest
from tests.conftest import _mock_namespace

from app.eligibility import constants as e9y_constants
from db.clients.postgres_connector import compose_dsn, get_dsn


class TestGetDsn:
    """
    There are 2 primary scenarios to test:
    1. Without an APP_ENVIRONMENT_NAMESPACE present (the default configuration for dev, normal QA, production)
    2. With an APP_ENVIRONMENT_NAMESPACE environment variable present (the multi-tenant QA configuration)
    """

    def test_without_namespace_env(self):
        """
        The default un-namespaced DSN for dev / prod based off the yml config files
        """

        # Given
        # When
        dsn = get_dsn()
        # Then
        assert (
            dsn
            == "postgresql://postgres:@eligibility-db:5432/api?sslmode=prefer&search_path=eligibility,public&application_name=eligibility"
        )
        assert dsn.info.name == "/api"

    def test_with_namespace_env(self, patch_namespace):
        """
        The namespaced DSN for multi-tenant QA based off the APP_ENVIRONMENT_NAMESPACE env variable and yml config files
        """

        # Given
        # When
        dsn = get_dsn()
        # Then
        assert (
            dsn
            == f"postgresql://postgres:@eligibility-db:5432/{_mock_namespace}__api?sslmode=prefer&search_path=eligibility,public&application_name=eligibility"
        )
        assert dsn.info.name == f"/{_mock_namespace}__api"


class TestComposeDsn:
    @pytest.mark.parametrize(
        argnames="scheme, user, password, host, port, db, schemas, ssl, app, expected_dsn",
        argvalues=[
            (  # Single schema
                "postgresql",
                "test-user",
                "test-password",
                "test-host",
                5432,
                "test-db",
                ("eligibility",),
                "public",
                "test-app",
                "postgresql://test-user:test-password@test-host:5432/test-db?sslmode=prefer&search_path=eligibility&application_name=test-app",
            ),
            (  # Multiple schemas
                "postgresql",
                "test-user",
                "test-password",
                "test-host",
                5432,
                "test-db",
                ("eligibility", "schmeligibility"),
                "public",
                "test-app",
                "postgresql://test-user:test-password@test-host:5432/test-db?sslmode=prefer&search_path=eligibility,schmeligibility&application_name=test-app",
            ),
            (  # No SSL
                "postgresql",
                "test-user",
                "test-password",
                "test-host",
                5432,
                "test-db",
                ("eligibility", "schmeligibility"),
                None,
                "test-app",
                "postgresql://test-user:test-password@test-host:5432/test-db?sslmode=disable&search_path=eligibility,schmeligibility&application_name=test-app",
            ),
            (  # No App
                "postgresql",
                "test-user",
                "test-password",
                "test-host",
                5432,
                "test-db",
                ("eligibility", "schmeligibility"),
                "public",
                None,
                "postgresql://test-user:test-password@test-host:5432/test-db?sslmode=prefer&search_path=eligibility,schmeligibility",
            ),
        ],
    )
    def test_method_parameters(
        self, scheme, user, password, host, port, db, schemas, ssl, app, expected_dsn
    ):
        # Given
        # When
        dsn = compose_dsn(
            scheme,
            user,
            password,
            host,
            port,
            db,
            *schemas,
            ssl=ssl,
            app=app,
        )
        # Then
        assert dsn == expected_dsn

    @pytest.mark.parametrize(
        argnames="flag_enabled",
        argvalues=[
            [False],
            [True],
        ],
        ids=[
            "db_switch_flag_disabled",
            "db_switch_flag_enabled",
        ],
    )
    def test_release_eligibility_database_instance_switch_feature_flag(
        self,
        flag_enabled: bool,
    ):
        """
        Checks the behavior of `compose_dsn` based on the feature flag when `get_dsn` is
        called. The expectation is that if disabled, it will use DB_HOST with the
        standard port of 5432, and if enabled, it will use DB_HOST_2 with a port of 5436.
        """
        # Given
        host_1 = "host_1"
        host_2 = "host_2"

        # When
        with patch.dict(os.environ, {"DB_HOST": host_1, "DB_HOST_2": host_2}), patch(
            "maven.feature_flags.bool_variation"
        ) as mock_bool_variation, patch(
            "db.clients.postgres_connector.compose_dsn"
        ) as mock_compose_dsn:
            mock_bool_variation.side_effect = lambda url, **kwargs: {
                e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_DATABASE_INSTANCE_SWITCH: flag_enabled
            }.get(url, kwargs.get("default", "nope"))
            get_dsn()

        # Then
        expected_host = host_1
        expected_port = 5432 if not flag_enabled else 5436
        args, _ = mock_compose_dsn.call_args

        assert expected_host in args
        assert expected_port in args
