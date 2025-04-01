import os
from unittest.mock import patch

import pytest

from app.eligibility import constants as e9y_constants
from config import settings
from db.sqlalchemy.sqlalchemy_config import init_engine


class TestSqlalchemyConfig:
    def test_init_engine_with_statement_timeout(self):
        db_settings = settings.DB()

        with patch(
            "db.sqlalchemy.sqlalchemy_config.create_engine"
        ) as mock_create_engine:
            init_engine(db_settings=db_settings, max_execution_seconds=10)
            mock_create_engine.assert_called_once_with(
                "postgresql+psycopg2://postgres:@eligibility-db:5432/api",
                isolation_level="AUTOCOMMIT",
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={"options": "-c statement_timeout=10000"},
            )

    def test_init_engine_without_statement_timeout(self):
        db_settings = settings.DB()

        with patch(
            "db.sqlalchemy.sqlalchemy_config.create_engine"
        ) as mock_create_engine:
            init_engine(db_settings=db_settings, max_execution_seconds=None)
            mock_create_engine.assert_called_once_with(
                "postgresql+psycopg2://postgres:@eligibility-db:5432/api",
                isolation_level="AUTOCOMMIT",
                pool_pre_ping=True,
                pool_recycle=3600,
            )

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
        Checks the behavior of `create_engine` based on the feature flag when `init_engine`
        is called. The expectation is that if disabled, it will use DB_HOST with the
        standard port of 5432, and if enabled, it will use DB_HOST_2 with a port of 5436.
        """
        # Given
        host_1 = "host_1"
        host_2 = "host_2"

        # When
        with patch.dict(os.environ, {"DB_HOST": host_1, "DB_HOST_2": host_2}), patch(
            "maven.feature_flags.bool_variation"
        ) as mock_bool_variation, patch(
            "db.sqlalchemy.sqlalchemy_config.create_engine"
        ) as mock_create_engine:
            mock_bool_variation.side_effect = lambda url, **kwargs: {
                e9y_constants.E9yFeatureFlag.RELEASE_ELIGIBILITY_DATABASE_INSTANCE_SWITCH: flag_enabled
            }.get(url, kwargs.get("default", "nope"))
            db_settings = settings.DB()
            init_engine(db_settings=db_settings, max_execution_seconds=None)

            # Then
            expected_host = host_1
            expected_port = 5432 if not flag_enabled else 5436
            expected_url = f"{db_settings.scheme}+psycopg2://{db_settings.user}:{db_settings.password}@{expected_host}:{str(expected_port)}/{db_settings.db}"
            args, _ = mock_create_engine.call_args

            assert expected_url in args
