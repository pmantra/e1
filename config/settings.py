from pathlib import Path
from typing import Optional

import dotenv
import typic

CUR_DIR = Path(__file__).resolve().parent


@typic.settings(prefix="APP_")
class App:
    env: str = "local"
    version: str = "0.0.0"

    @property
    def dev_enabled(self):
        return self.env == "local"

    @property
    def is_non_prod(self):
        return self.env != "production"

    @property
    def environment(self):
        return self.env


@typic.settings(prefix="ASYNCIO_")
class Asyncio:
    debug: bool = True


@typic.settings(prefix="DD_")
class Datadog:
    service: str = ""


@typic.settings(prefix="LOG_")
class Log:
    level: str = "info"
    json: bool = False


@typic.settings(prefix="PUBSUB_")
class Pubsub:
    emulator_host: Optional[str] = None


@typic.settings(prefix="ADMIN_")
class Admin:
    secret_key: str = "3dea91098701acbf8bada4d8ccda29f5f616bebcd3e8ceb5"
    can_create_member: bool = False


@typic.settings(prefix="GCP_")
class GCP:
    project: str = "local-dev"
    data_export_topic: str = "data_export"
    census_file_bucket: str = "census-files"
    census_file_topic: str = "eligibility-file-notifications"
    census_file_group: str = "my-topic-sub"
    census_file_group_tmp: str = "my-topic-sub-2"
    census_file_group_split: str = "file-split-sub"
    integrations_topic: str = "eligibility-integrations"
    integrations_group: str = "e9y-integrations-workers"
    unprocessed_topic: str = "unprocessed-topic"
    unprocessed_group: str = "unprocessed-group"
    processed_topic: str = "processed-topic"
    processed_group: str = "processed-group"


@typic.settings(prefix="REDIS_")
class Redis:
    host: str = "eligibility-redis-master"
    password: str = ""


@typic.settings(prefix="DB_")
class DB:
    scheme: str = "postgresql"
    host: str = "eligibility-db"
    host_2: str = "eligibility-db"
    db: str = "api"
    user: str = "postgres"
    password: str = ""
    schema: str = "eligibility"
    main_port: int = 5432
    read_port: int = 5434


@typic.settings(prefix="MONO_DB_")
class MonoDB:
    host: str = "eligibility-mono-db"
    db: str = "maven"
    user: str = "root"
    password: str = ""


@typic.settings(prefix="MSFT_")
class Microsoft:
    url: str = "https://maveneligibilityservice-uat.azurefd.net/EligibilityApi/EmployeeEligibilityFunction?"
    authority: str = "https://login.windows.net/microsoft.onmicrosoft.com"
    client_id: str = ""
    client_secret: str = ""
    scope: str = "api://4d53d7c1-9d3e-4839-b54f-80c6124b05d8/.default"
    mode: str = "ONLY_CLIENT_CHECK"
    private_key_path = ""
    certificate_path = ""


def load_env():
    envfile = CUR_DIR / ".env"
    if envfile.exists():
        dotenv.load_dotenv(envfile)
