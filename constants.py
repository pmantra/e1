import pathlib
from typing import Final

from mmlib.ops import stats

POD: Final[str] = stats.PodNames.CORE_SERVICES.value

APP_NAME = "eligibility"
APP_FACET = ""
PROJECT_DIR = pathlib.Path(__file__).resolve().parent
PROTOBUF_DIR = PROJECT_DIR / "api" / "protobufs" / "generated" / "python"

# for systems that require a project id, even if you're on local dev (like the pubsub emulator)
LOCAL_PROJECT = "local-dev"
