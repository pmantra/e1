from .admin import AdminCommand
from .backfill import BackfillCommand
from .file_ingest import FileIngestCommand
from .file_split import FileSplitCommand
from .get_sub_population_member_ids import GetSubPopulationMemberIdsCommand
from .http_server import HTTPAPICommand
from .persist import PersistCommand
from .pre_verify import PreVerifyCommand
from .purge_duplicate_optum_records import PurgeDuplicateOptumRecordsCommand
from .purge_expired_records import PurgeExpiredRecordsCommand
from .server import APICommand
from .sync import SyncCommand
from .transform import TransforCommand
from .worker import WorkerCommand
from .file_dryrun import FileDryRunCommand

COMMANDS = (
    AdminCommand,
    APICommand,
    HTTPAPICommand,
    SyncCommand,
    WorkerCommand,
    BackfillCommand,
    FileIngestCommand,
    TransforCommand,
    PersistCommand,
    PreVerifyCommand,
    PurgeExpiredRecordsCommand,
    PurgeDuplicateOptumRecordsCommand,
    GetSubPopulationMemberIdsCommand,
    FileSplitCommand,
    FileDryRunCommand,
)
