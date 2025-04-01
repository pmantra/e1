import enum


class ApmService(str, enum.Enum):
    ELIGIBILITY_TASKS = "eligibility-tasks"
    ELIGIBILITY_WORKER = "eligibility-worker"
    ELIGIBILITY_JOBS = "eligibility-jobs"
    ELIGIBILITY_DRYRUN = "eligibility-dryrun"
