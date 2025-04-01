from typing import AnyStr, Optional

from mmlib.ops import log

from app.common.gcs import Storage
from app.eligibility.gcs import EligibilityFileManager
from config import settings

logger = log.getLogger(__name__)

"""
The following functions are used to encrypt a file in bucket 
Example for use it in QA2, other env need adjust the env command

- Connect to QA2 worker
```
gcloud auth login --update-adc
tsh kube login maven-clinic-qa2_us-central1_qa2
kubectl exec -it deployment/eligibility-worker -c worker -- bash
```

- Run the following to init python env
```
source /.venv/bin/activate
python -m asyncio
```

- Run script to encrypt file
plain_file: is the plain file path in the bucket
encrypted_file: output encrypted file path in the bucket
kek_name and signing_key_name can be find via an existing file.
For QA2, 
go to bucket https://console.cloud.google.com/storage/browser/5172ce3f-22b9-28b7-e4c8-4418b17e558e/lihe_directory 
find the file: encrypted.csv, click the 3 dots on the right side and click "Edit metadata"
In the metadata: 
  - value of "key" is the kek_name
  - value of the "sigKey" is the signing_key_name
  
In Prod, just find any encrypted file, check the metadata
```
plain_file="lihe_directory/lc_20231013.csv" 
encrypted_file="lihe_encrypted/lc_20231013_encrypted.csv" 
kek_name="****"
signing_key_name="****"

from utils import encrypt_file

await encrypt_file.encrypt(
    plain_file=plain_file,
    encrypted_file=encrypted_file,
    kek_name=kek_name,
    signing_key_name=signing_key_name,
)

# To verify your file:
# DO NOT verify large file, will out of memory!
plain_data = await encrypt_file.read_file(file=plain_file)
read_encrypted = await encrypt_file.read_file(file=encrypted_file)
assert plain_data == read_encrypted
```
"""


async def encrypt(
    *,
    plain_file: str,
    encrypted_file: str,
    kek_name: str,
    signing_key_name: str,
) -> None:
    bucket = settings.GCP().census_file_bucket
    project = settings.GCP().project

    if project and project == "local-dev":
        logger.error("Local dev env does not support encryption")
        return

    storage = Storage(project)
    encrypted = True
    file_manager = EligibilityFileManager(storage, encrypted)
    plain_data = await file_manager.get(name=plain_file, bucket_name=bucket)
    if not plain_data:
        logger.error(
            f"Cannot read file={plain_file}, please make sure file is not empty"
        )
        return

    try:
        await file_manager.put(
            data=plain_data,
            name=encrypted_file,
            bucket_name=bucket,
            kek_name=kek_name,
            signing_key_name=signing_key_name,
        )
        logger.info(
            f"Encryption completed: plain_file={plain_file}, encrypted_file={encrypted_file}"
        )
    except Exception as e:
        logger.error("Error while encrypt file", exc_info=e)


async def read_file(*, file: str) -> Optional[AnyStr]:
    bucket = settings.GCP().census_file_bucket
    project = settings.GCP().project

    storage = Storage(project)
    encrypted = True
    file_manager = EligibilityFileManager(storage, encrypted)
    data_read = await file_manager.get(name=file, bucket_name=bucket)
    return data_read
