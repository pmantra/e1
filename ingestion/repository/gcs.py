import asyncio
from typing import AnyStr, Optional

import ddtrace

from app.common import crypto, gcs

__all__ = "EligibilityFileManager"


class EligibilityFileManager:
    """A service for uploading and downloading data into GCS."""

    __slots__ = "storage", "crypto", "encrypted", "kek", "sig_key"

    def __init__(self, project: str):
        if project != "local-dev":
            storage = gcs.Storage(project)
            encrypted = True
        else:
            encrypted = False
            storage = gcs.LocalStorage("local-dev")

        self.storage = storage
        self.encrypted = encrypted
        self.crypto = crypto.Cryptographer() if self.encrypted else None

    def __repr__(self):
        encrypted = self.encrypted
        return f"<{self.__class__.__name__} {encrypted=}>"

    @ddtrace.tracer.wrap()
    async def get(self, name: str, bucket_name: str) -> Optional[AnyStr]:
        """Get data saved at `name` in the bucket at `bucket_name` in GCS."""
        loop = asyncio.get_event_loop()
        blob = await self.storage.get_blob(name, bucket_name)
        if not blob:
            return None

        if not isinstance(blob, gcs.LocalBlob):
            # Add filename and file size to datadog span tags
            current_span = ddtrace.tracer.current_span()
            current_span.set_tag("file.name", blob.name)
            current_span.set_tag("file.size", blob.size)

        data = await blob.download(loop=loop)
        metadata = blob.metadata or {}
        if not self.encrypted or not metadata:
            return data

        decrypted, metadata = await self.crypto.decrypt(data, metadata)
        return decrypted

    @ddtrace.tracer.wrap()
    async def put(
        self,
        data: AnyStr,
        name: str,
        bucket_name: str,
        *,
        kek_name: str = None,
        signing_key_name: str = None,
    ):
        """
        upload data to GCS bucket and save as GCS file
        encrypt data if kek_name and signing_key_name passed
        @param data: file content/ data to be stored
        @param name: file name ex: some_directory/encrypted.csv
        @param bucket_name: GCS bucket name
        @param kek_name: The name of the target KEK in GC KMS.
        @param signing_key_name: The name used to determine the signing key for encryption.
        @return: None
        """
        if self.encrypted and kek_name and signing_key_name:
            _, ciphertext, metadata = await self.crypto.encrypt(
                data, kek_name, signing_key_name
            )
            await self.storage.save_blob(
                ciphertext,
                name,
                bucket_name,
                content_type="application/octet-stream",
                **metadata,
            )
        else:
            await self.storage.save_blob(data, name, bucket_name)
