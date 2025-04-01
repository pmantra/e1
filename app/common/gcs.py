import asyncio
import os
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial
from typing import AnyStr, Optional

from google.cloud import storage

import constants


class AsyncBlob:
    """An asyncio wrapper object around a `google.cloud.storage.Blob`.

    Implements a basic protocol for downloading and uploading a blob in an
    asyncio-friendly context.
    """

    __slots__ = "blob", "pool"

    def __init__(self, blob: storage.Blob, *, pool: ThreadPoolExecutor = None):
        self.blob = blob
        self.pool = pool or ThreadPoolExecutor()

    def __repr__(self):
        name, bucket = self.blob.name, self.blob.bucket.name
        return f"<{self.__class__.__name__} {name=}, {bucket=}>"

    def __getattr__(self, item):
        return getattr(self.blob, item)

    async def download(self, *, loop: asyncio.AbstractEventLoop = None) -> bytes:
        """Download the data located in this Blob asynchronously."""
        loop = loop or asyncio.get_event_loop()
        return await loop.run_in_executor(self.pool, self.blob.download_as_bytes)

    async def upload(
        self,
        data: AnyStr,
        *,
        content_type: str = "text/plain",
        loop: asyncio.AbstractEventLoop = None,
    ):
        """Upload this data into this Blob asynchronously."""
        loop = loop or asyncio.get_event_loop()
        upload = partial(
            self.blob.upload_from_string, data=data, content_type=content_type
        )
        await loop.run_in_executor(self.pool, upload)


class Storage:
    """A simple crud client for fetching or saving a blob in GCS."""

    __slots__ = "client", "pool"

    def __init__(self, project: str = None, dev: bool = False):
        self.client = (
            storage.Client.create_anonymous_client()
            if dev
            else storage.Client(project=project)
        )
        self.client.project = self.client.project or project
        self.pool = ThreadPoolExecutor()

    async def get_blob(self, name: str, bucket_name: str) -> Optional[AsyncBlob]:
        """Fetch a blob from GCS."""
        loop = asyncio.get_event_loop()
        bucket = self.client.bucket(bucket_name)
        blob = await loop.run_in_executor(self.pool, bucket.get_blob, name)
        return blob and AsyncBlob(blob, pool=self.pool)

    async def save_blob(
        self,
        data: AnyStr,
        name: str,
        bucket_name: str,
        *,
        content_type: str = "text/plain",
        **metadata,
    ):
        """Save a blob to GCS with the given data."""
        loop = asyncio.get_event_loop()
        bucket: storage.Bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(name)
        blob.metadata = metadata
        async_blob = AsyncBlob(blob, pool=self.pool)
        await async_blob.upload(data=data, content_type=content_type, loop=loop)


FIXTURES = constants.PROJECT_DIR / ".storage"


class LocalBlob:
    """A local proxy which provides the same interface as `AsyncBlob`."""

    __slots__ = "name", "bucket", "pool", "metadata", "encoding"

    def __init__(self, name: str, bucket_name: str, *, pool: ThreadPoolExecutor = None):
        self.name = name
        self.encoding = "utf8"
        self.bucket = bucket_name
        self.pool = pool or ThreadPoolExecutor()
        self.metadata = {}

    def _read(self) -> Optional[bytes]:
        path = FIXTURES / self.bucket / self.name
        if path.exists():
            return path.read_bytes()
        return None

    def _write(self, data: AnyStr):
        dir = FIXTURES / self.bucket
        dir.mkdir(parents=True, exist_ok=True)

        sub_folder = os.path.dirname(self.name)
        dir_with_sub = dir / sub_folder
        dir_with_sub.mkdir(parents=True, exist_ok=True)

        path = dir / self.name
        path.write_bytes(data) if isinstance(data, bytes) else path.write_text(data)

    async def download(self, *, loop: asyncio.AbstractEventLoop = None):
        loop = loop or asyncio.get_event_loop()
        return await loop.run_in_executor(self.pool, self._read)

    async def upload(
        self,
        data: AnyStr,
        *,
        content_type: str = "text/plain",
        loop: asyncio.AbstractEventLoop = None,
    ):
        loop = loop or asyncio.get_event_loop()
        return await loop.run_in_executor(self.pool, self._write, data)


class LocalStorage:
    """A local mock for running in dev environments."""

    def __init__(self, project: str = None, dev: bool = False):
        self.project = project
        self.blobs = defaultdict(dict)

    async def get_blob(self, name: str, bucket_name: str) -> LocalBlob:
        if name in self.blobs[bucket_name]:
            return self.blobs[bucket_name][name]
        blob = LocalBlob(name, bucket_name=bucket_name)
        self.blobs[bucket_name][name] = blob
        return blob

    async def save_blob(
        self,
        data: AnyStr,
        name: str,
        bucket_name: str,
        *,
        content_type: str = "text/plain",
        **metadata,
    ):
        blob = await self.get_blob(name, bucket_name)
        blob.metadata = metadata
        await blob.upload(data, content_type=content_type)
