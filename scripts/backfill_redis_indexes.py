import asyncio
from collections import defaultdict

import aioredis
from mmlib.ops import log
from mmlib.redis.client import make_dsn

from config import settings
from db import tmp

log = log.getLogger(__name__)


async def backfill():
    log.info("Backfilling indexes for parsed records.")
    store = tmp.ParsedRecordStorage(
        make_dsn(settings.redis.host, password=settings.redis.password)
    )
    await store.store.redis.initialize()
    try:
        log.info("Querying for all existing keys.")
        query = store.query(pagesize=100_000, values=False)
        files, orgs = defaultdict(set), []
        orgsadd = orgs.append
        getfileix = store.fileix
        getorgix = store.orgix
        async for page in query:
            for k in page:
                typ, action, orgid, fileid, _ = k.split(":")
                files[fileid].add(k)
                orgsadd(orgid)
        log.info("Got keys for orgs/files.")
        log.info("Filling indexes.")
        redis: aioredis.Redis = store.store.redis.redis
        for fileid, orgid in zip(files, orgs):
            members = files[fileid]
            member = members.pop()
            await redis.sadd(getfileix(fileid), member, *members)
            await redis.sadd(getorgix(orgid), member, *members)
        log.info("Done backfilling indexes.")
    finally:
        await store.store.redis.teardown()


if __name__ == "__main__":
    asyncio.run(backfill())
