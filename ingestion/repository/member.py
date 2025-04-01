from __future__ import annotations

from typing import List

import ddtrace

from db import model as db_model
from db.clients import member_client as m_client

__all__ = "MemberRepository"


class MemberRepository:
    def __init__(
        self,
        member_client: m_client.Members | None = None,
        use_tmp: bool | None = False,
    ):
        self._use_tmp = use_tmp
        self._member_client: m_client.Members = member_client or m_client.Members()

    @ddtrace.tracer.wrap()
    async def persist_optum_members(
        self, records: List[db_model.ExternalRecordAndAddress]
    ):
        if self._use_tmp:
            return await self._member_client.tmp_bulk_persist_external_records(
                external_records=records
            )
        else:
            return await self._member_client.bulk_persist_external_records(
                external_records=records
            )
