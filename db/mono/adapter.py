"""Largely taken from `aiosql.adapters.asyncpg`.

There are probably bugs.

https://github.com/nackjicholson/aiosql/blob/master/aiosql/adapters/asyncpg.py
"""

from collections import defaultdict
from contextlib import asynccontextmanager

import aiomysql
from aiosql.patterns import var_pattern


class MaybeAcquire:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        if isinstance(self.client, aiomysql.Pool):
            self._managed_conn = await self.client.acquire()
            return self._managed_conn
        self._managed_conn = None
        return self.client

    async def __aexit__(self, exc_type, exc, tb):
        if self._managed_conn is not None:
            await self.client.release(self._managed_conn)


class AsyncMySQLAdapter:
    is_aio_driver = True

    def __init__(self):
        self.var_replacements = defaultdict(dict)

    def process_sql(self, query_name, _op_type, sql):
        count = 0
        adj = 0
        replacement = "%s"
        replacement_len = len(replacement)
        for match in var_pattern.finditer(sql):
            gd = match.groupdict()
            # Do nothing if the match is found within quotes.
            if gd["dblquote"] is not None or gd["quote"] is not None:
                continue

            var_name = gd["var_name"]
            if var_name not in self.var_replacements[query_name]:
                count += 1
                self.var_replacements[query_name][var_name] = count

            start = match.start() + len(gd["lead"]) + adj
            end = match.end() - len(gd["trail"]) + adj

            sql = sql[:start] + replacement + sql[end:]
            # the lead ":" char is the reason for the +1
            var_len = len(var_name) + 1
            if replacement_len < var_len:
                adj = adj + replacement_len - var_len
            else:
                adj = adj + var_len - replacement_len

        return sql

    def maybe_order_params(self, query_name, parameters):
        if isinstance(parameters, dict):
            xs = [
                (self.var_replacements[query_name][k], v) for k, v in parameters.items()
            ]
            xs = sorted(xs, key=lambda x: x[0])
            return [x[1] for x in xs]
        elif isinstance(parameters, tuple):
            return parameters
        else:
            raise ValueError(
                f"Parameters expected to be dict or tuple, received {parameters}"
            )

    async def select(
        self, conn: aiomysql.Connection, query_name, sql, parameters, record_class=None
    ):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, parameters)
                results = await cursor.fetchall()
            if record_class is not None:
                results = [record_class(**dict(rec)) for rec in results]
        return results

    async def select_one(self, conn, query_name, sql, parameters, record_class=None):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, *parameters)
                result = await cursor.fetchone()
            if result is not None and record_class is not None:
                result = record_class(**dict(result))
        return result

    async def select_value(self, conn, query_name, sql, parameters):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, *parameters)
                result = await cursor.fetchone()
        return result[0] if result else None

    @asynccontextmanager
    async def select_cursor(self, conn, query_name, sql, parameters):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, *parameters)
                yield cursor

    async def insert_returning(self, conn, query_name, sql, parameters):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, *parameters)
                result = await cursor.fetchall()
            if result:
                return result[0] if len(result) == 1 else result
            else:
                return None

    async def insert_update_delete(self, conn, query_name, sql, parameters):
        parameters = self.maybe_order_params(query_name, parameters)
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.execute(sql, *parameters)

    async def insert_update_delete_many(self, conn, query_name, sql, parameters):
        parameters = [
            self.maybe_order_params(query_name, params) for params in parameters
        ]
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                await cursor.executemany(sql, *parameters)

    @staticmethod
    async def execute_script(conn, sql):
        connection: aiomysql.Connection
        async with MaybeAcquire(conn) as connection:
            cursor: aiomysql.Cursor
            async with connection.cursor() as cursor:
                return await cursor.execute(sql)
