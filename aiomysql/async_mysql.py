# _*_ coding:utf-8 _*_
import asyncio
import traceback
import aiomysql
from loguru import logger
# 通过使用连接池, 大幅度提升查询效率

class AsyncMysqlClient(object):

    def __init__(self, pool):
        self.pool = pool
        self.connection = None

    async def get_client(self):
        self.connection = await self.pool.acquire()
        return self.connection

    async def get_cursor(self):
        if self.connection is None:
            await self.get_client()
        self.cursor = await self.connection.cursor(aiomysql.DictCursor)
        return self.cursor

    async def free_connection(self):
        if self.connection is not None:
            self.pool.release(self.connection)
            self.connection = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        raise TypeError("Use async with instead")

    def __enter__(self):
        raise TypeError("Use async with instead")

    async def __aenter__(self):
        await self.get_cursor()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.cursor and not self.cursor.closed:
            await self.cursor.close()
        await self.free_connection()

    async def create_table(self, sql, retry=3, raise_error=True):
        for i in range(retry):
            try:
                await self.cursor.execute(sql)
                return await self.cursor.fetchone()
            except BaseException:
                logger.error("retry: {}, sql: {}\n{}".format(i, sql, traceback.format_exc()))
                if i + 1 == retry:
                    if raise_error:
                        raise

    async def insert_table(self, sql, retry=3, raise_error=True):
        for i in range(retry):
            try:
                await self.cursor.execute(sql)
                return await self.cursor.fetchone()
            except BaseException:
                logger.error("retry: {}, sql: {}\n{}".format(i, sql, traceback.format_exc()))
                if i + 1 == retry:
                    if raise_error:
                        raise

    async def fetch_one(self, sql, retry=3, raise_error=True):
        for i in range(retry):
            try:
                await self.cursor.execute(sql)
                return await self.cursor.fetchone()
            except BaseException:
                logger.error("retry: {}, sql: {}\n{}".format(i, sql, traceback.format_exc()))
                if i + 1 == retry:
                    if raise_error:
                        raise

    async def fetch_all(self, sql, retry=3, raise_error=True):
        for i in range(retry):
            try:
                await self.cursor.execute(sql)
                return await self.cursor.fetchall()
            except BaseException:
                logger.error("retry: {}, sql: {}\n{}".format(i, sql, traceback.format_exc()))
                if i + 1 == retry:
                    if raise_error:
                        raise


class AsyncMysqlPool:

    def __init__(self, host, port, user, password, db, charset="utf8", minsize=3, maxsize=30):
        self.config = dict(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            charset=charset,
            minsize=minsize,
            maxsize=maxsize
        )
        self.pool = aiomysql.Pool(autocommit=True, echo=False, pool_recycle=-1, loop=asyncio.get_event_loop(), **self.config)

    @property
    def client(self):
        return AsyncMysqlClient(self.pool)
