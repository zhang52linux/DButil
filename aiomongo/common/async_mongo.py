# -*- coding:utf-8 -*-
import logging
import asyncio
import motor.motor_asyncio
from pymongo import UpdateOne, database


root_logger = logging.getLogger()


class MongoGetter:

    def __init__(self, client, collection, body=None, return_fields=None, page_size=10000, total_size=None,
                 cursor=None, retry=5, logger=root_logger):
        self.client = client
        self.collection = collection
        self.body = body or {}
        self.return_fields = dict.fromkeys(return_fields, 1) if return_fields else None
        self.page_size = page_size
        self.retry = retry
        self.total_size = total_size
        self.fetch_count = 0
        self.buffer = []
        self.logger = logger
        self.cursor = cursor if cursor else self.client[self.collection].find(self.body, self.return_fields)

    async def get_data(self):
        if self.total_size is None:
            if self.body:
                self.total_size = await self.client[self.collection].count_documents(self.body)
            else:
                self.total_size = await self.client[self.collection].estimated_document_count()
        if self.fetch_count >= self.total_size:
            raise StopAsyncIteration
        async for document in self.cursor:
            self.buffer.append(document)
            self.fetch_count += 1
            if len(self.buffer) >= self.page_size:
                break

    def __aiter__(self):
        return self

    async def __anext__(self, retry=1):
        try:
            await self.get_data()  # 调用一次就会往buffer中插入100条记录
            finished_rate = self.fetch_count / self.total_size if self.total_size else 1
            self.logger.info("fetch count {} from {}, total count {}, finished {:.5f}%".format(self.fetch_count, self.collection, self.total_size, finished_rate * 100))
            result, self.buffer = self.buffer, []
            return result
        except StopAsyncIteration:
            raise
        except BaseException:
            if self.retry > retry:
                await asyncio.sleep(2)
                return await self.__anext__(retry + 1)
            else:
                raise


class MongoWriter:

    def __init__(self, client, collection, retry=5, logger=root_logger):
        self.client = client
        self.collection = collection
        self.retry = retry
        self.logger = logger
        self.total_count = 0
        self.succ_count = 0
        self.fail_count = 0

    async def write(self, documents, raise_error=True):
        # nInserted 不存在则插入， $set用来更新字段的
        if not documents:
            return {"writeErrors": [], "writeConcernErrors": [], "nInserted": 0, "nUpserted": 0, "nMatched": 0,
                    "nModified": 0, "nRemoved": 0, "upserted": []}
        # upsert 意思是不存在则插入: 特殊的更新,如果没有找到符合条件的更新文档条件,就会以这个条件和更新文档为基础创建一个新的文档
        # 存在则正常更新，优点: 解决了多并发下先查询是否存在在决定是插入还是更新导致的竞态问题
        documents = [UpdateOne({'_id': each["_id"]}, {"$set": each}, upsert=True) for each in documents]
        self.total_count += len(documents)
        for i in range(self.retry):
            try:
                resp = await self.client[self.collection].bulk_write(documents)
                self.succ_count += len(documents)
                self.logger.info("success write {} documents to {}, total write {}".format(len(documents), self.collection, self.succ_count))
                return resp.bulk_api_result
            except BaseException:
                if i + 1 == self.retry and raise_error:
                    self.fail_count += len(documents)
                    raise
                await asyncio.sleep(2)


class AsyncMongo:

    def __init__(self, config, logger=root_logger):
        self.addr = "mongodb://{username}:{password}@{host}:{port}/{database}".format(**config)
        self.client: database.Database = motor.motor_asyncio.AsyncIOMotorClient(self.addr)[config["database"]]
        self.logger = logger

    def getter(self, collection, body=None, return_fields=None, page_size=1000, retry=5):
        return MongoGetter(self.client, collection, body, return_fields, page_size, retry=retry, logger=self.logger)

    def writer(self, collection, retry=5):
        return MongoWriter(self.client, collection, retry, self.logger)

    async def find_one(self, coll, filter, retry=3, raise_error=True):
        try:
            data = await self.client[coll].find_one(filter)
            return data
        except BaseException:
            if retry:
                return await self.find_one(coll, filter, retry - 1, raise_error)
            elif raise_error:
                raise

    async def fetch_all(self, coll, filter, return_fields=None, retry=3, raise_error=True):
        data = []
        cursor = self.client[coll].find(filter, return_fields)
        for i in range(retry):
            try:
                async for document in cursor:
                    data.append(document)
            except Exception as e:
                if i + 1 >= retry and raise_error:
                    raise
            else:
                break
        return data
