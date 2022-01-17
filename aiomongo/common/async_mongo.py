# -*- coding:utf-8 -*-
from loguru import logger
import asyncio
import motor.motor_asyncio
from pymongo.operations import DeleteOne, UpdateOne
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pymongo.results import BulkWriteResult


class MongoGetter:

    def __init__(self, client, collection, body=None, return_fields=None, page_size=10000, total_size=None, cursor=None, retry=5):
        self.client = client
        self.collection = collection
        self.body = body or {}
        self.return_fields = dict.fromkeys(return_fields, 1) if return_fields else None
        self.page_size = page_size
        self.retry = retry
        self.total_size = total_size
        self.fetch_count = 0
        self.buffer = []
        self.cursor = cursor if cursor else self.client.find(self.body, self.return_fields)

    async def get_data(self):
        if self.total_size is None:
            if self.body:
                self.total_size = await self.client.count_documents(self.body)
            else:
                self.total_size = await self.client.estimated_document_count()
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
            await self.get_data()  # 调用一次就会往buffer中插入10000条记录
            finished_rate = self.fetch_count / self.total_size if self.total_size else 1
            logger.info("fetch count {} from {}, total count {}, finished {:.5f}%".format(self.fetch_count, self.collection, self.total_size, finished_rate * 100))
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

    def __init__(self, client, collection, retry=5):
        self.client = client
        self.collection = collection
        self.retry = retry
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
                result: BulkWriteResult = await self.client.bulk_write(documents)
                self.succ_count += len(documents)
                logger.success("success write {} documents to {}, total write {}".format(len(documents), self.collection, self.succ_count))
                return result.bulk_api_result
            except BulkWriteError as e:
                if i + 1 == self.retry and raise_error:
                    self.fail_count += len(documents)
                    raise
                await asyncio.sleep(2)


class AsyncMongo:

    def __init__(self, url: str = None, host: str = 'localhost', port: int = 27017, database: str = 'admin', username: str = None, password: str = None, **kwargs):
        if url:
            self.client = self.get_client(url, **kwargs)
        else:
            if password is not None and username is not None:
                self.client = self.get_client(host=host, port=port, username=username, password=password, authSource=database, **kwargs)
            else:
                self.client = self.get_client(host=host, port=port, **kwargs)

        self.db = self.get_database(database)

    def __str__(self) -> str:
        return f'db: {self.db}'

    # ------------------------------------------------------------------------ #

    @staticmethod
    def get_client(*args, **kwargs):
        """ Get mongo async client. """
        return motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs)

    def get_database(self, database_name: str) -> Database:
        """ Get database object. """
        return self.client[database_name]

    def get_collection(self, coll_name: str, **kwargs) -> Collection:
        """ Get collection object. """
        return self.db.get_collection(coll_name, **kwargs)

    # ------------------------------------------------------------------------ #

    def getter(self, collection, body=None, return_fields=None, page_size=1000, retry=5):
        return MongoGetter(self.get_collection(collection), collection, body, return_fields, page_size, retry=retry)

    def writer(self, collection, retry=5):
        return MongoWriter(self.get_collection(collection), collection, retry)

    async def delete(self, coll_name: str, documents: list([dict]), log_switch: bool = True) -> bool:
        # 根据_id删除的数据
        operate_list = [DeleteOne({'_id': i['_id']}) for i in documents if i.get('_id')]

        if not operate_list:
            logger.warning('documents no _id')
            return True

        try:
            collect = self.get_collection(coll_name)
            result: BulkWriteResult = await collect.bulk_write(operate_list, ordered=False)
            log_switch and logger.info(f'mongo:{collect.full_name} | deleted {result.deleted_count} | total {len(documents)}')
            return True
        except BulkWriteError as e:
            logger.error(e.details)
            return False

    async def find_one(self, coll_name: str, filter: dict = {}, return_fields: list = None):
        projection = dict.fromkeys(return_fields, 1) if return_fields else None
        return await self.get_collection(coll_name).find_one(filter, projection) or {}

    async def fetch_all(self, coll_name: str, filter: dict = {}, return_fields: list = None, retry:int = 3, raise_error: bool = True):
        data = list()
        projection = dict.fromkeys(return_fields, 1) if return_fields else None
        cursor = self.get_collection(coll_name).find(filter, projection) or {}
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
