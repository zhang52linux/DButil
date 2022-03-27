# _*_ coding:utf-8 _*_
from loguru import logger
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
import pymongo
import motor.motor_asyncio
from pymongo.operations import DeleteOne, UpdateOne
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import BulkWriteError
from pymongo.results import BulkWriteResult
from typing import Sequence
from common.async_wrap_funs import retry_if_exception


class MongoGetter:

    def __init__(self, client, collection, body: dict = None, return_fields: dict = None, page_size: int = 10000000000, batch_size: int = 100, total_size: int = None, cursor=None, retry=5):
        self.client = client
        self.collection = collection
        self.body = body or {}
        self.return_fields = dict.fromkeys(return_fields, 1) if return_fields else None
        self.page_size = page_size
        self.retry = retry
        self.total_size = total_size
        self.fetch_count = 0
        self.buffer = []
        self.cursor = cursor if cursor else self.client.find(self.body, self.return_fields).batch_size(batch_size)

    async def get_data(self):
        if self.total_size is None:
            self.total_size = await self.client.count_documents(self.body) if self.body else await self.client.estimated_document_count()

        if self.fetch_count >= self.total_size:
            raise StopAsyncIteration

        async for document in self.cursor:
            self.buffer.append(document)
            self.fetch_count += 1
            if len(self.buffer) >= self.page_size:
                break

    def __aiter__(self):
        '''Return an async iterator object'''
        return self

    async def __anext__(self, retry=1):
        try:
            await self.get_data()
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
            return {"writeErrors": [], "writeConcernErrors": [], "nInserted": 0, "nUpserted": 0, "nMatched": 0, "nModified": 0, "nRemoved": 0, "upserted": []}
        documents = [UpdateOne({'_id': each["_id"]}, {"$set": each}, upsert=True) for each in documents]

        self.total_count += len(documents)
        for i in range(self.retry):
            try:
                result: BulkWriteResult = await self.client.bulk_write(documents, ordered=False)
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
        return motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs, serverSelectionTimeoutMS=5000)

    def get_database(self, database_name: str) -> Database:
        """ Get database object. """
        return self.client[database_name]

    def get_collection(self, coll_name: str, **kwargs) -> Collection:
        """ Get collection object. """
        return self.db.get_collection(coll_name, **kwargs)

    # -----------------------------CRUD---------------------------------- #

    async def create_index(self, coll_name: str, keys: Sequence, sort_type=pymongo.ASCENDING, unique=False):
        """Creates an index on this collection.

        Args:
            coll_name ([type]): collection name.
            keys (Sequence): The key that creates the index(Single or compound).
            sort_type (int, optional): Type of index created. Defaults to pymongo.ASCENDING.
                `pymongo.ASCENDING`, `pymongo.DESCENDING`, `pymongo.GEO2D`, `pymongo.GEOHAYSTACK`, `pymongo.GEOSPHERE`, `pymongo.HASHED`, `pymongo.TEXT`
            unique (bool, optional): creates a uniqueness constraint on the index. Defaults to True.

        """
        coll = self.get_collection(coll_name)
        _keys = [(key, sort_type) for key in keys] if isinstance(keys, (list, tuple)) else [(keys, sort_type)]

        return await coll.create_index(_keys, unique=unique, background=True)

    async def get_index_info(self, coll_name: str):
        """ Get collection index information. """
        return await self.get_collection(coll_name).index_information()

    async def get_count(self, coll_name: str, filter: dict = {}):
        """ Get the number of collection documents. """
        collect = self.get_collection(coll_name)
        return await collect.count_documents(filter) if filter else await collect.estimated_document_count()

    # ------------------------------------ CRUD ------------------------------------ #

    def getter(self, collection, body=None, return_fields=None, retry=5):
        return MongoGetter(self.get_collection(collection), collection, body, return_fields, retry=retry)

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

    @retry_if_exception(BaseException)
    async def fetch_all(self, coll_name: str, filter: dict = {}, return_fields: list = None, batch_size: int = 10000, retry: int = 3, raise_error: bool = True):
        '''
        cursor:
            - 游标是一种能从包括多条数据记录的结果集中每次提取一条记录的机制
            - 游标充当指针的作用
            - 游标是一种临时的数据库对象
            - 游标由结果集和结果集中指向特定记录的游标位置组成
        cursor用途:
            - 游标的一个常见用途就是保存查询结果
            - 如果处理过程需要重复使用一个记录集，那么创建一次游标而重复使用若干次，比重复查询数据库要快的多
        batch_size:
            - 限制每批响应中要返回的文档数。每个批次都需要往返服务器(限制游标最多有batch_size个数据)
            - 比如说: select语句对应的数据集有10000条数据, batch_size设置为100, 则要往返服务器100次去获取数据
            - 每批次最多返回16mb数据(return 4-16MB of results per batch)
            - 如果字段多且内容复杂, 可能当游标中的数据只有1000条的时候, 数据量已经达到了16mb, 那么实际游标中只有1000条数据,而不会是设置的batch_size个
        limit:
            - 限制此游标返回的结果数, 只从游标中取多少数据
        '''
        data = list()
        projection = dict.fromkeys(return_fields, 1) if return_fields else None
        cursor = self.get_collection(coll_name).find(filter, projection).sort('_id', pymongo.ASCENDING).batch_size(batch_size)
        async for document in cursor:
            data.append(document)
        return data
