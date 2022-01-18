# -*- coding:utf-8 -*-
import asyncio
from asyncio.log import logger
from common.async_mongo import AsyncMongo


class BaseMongo:
    uri_dic = dict(
        host="47.106.82.140",
        port=55555,
        username="yafeng",
        password="yafeng",
        database="yafeng_data",
    )
    mongo = AsyncMongo(**uri_dic)


class BaseMongoSpider(BaseMongo):
    def __init__(self):
        super(BaseMongoSpider, self).__init__()

    async def writer_data(self, coll, data):
        writer = self.mongo.writer(coll)
        return await writer.write(data)

    async def reader_data(self, coll_name, filter):
        return self.mongo.getter(collection=coll_name, body=filter)

    async def find_all(self, coll_name, filter):
        return await self.mongo.fetch_all(coll_name=coll_name, filter=filter)


async def updateFileds():
    coll = "dossen_project_bussiness_data"
    filter = {"dataType": "bussiness", "appCode": "meituan", "dossenId": "0772076"}
    mongo_writer = BaseMongoSpider()
    import time
    start_time = time.time()
    getter = await mongo_writer.reader_data(coll_name=coll, filter=filter)
    async for docs in getter:
        # logger.info("-------------------------------------")
        await mongo_writer.writer_data(coll=coll, data=docs)
    end_time = time.time()
    print(end_time - start_time)

    start_time = time.time()
    result = await mongo_writer.find_all(coll_name=coll, filter=filter)
    end_time = time.time()
    print(end_time - start_time)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateFileds())
