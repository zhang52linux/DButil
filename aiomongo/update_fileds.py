# -*- coding:utf-8 -*-
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# 原生模块 第三方模块 自定义模块
from common.async_mongo import AsyncMongo


class BaseMongo:
    uri_dic = dict(
        host="xx.xx.xx.xx",
        port=27042,
        username="xxx",
        password="xxx",
        database="xxx",
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
    coll = "dossen_project_data"
    filter = {"dataType": "room", "date": {'$gte': "2021-12-01", '$lte': "2022-02-20"}, "appCode": "ctrip", "rid": "2431001"}
    mongo_writer = BaseMongoSpider()
    getter = await mongo_writer.reader_data(coll_name=coll, filter=filter)
    async for docs in getter:
        deal_docs = []
        for doc in docs:
            for it in doc['data']:
                if it['pmsRoomTypeId'] == "JT":
                    it['pmsRoomTypeId'] = "JTF"
            deal_docs.append(doc)
        await mongo_writer.writer_data(coll=coll, data=deal_docs)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateFileds())
