import asyncio
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

    async def save2mongo(self, coll, data):
        writer = self.mongo.writer(coll)
        return await writer.write(data)

    async def findOne(self, coll_name, filter):
        return await self.mongo.find_one(coll_name=coll_name, filter=filter)


async def updateFileds():
    coll = "dossen_project_data"
    filter = {"dataType": "roomPiceAndRisk", "date": "2022-01-04", "rid": "0759017", "appCode": "ctrip"}
    mongo_writer = BaseMongoSpider()
    result = await mongo_writer.findOne(coll_name=coll, filter=filter)
    result["risk"] = "高风险"
    print(result)
    # await mongo_writer.save2mongo(coll=coll, data=[result])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(updateFileds())
