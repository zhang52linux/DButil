import asyncio
from common.async_sentinel import AsyncRedisSentinelHelper


async def main():
    # redis info
    uri_dic = dict(
        sentinel_list=[('xx.xx.xx.xx', 6379), ('xx.xx.xx.xx', 6379), ('xx.xx.xx.xx', 6379)],
        password="password",
        service_name='mymaster',
        db=0
    )
    # create redis link
    rds = AsyncRedisSentinelHelper(uri_dic)
    reader = await rds.writer
    lua_2 = """
        local num = redis.call("llen", KEYS[1]);
        if not num then
            return 0;
        else
            local res = num * ARGV[1];
            return res;
        end
    """
    script_2 = reader.register_script(lua_2)
    print(await script_2(keys=["December:details"], args=[1]))


if __name__ == '__main__':
    asyncio.run(main())
