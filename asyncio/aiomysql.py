import asyncio
import aiomysql


async def test_example(loop):
    pool = await aiomysql.create_pool(host='xxx.xxxx.xxxx.xxxxx', port=3306,
                                      user='xxxxx', password='xxxxx',
                                      db='xxxxx', loop=loop, charset="utf8", autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # await cur.execute('INSERT into 邓紫棋 VALUES("1","1","1");')
            await cur.execute("SELECT * from 邓紫棋;")
            # res = await cur.fetchone()
            # print(res)
            res = await cur.fetchmany(2)
            print(res)
            # res = await cur.fetchall()
            # print(res)

    pool.close()
    await pool.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))
