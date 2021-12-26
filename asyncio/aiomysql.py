import asyncio
import aiomysql


async def test_example(loop):
    pool = await aiomysql.create_pool(host='192.168.0.124', port=3306,
                                      user='root', password='today',
                                      db='vip_music', loop=loop, charset="utf8", autocommit=True)
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
