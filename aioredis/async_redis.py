# _*_ coding:utf-8 _*_
import asyncio
import aioredis
import uvloop  # pip3 install uvloop <速度更快, 适用linux>
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class AsyncRedis(object):

    def __init__(self, config):
        self.default_db = config["db"]
        self.config = dict(
            url="redis://{0}:{1}".format(config['host'], config['port']),
            password=config.get("password", None),
            socket_timeout=600,
            retry_on_timeout=True,
            socket_keepalive=True,
            decode_responses=True,
            encoding="utf-8",   # 设置编码, 返回后的结果会自动 decode
            max_connections=1000,
            db=config["db"]
        )
        self.client_map = {}
        self.init_scripts()
    
    def init_scripts(self, script_dir=None):
        """ 加载所有lua脚本 默认从scripts目录下获取 """
        self._scripts = {}
        script_dir = script_dir or os.path.join(os.path.dirname(__file__), 'scripts')
        for file_path in glob.glob(os.path.join(script_dir, '*.lua')):
            with open(file_path, 'r') as f:
                script_obj = self.register_script(f.read())
                script_name = os.path.splitext(os.path.basename(file_path))[0]
                self._scripts[script_name] = script_obj

    async def run_script(self, script_name, keys=None, args=None):
        """
        Execute a walrus script with the given arguments.

        :param script_name: The base name of the script to execute.
        :param list keys: Keys referenced by the script.
        :param list args: Arguments passed in to the script.
        :returns: Return value of script.

        .. note:: Redis scripts require two parameters, ``keys``
            and ``args``, which are referenced in lua as ``KEYS``
            and ``ARGV``.
        """
        return await self._scripts[script_name](keys, args)

    async def zset_set_by_score(self, name: str, min: int, max: int, score: int, num: int = '') -> list:
        """ 获取指定区间数据并修改分数为指定值 """
        return await self.run_script('zset_set_by_score', keys=[name], args=[min, max, score, num])

    async def zset_increase_by_score(self, name: str, min: int, max: int, increment: int, num: int = '') -> list:
        """ 获取指定区间数据并修改分数加上增量 """
        return await self.run_script('zset_increase_by_score', keys=[name], args=[min, max, increment, num])

    async def zset_del_by_score(self, name: str, min: int, max: int, num: int = '') -> list:
        """ 获取指定区间数据并删除 """
        return await self.run_script('zset_del_by_score', keys=[name], args=[min, max, num])

    @staticmethod
    async def _isclosed(client):
        try:
            return "PONG" != await client.ping()
        except BaseException:
            return True

    async def get_async_redis_client_pool(self, db):
        if db not in self.client_map or await self._isclosed(self.client_map[db]):
            pool = aioredis.ConnectionPool.from_url(**self.config)
            self.client_map[db] = aioredis.Redis(connection_pool=pool)
        return self.client_map[db]

    async def client(self, db=None) -> aioredis.Redis:
        return await self.get_async_redis_client_pool(self.default_db if db is None else db)
