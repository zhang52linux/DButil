# _*_ coding:utf-8 _*_
import os
import glob
import asyncio
import aioredis
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
from typing import Mapping, Optional, Union
from aioredis.connection import ConnectionPool


class AsyncRedisDB(aioredis.Redis):
    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 6379,
        db: Union[str, int] = 0,
        password: Optional[str] = None,
        socket_timeout: Optional[float] = 600,
        socket_connect_timeout: Optional[float] = None,
        socket_keepalive: Optional[bool] = True,
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None,
        connection_pool: Optional[ConnectionPool] = None,
        unix_socket_path: Optional[str] = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        retry_on_timeout: bool = True,
        ssl: bool = False,
        ssl_keyfile: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: Optional[str] = None,
        ssl_check_hostname: bool = False,
        max_connections: Optional[int] = None,
        single_connection_client: bool = False,
        health_check_interval: int = 0,
        client_name: Optional[str] = None,
        username: Optional[str] = None,
    ):
        super().__init__(host=host, port=port, db=db, password=password, socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout, socket_keepalive=socket_keepalive, socket_keepalive_options=socket_keepalive_options, connection_pool=connection_pool, unix_socket_path=unix_socket_path, encoding=encoding, encoding_errors=encoding_errors, decode_responses=decode_responses,
                         retry_on_timeout=retry_on_timeout, ssl=ssl, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_cert_reqs=ssl_cert_reqs, ssl_ca_certs=ssl_ca_certs, ssl_check_hostname=ssl_check_hostname, max_connections=max_connections, single_connection_client=single_connection_client, health_check_interval=health_check_interval, client_name=client_name, username=username)
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


class AsyncRedis(object):

    def __init__(self, config):
        self.default_db = config.get("db", 0)
        self.config = dict(
            db=config.get("db", 0),
            password=config.get("password", None),
            max_connections=1000,
            encoding="utf-8",
            decode_responses=True
        )
        self.client_map = {}
        self.url = "redis://{0}:{1}".format(config['host'], config['port'])

    @staticmethod
    async def _isclosed(client):
        try:
            return "PONG" != await client.ping()
        except BaseException:
            return True

    async def get_async_redis_client_pool(self):
        if self.config["db"] not in self.client_map or await self._isclosed(self.client_map[self.config["db"]]):
            pool = aioredis.ConnectionPool.from_url(url=self.url, **self.config)
            self.client_map[self.config["db"]] = AsyncRedisDB(connection_pool=pool)
        return self.client_map[self.config["db"]]

    async def client(self, db=None) -> aioredis.Redis:
        """ 默认不给db赋值 """
        self.config["db"] = self.default_db if db is None else db
        return await self.get_async_redis_client_pool()
