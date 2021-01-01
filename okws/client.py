# 给最终用户的接口， 从 redis 取 okex websockets 的数据
import json
import logging
from typing import Union
import asyncio
import redis
from okws.interceptor import execute
import time
from okws.ws2redis.candle import config as candle
from okws.ws2redis.normal import config as normal

from okws.settings import LISTEN_CHANNEL, REDIS_INFO_KEY, REDIS_URL

logger = logging.getLogger(__name__)


class Client:
    def __init__(self, redis_url=REDIS_URL):
        pool = redis.ConnectionPool.from_url(redis_url, encoding='utf8', decode_responses=True)
        self.redis = redis.Redis(connection_pool=pool)
        self.interceptors = [normal['read'], candle['read']]
        self.id = id(self)
        self.redis_path = f"{REDIS_INFO_KEY}/{self.id}"

    def get(self, name, path, params=None):
        # if self.redis is None:
        #     self.redis = await aioredis.create_redis(self.redis_url)
        if params is None:
            params = {}

        ctx = {
            'id': self.id,
            'name': name,
            'path': path,
            'redis': self.redis
        }
        ctx.update(params)
        asyncio.run(execute(ctx, self.interceptors))
        return ctx.get('response')

    def send(self, cmd: dict):
        # send cmd to websocket
        # if 'name' not in cmd:
        #     logger.warning(f"未指定 websocket 服务名! {cmd}")
        cmd['id'] = self.id
        self.redis.publish(LISTEN_CHANNEL, json.dumps(cmd))

    def open_ws(self, name, auth_params=None):
        if auth_params is None:
            auth_params = {}

        self.send({
            'op': 'open',
            'name': name,
            'args': auth_params
        })

    def get_return_info(self):
        # 取得服务器返回信息
        ret = self.redis.get(self.redis_path)
        logger.info(f"{self.redis_path}:{ret}")
        if ret is not None:
            return json.loads(ret)
        else:
            return ret

    def subscribe(self, name, channels: Union[list, str]):
        self.send({
            'op': 'subscribe',
            'name': name,
            'args': channels if type(channels) == list else [channels]
        })

    async def close_ws(self, name):
        self.send({
            'op': 'close',
            'name': name
        })

    def server_quit(self):
        # 关闭 ws
        self.send({
            'op': 'quit_server'
        })

    def servers(self):
        # 返回当前运行的 ws
        self.send({
            'op': 'servers'
        })
    def server_status(self,server):
        # 返回对应服务器状态
        path = f"okex/{server}/status"
        return self.redis.get(path)


    def redis_clear(self, path="okex/*"):
        # 清除 redis 服务器中的相关数据
        keys = self.redis.keys(path)
        for key in keys:
            self.redis.delete(key)


def client(redis_url=REDIS_URL) -> Client:
    # 使用些函数初始化 OKEX 类
    okex = Client(redis_url)
    return okex
