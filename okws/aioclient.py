# 给最终用户的接口， 从 redis 取 okex websockets 的数据
import asyncio
import json
import logging
from typing import Union

import aioredis

from okws.interceptor import execute
from okws.ws2redis.candle import config as candle
from okws.ws2redis.normal import config as normal
from .settings import default_settings

logger = logging.getLogger(__name__)


class Client:
    async def get(self, name, path, params=None):
        if params is None:
            params = {}
        ctx = {
            'id': self.id,
            'name': name,
            'path': path,
            'redis': self.redis
        }
        ctx.update(params)
        await execute(ctx, self.interceptors)
        return ctx.get('response')

    async def close(self):
        if self.redis is not None:
            self.redis.close()
            await self.redis.wait_closed()
            self.redis = None

    def __init__(self, REDIS_URL, REDIS_INFO_KEY, LISTEN_CHANNEL, **argv):
        # 注意初始化要执行 init()
        self.redis_url = REDIS_URL
        self.redis = None
        self.interceptors = [normal['read'], candle['read']]
        self.id = id(self)
        self.redis_path = f"{REDIS_INFO_KEY}/{self.id}"
        self.listen_channel = LISTEN_CHANNEL

    async def init(self):
        self.redis = await aioredis.create_redis(self.redis_url)

    async def send(self, cmd: dict):
        # send cmd to websocket
        # if 'name' not in cmd:
        #     logger.warning(f"未指定 websocket 服务名! {cmd}")
        cmd['id'] = self.id
        await self.redis.publish_json(self.listen_channel, cmd)

    async def open_ws(self, name, auth_params=None):
        if auth_params is None:
            auth_params = {}
        await self.send({
            'op': 'open',
            'name': name,
            'args': auth_params
        })
        # await self.redis.publish_json(LISTEN_CHANNEL)
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def subscribe(self, name, channels: Union[list, str]):
        await self.send({
            'op': 'subscribe',
            'name': name,
            'args': channels if type(channels) == list else [channels]
        })
        await asyncio.sleep(0)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def close_ws(self, name):
        await self.send({
            'op': 'close',
            'name': name
        })
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def server_quit(self):
        await self.send({
            'op': 'quit_server'
        })
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def servers(self):
        await self.send({
            'op': 'servers'
        })
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def redis_clear(self, path="okex/*"):
        # 清除 redis 服务器中的相关数据
        keys = await self.redis.keys(path)
        for key in keys:
            await self.redis.delete(key)

    def __del__(self):
        logger.debug('退出')
        if self.redis is not None:
            self.redis.close()


async def client(configs=None) -> Client:
    # 使用些函数初始化 OKEX 类
    if configs is None:
        configs = {}
    configs.update(default_settings)
    okex = Client(**configs)
    await okex.init()
    return okex
