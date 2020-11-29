# 给最终用户的接口， 从 redis 取 okex websockets 的数据
import asyncio
import json
import logging
from typing import Union

import aioredis
from okws.interceptor import execute

from okws.ws.okex.candle import config as candle
from okws.ws.okex.normal import config as normal

from .settings import LISTEN_CHANNEL, REDIS_INFO_KEY, REDIS_URL

logger = logging.getLogger(__name__)


class _Client:
    async def get(self, name, path, params={}):
        # if self.redis is None:
        #     self.redis = await aioredis.create_redis(self.redis_url)
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

    def __init__(self, redis_url=REDIS_URL):
        # 注意初始化要执行 init()
        self.redis_url = redis_url
        self.redis = None
        self.interceptors = [normal['read'], candle['read']]
        self.id = id(self)
        self.redis_path = f"{REDIS_INFO_KEY}/{self.id}"

    async def init(self):
        self.redis = await aioredis.create_redis(self.redis_url)

    async def send(self, cmd: dict):
        # send cmd to websocket
        # if 'name' not in cmd:
        #     logger.warning(f"未指定 websocket 服务名! {cmd}")
        await self.redis.publish_json(LISTEN_CHANNEL, cmd)

    async def open_ws(self, name, auth_params={}):
        await self.send({
            'id': self.id,
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
            'id': self.id,
            'op': 'subscribe',
            'name': name,
            'args': channels if type(channels) == list else [channels]
        })
        await asyncio.sleep(0)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def close_ws(self, name):
        await self.send({
            'id': self.id,
            'op': 'close',
            'name': name
        })
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def server_quit(self):
        await self.send({
            'id': self.id,
            'op': 'quit_server'
        })
        await asyncio.sleep(1)
        ret = await self.redis.get(self.redis_path, encoding='utf-8')
        return json.loads(ret)

    async def servers(self):
        await self.send({
            'id': self.id,
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


async def create_control(redis_url=REDIS_URL) -> _Client:
    # 使用些函数初始化 OKEX 类
    okex = _Client(redis_url)
    await okex.init()
    return okex
