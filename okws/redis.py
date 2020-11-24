from websockets.exceptions import ConnectionClosed
import websockets
from asyncio.exceptions import CancelledError
import socket
import asyncio
import aioredis
import logging
import json

logger = logging.getLogger(__name__)


class Client():
    """连接到 redis ,并且取得 channel 数据
    当接收到 redis 数据时，会调用 app(request)，并且对于 app(request) 的返回数据，会原样发送到 redis 服务器?
    request:
        _signal_: 有 READY，CONNECTED，ON_DATA，DISCONNECTED，EXIT。
             READY 当要开始联接 ws 服务器前
             CONNECTED 已成功联接上 redis 服务器
             ON_DATA  接收到 redis 服务器 对应 channel 数据，数据在 request['_data_']中.
             DISCONNECTED 当联接中断时发送
             EXIT  clientX 退出
        _server_: self

    例：
    import Client

    async def app(request):
        print(request)

    clientX = Client(app)
    asyncio.run(clientX.run())  # or asyncio.create_task(clientX.run())

    """

    def __init__(self, channel, app, url="redis://localhost"):
        """初始化

        Args:
            channel: subscribe channel
            app (request): 回调函数，当接收到服务器数据时，会调用 app(request)
            url (str, optional): [description]. Defaults to "redis://localhost".
        """
        self.url = url
        self.channel = channel
        self.app = app
        self.task = None

    async def run_app(self, signal, **request):
        request["_signal_"] = signal
        request["_server_"] = self
        return await self.app(request)

    async def reader(self, ch):
        async for message in ch.iter(encoding='utf-8'):
            ret = await self.run_app("ON_DATA", _data_=message)
            if ret == -1:
                return
 
    async def run(self):
        try:
            logger.info("READY")
            await self.run_app("READY")
            redis = await aioredis.create_redis_pool(self.url)
            ch, = await redis.subscribe(self.channel)
            logger.info("已连接")
            await self.run_app("CONNECTED")
            self.task = asyncio.create_task(self.reader(ch))
            await self.task
        except asyncio.CancelledError:
            logger.info("任务取消")
        finally:
            await self.run_app("DISCONNECTED")
            logger.info("redis client stopped.")
            ch.close()
            if redis is not None:
                redis.close()
                await redis.wait_closed()
                redis = None
            await self.run_app("EXIT")

    def close(self):
        if self.task is not None:
            self.task.cancel()
            self.task = None
