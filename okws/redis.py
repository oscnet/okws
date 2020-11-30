import asyncio
import logging
from typing import Union
from .settings import REDIS_URL

import aioredis

logger = logging.getLogger(__name__)


class Redis:
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
    import okws

    async def app(request):
        print(request)

    redis = okws.Redis(channels,app)
    asyncio.run(redis.run())  # or asyncio.create_task(redis.run()) 注意：如果 Redis gets destroyed, 会自动退出任务，请要运行时保持引用有效。

    """

    def __init__(self, channels: Union[list, str], app, url=REDIS_URL):
        """初始化

        Args:
            channels: subscribe channels
            app (request): 回调函数，当接收到 redis publish 数据时，会调用 app(request)
            url (str, optional): redis url . Defaults to "redis://localhost".
        """
        self.url = url
        self.channels = channels if type(channels) == list else [channels]
        self.app = app
        self.tasks = []

    async def run_app(self, signal, **request):
        request["_signal_"] = signal
        request["_server_"] = self
        return await self.app(request)

    async def reader(self, ch):
        async for message in ch.iter():
            try:
                msg = message.decode('utf-8')
                ret = await self.run_app("ON_DATA", _data_=msg)
                if ret == -1:
                    return
            except UnicodeDecodeError:
                logger.warning(f"redis {self.channels}: can't decode {message} to utf-8!")
            except Exception:
                logger.exception(f"redis {self.channels}: app 出错")

    async def run(self):
        try:
            logger.info(f"redis {self.channels}: READY")
            await self.run_app("READY")
            redis = await aioredis.create_redis_pool(self.url)
            channels = await redis.subscribe(*self.channels)
            logger.info(f"redis {self.channels}: 已连接")
            await self.run_app("CONNECTED")
            for ch in channels:
                task = asyncio.create_task(self.reader(ch))
                self.tasks.append(task)
            await asyncio.wait(self.tasks)
            # await self.task

        except asyncio.CancelledError:
            logger.info(f"redis {self.channels}:任务取消")
        finally:
            await self.run_app("DISCONNECTED")
            logger.info(f"redis {self.channels}: stopped.")
            for ch in channels:
                ch.close()
            await self.run_app("EXIT")
            if redis is not None:
                redis.close()
                await redis.wait_closed()


    def close(self):
        for task in self.tasks:
            task.cancel()

    def __del__(self):
        self.close()
