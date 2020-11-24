"""处理 okex ws 数据
"""
import json
import logging
import aioredis
from interceptor.interceptor import Interceptor, execute
from okws.ws.okex.decode import decode
from .candle import config as candle
from .normal import config as normal

logger = logging.getLogger(__name__)


class App(Interceptor):
    MAX_ARRAY_LENGTH = 100,

    def __init__(self, name, exchange_params={}, redis_url="redis://localhost"):
        self.name = name
        self.redis_url = redis_url
        self.redis = None
        self.decode = decode(exchange_params)

    async def __call__(self, ctx):
        return await execute(ctx, [self.decode, self])

    async def enter(self, request):
        # logger.debug(f"request={request}")
        if request['_signal_'] == 'READY' and self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.redis_url)
        elif request['_signal_'] == 'CONNECTED':
            await self.redis.publish(f"okex/{self.name}/event", json.dumps({'op': 'CONNECTED'}))
            logger.debug(f"{self.name} 已连接")
        elif request['_signal_'] == 'DISCONNECTED':
            await self.redis.publish(f"okex/{self.name}/event", json.dumps({'op': 'DISCONNECTED'}))
            logger.debug(f"{self.name} 已连接")
        elif request['_signal_'] == 'EXIT':
            await self.redis.publish(f"okex/{self.name}/event", json.dumps({'op': 'EXIT'}))
            logger.info(f"{self.name} 退出")
            await self.close()
        elif request['_signal_'] == 'ON_DATA':
            logger.debug(request['DATA'])
            if "table" in request['DATA']:
                await self.redis.publish(f"okex/{self.name}/{request['DATA']['table']}", request['_data_'])
                # save to redis
                await execute({"data": request['DATA'], "redis": self.redis, "name": self.name},
                              [normal['write'], candle['write']])

            elif "event" in request['DATA']:
                await self.redis.publish(f"okex/{self.name}/event", request['_data_'])
                if request['DATA']['event'] == 'error':
                    logger.warning(f"{self.name} 收到错误信息：{request['DATA']}")
                else:
                    logger.info(f"{self.name} ：{request['DATA']}")
            else:
                logger.warn(f"{self.name} 收到未知数据：{request['_data_']}")

    async def close(self):
        if self.redis is not None:
            self.redis.close()
            await self.redis.wait_closed()
            self.redis = None

    def __del__(self):
        # logger.info('退出')
        if self.redis is not None:
            self.redis.close()
