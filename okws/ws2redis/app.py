"""处理 okex ws 数据
"""
import json
import logging
import aioredis
from okws.interceptor import Interceptor, execute
import okws
from okws.ws2redis.candle import config as candle
from okws.ws2redis.normal import config as normal

logger = logging.getLogger(__name__)


def App(name, api_params=None, redis_url="redis://localhost"):
    if api_params is None:
        api_params = {}
    decode = okws.okex.Decode(api_params)
    ws2redis = Ws2redis(name, redis_url)

    async def app(ctx):
        await execute(ctx, [decode, ws2redis])

    return app


class Ws2redis(Interceptor):
    MAX_ARRAY_LENGTH = 100,

    def __init__(self, name, redis_url="redis://localhost"):
        self.name = name
        self.redis_url = redis_url
        self.redis = None
        # 用于指示当前 ws 状态，分别有 READY，CONNECTED，DISCONNECTED，EXIT，ON_DATA
        self.status_path = f"okex/{self.name}/status"
        self.event_path = f"okex/{self.name}/event"

    async def enter(self, request):
        # logger.debug(f"request={request}")
        if request['_signal_'] == 'READY' and self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.redis_url)
        elif request['_signal_'] == 'CONNECTED':
            await self.redis.publish(self.event_path, json.dumps({'op': 'CONNECTED'}))
            await self.redis.setex(self.status_path, 1,'CONNECTED')
            logger.info(f"{self.name} 已连接")
        elif request['_signal_'] == 'DISCONNECTED':
            await self.redis.publish(self.event_path, json.dumps({'op': 'DISCONNECTED'}))
            await self.redis.set(self.status_path, 'DISCONNECTED')
            logger.info(f"{self.name} DISCONNECTED")
        elif request['_signal_'] == 'EXIT':
            await self.redis.publish(self.event_path, json.dumps({'op': 'EXIT'}))
            await self.redis.set(self.status_path, 'EXIT')
            logger.info(f"{self.name} 退出")
            await self.close()
        elif request['_signal_'] == 'ON_DATA':
            # 用于指示收到数据
            await self.redis.publish(self.event_path, json.dumps({'op': 'ON_DATA'}))
            await self.redis.setex(self.status_path, 1, 'ON_DATA')
            logger.debug(request['DATA'])
            if "table" in request['DATA']:
                await self.redis.publish(f"okex/{self.name}/{request['DATA']['table']}", json.dumps(request['DATA']))
                # save to redis
                await execute({"data": request['DATA'], "redis": self.redis, "name": self.name},
                              [normal['write'], candle['write']])

            elif "event" in request['DATA']:
                await self.redis.publish(self.event_path, json.dumps(request['DATA']))
                if request['DATA']['event'] == 'error':
                    logger.warning(f"{self.name} 收到错误信息：{request['DATA']}")
                else:
                    logger.info(f"{self.name} ：{request['DATA']}")
            else:
                logger.warning(f"{self.name} 收到未知数据：{request['DATA']}")

    async def close(self):
        if self.redis is not None:
            self.redis.close()
            await self.redis.wait_closed()
            self.redis = None

    def __del__(self):
        # logger.info('退出')
        if self.redis is not None:
            self.redis.close()
