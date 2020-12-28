import asyncio
import logging
import json
import aioredis
import okws
from okws.interceptor import Interceptor, execute

from .exception import SignalError
from .utils import import_string
from .okws_status import ws_status_listener, okws_exist, open_ws, okws_connect, subscribe

logger = logging.getLogger(__name__)

redis_url = "redis://localhost"

SIGNAL_CHANNEL = "okex/signals"


async def main(config):
    try:

        okex = await okws.client(redis_url)
        if not await okws_exist(okex):
            logger.warning(f"未检测到 okws 运行，程序退出。")
            return

        okws_listen_task = asyncio.create_task(okws_connect(okex, config))

        ws_listen_task = asyncio.create_task(ws_status_listener(okex, config))

        tasks = await listen_and_do(okex, config)

        await open_ws(okex, config)
        await asyncio.sleep(1)
        # 如果相应的 ws 已经连接的话，需要重新订阅一下
        await subscribe(okex, config)

        if tasks:
            await asyncio.wait(tasks)

    except asyncio.CancelledError:
        logger.info("程序退出")
    finally:
        if not ws_listen_task.done():
            ws_listen_task.cancel()
        if not okws_listen_task.done():
            okws_listen_task.cancel()
        logger.info('程序退出')


def run(config):
    try:
        asyncio.run(main(config))
    except KeyboardInterrupt:
        logging.info('Ctrl+C 完成退出')