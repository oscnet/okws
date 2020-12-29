import asyncio
import logging
import json
import okws
from okws.interceptor import Interceptor, execute

from .status import ws_status_listener, okws_exist, open_ws, okws_connect, subscribe

logger = logging.getLogger(__name__)

redis_url = "redis://localhost"

SIGNAL_CHANNEL = "okex/signals"


# 用于运行信号发生器，最后将信号发到 redis 的 SIGNAL_CHANNEL 上
def signal_function(interceptors: [Interceptor], signal, client):
    async def _signal(ctx):
        if ctx.get('_signal_') != 'ON_DATA':
            # signaler 不关心其它事件
            return
        ctx['_okws_'] = client
        ctx['_config_'] = signal
        ctx['_interceptors_'] = interceptors
        await execute(ctx, interceptors)
        try:
            if ctx.get('op') == 'PUBLISH' and ctx.get('PUBLISH'):
                msg = json.dumps(ctx['PUBLISH'], default=str)
                await client.redis.publish(SIGNAL_CHANNEL, msg)
        except Exception:
            logger.exception(f"publish to redis error! {ctx['PUBLISH']}")

    return _signal


def cancel_task(tasks):
    for task in tasks:
        task.cancel()


def create_signaler(signal, client):
    interceptors = []
    for s in signal['signal']:
        # 导入策略信号类
        try:
            algo_class = import_string(s)
            # 初始化类
            instance = algo_class(signal)
            interceptors.append(instance)
        except ImportError:
            logger.error(f"找不到策略 {s}")
            raise

    channels = map(
        lambda ch: f"okex/{signal['server']}/{ch}", signal['channels'])
    listen = okws.Redis(
        list(channels),
        signal_function(interceptors, signal, client)
    )
    task = asyncio.create_task(listen.run())
    # 将当前任务加入，方便  signaler 取消任务
    signal['task'] = task
    logger.info(f"加入 {signal['name']} <{signal['channels']}>")
    return task


async def listen_and_do(client, config):
    # 订阅有关数据
    tasks = []
    for signal in config['signals']:
        try:
            task = create_signaler(signal, client)
            tasks.append(task)

        except Exception:
            logger.exception(f"初始化信号失败！")
            cancel_task(tasks)
            return []

    return tasks


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