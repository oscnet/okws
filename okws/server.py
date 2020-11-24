"""
 # 从 redis 取得用户指令并执行

 # 指令格式

 1. 订阅指令，同交易所一样
    如: {"op": "subscribe", "args": ["<SubscriptionTopic>"]}
    如果订阅的不是公共频道，要指定 名称 如 {"op": "subscribe", "args": ["<SubscriptionTopic>"],"name":"myname"}

 2. 连接 ws 服务器
    {"op": "open", "name":"myname", "args": {
        "apiKey": "", "secret": "", "password": ""}}

 3. 关闭与 ws 服务器的连接
    {"op": "close", "name":"myname"}

"""
from .redis import Client
import asyncio
import json
import logging
import aioredis
from okws.ws.okex.app import App
from okws.ws.okex.client import Client as okex

LISTEN_CHANNEL = 'trade-ws'
REDIS_INFO_KEY = 'trade-ws/info'

logger = logging.getLogger(__name__)


async def redis_msg(redis, event, msg, errorcode):
    # event: info, warn, error
    msg = {"event": event, "message": msg, "errorCode": errorcode}
    msg = json.dumps(msg)
    logger.debug(msg)
    await redis.set(REDIS_INFO_KEY, msg)


class RedisCommand:
    # 处理通过 redis 发过来的用户命令
    URL = 'redis://localhost'

    def __init__(self):
        self.ws_clients = {}
        self.tasks = {}
        self.redis = None

    async def ws_send(self, ctx):
        cmd = json.loads(ctx['_data_'])
        name = cmd['name']
        if name in self.ws_clients:
            del cmd['name']
            await self.ws_clients[name].send(json.dumps(cmd))
            await redis_msg(self.redis, 'info', '已发送到 websocket 服务器', 80000)
        else:
            await redis_msg(self.redis, 'error',
                            f"没有对应的 {cmd['name']} websocket 连接！", 80011)

    async def close_ws(self, ctx):
        cmd = json.loads(ctx['_data_'])
        if cmd['name'] in self.ws_clients:
            self.ws_clients[cmd['name']].close()
            del self.ws_clients[cmd['name']]
            await redis_msg(self.redis, 'info', '', 80000)
        else:
            msg = f"没有对应的 {cmd['name']} websocket 连接！"
            logging.warning(msg)
            await redis_msg(self.redis, 'error', msg, 80011)

    async def open_ws(self, ctx):
        cmd = json.loads(ctx['_data_'])
        if cmd['name'] in self.ws_clients:
            msg = f"{cmd['name']} 已存在!"
            await redis_msg(self.redis, 'error', msg, 80001)
            logger.warning(msg)
            return
        if 'name' in cmd:
            args = cmd.get('args', {})
            client = okex(App(cmd['name'], args))
            self.ws_clients[cmd['name']] = client
            task = asyncio.create_task(client.run())
            self.tasks[cmd['name']] = task
            await redis_msg(self.redis, 'info', '', 80000)
        else:
            msg = f"指令错误:{cmd}"
            await redis_msg(self.redis, 'error', msg, 80010)
            logger.warning(msg)

    async def execute(self, ctx):
        if ctx['_signal_'] == 'CONNECTED':
            self.redis = await aioredis.create_redis_pool(self.URL)
            logger.info('服务已启动！')

        elif ctx['_signal_'] == 'ON_DATA':
            # logging.info(ctx)
            try:
                self.check_tasks()
                cmd = json.loads(ctx['_data_'])
                # 不显示敏感数据
                msg = cmd.copy()
                if 'args' in msg:
                    del msg['args']
                logger.info(f"收到命令：{msg}")
                if cmd['op'] == 'open':
                    await self.open_ws(ctx)
                elif cmd['op'] == 'close':
                    await self.close_ws(ctx)
                elif cmd['op'] == 'quit_server':
                    ctx['_server_'].close()
                    await redis_msg(self.redis, 'info', '', 80000)
                elif cmd['op'] == 'servers':
                    await redis_msg(self.redis, 'info', list(self.ws_clients.keys()), 80000)
                else:
                    # 发送到对应的 ws client
                    await self.ws_send(ctx)

            except Exception:
                msg = f"指令错误：{ctx['_data_']}"
                logging.exception(msg)
                await redis_msg(self.redis, 'error', msg, 80010)

    async def __call__(self, *args, **kwargs):
        await self.execute(args[0])

    def check_tasks(self):
        remove_names = []
        for name, task in self.tasks.items():
            if task.cancelled() or task.done():
                remove_names.append(name)

        for name in remove_names:
            del self.tasks[name]
            if name in self.ws_clients:
                del self.ws_clients[name]

    def __del__(self):
        for client in self.ws_clients.values():
            client.close()

        if self.redis is not None:
            self.redis.close()
        logger.info("server 退出！")


async def run():
    redis = Client(LISTEN_CHANNEL, RedisCommand())
    await redis.run()


if __name__ == '__main__':
    from .config import main

    main()
