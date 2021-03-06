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
import asyncio
import json
import logging

import aioredis

import okws

logger = logging.getLogger(__name__)


class RedisCommand:
    # 处理通过 redis 发过来的用户命令

    def __init__(self, redis_url='redis://localhost', redis_info_key='trade-ws/info'):
        self.ws_clients = {}
        self.tasks = {}
        self.redis = None
        self.redis_url = redis_url
        self.redis_info_key = redis_info_key

    async def redis_msg(self, cid, event, msg, errorcode):
        # event: info, warn, error
        msg = {"event": event, "message": msg, "errorCode": errorcode}
        msg = json.dumps(msg)
        logger.debug(msg)
        await self.redis.set(f"{self.redis_info_key}/{cid}", msg)

    async def ws_send(self, ctx):
        cmd = json.loads(ctx['_data_'])
        name = cmd['name']
        cid = cmd['id']
        if name in self.ws_clients:
            del cmd['name']
            del cmd['id']
            await self.ws_clients[name].send(json.dumps(cmd))
            await self.redis_msg(cid, 'info', '已发送到 websocket 服务器', 80000)
        else:
            await self.redis_msg(cid, 'error',
                                 f"没有对应的 {cmd['name']} websocket 连接！", 80011)

    async def close_ws(self, ctx):
        cmd = json.loads(ctx['_data_'])
        if cmd['name'] in self.ws_clients:
            self.ws_clients[cmd['name']].close()
            del self.ws_clients[cmd['name']]
            await self.redis_msg(cmd['id'], 'info', '', 80000)
        else:
            msg = f"没有对应的 {cmd['name']} websocket 连接！"
            logging.warning(msg)
            await self.redis_msg(cmd['id'], 'error', msg, 80011)

    async def open_ws(self, ctx):
        cmd = json.loads(ctx['_data_'])
        if cmd['name'] in self.ws_clients:
            msg = f"{cmd['name']} 已存在!"
            await self.redis_msg(cmd['id'], 'error', msg, 80001)
            logger.warning(msg)
            return
        if 'name' in cmd:
            args = cmd.get('args', {})
            client = okws.Websockets(okws.app(cmd['name'], args, self.redis_url))
            self.ws_clients[cmd['name']] = client
            task = asyncio.create_task(client.run())
            self.tasks[cmd['name']] = task
            await self.redis_msg(cmd['id'], 'info', '', 80000)
        else:
            msg = f"指令错误:{cmd}"
            await self.redis_msg(cmd['id'], 'error', msg, 80010)
            logger.warning(msg)

    async def execute(self, ctx):
        if ctx['_signal_'] == 'CONNECTED':
            logger.info('okws 服务已启动！')

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
                    await self.redis_msg(cmd['id'], 'info', '', 80000)
                elif cmd['op'] == 'servers':
                    await self.redis_msg(cmd['id'], 'info', list(self.ws_clients.keys()), 80000)
                else:
                    # 发送到对应的 ws client
                    await self.ws_send(ctx)

            except Exception:
                msg = f"指令错误：{ctx['_data_']}"
                logging.exception(msg)
                await self.redis_msg(cmd['id'], 'error', msg, 80010)

    async def __call__(self, *args, **kwargs):
        if self.redis is None:
            self.redis = await aioredis.create_redis_pool(self.redis_url)
        ctx = args[0]
        await self.execute(ctx)
        # 以下代码已经没有用了
        # if ctx['_signal_'] != 'ON_DATA':
        #     # 向 `OKWS_INFO` 频道发送信号，当 `okws` 重启时，客户端就可以在这个频道收到 `CONNECTED` 信号时重新连接 websocket 及订阅。
        #     try:
        #         await self.redis.publish(OKWS_INFO, ctx['_signal_'])
        #     except (RuntimeError, aioredis.errors.PoolClosedError, aioredis.errors.ConnectionForcedCloseError):
        #         # 当任务取消时， aioredis 连接池可能会关闭一下，或会发出 RuntimeError: this is unexpected。
        #         pass

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
        logger.info("okws 退出！")


async def run(redis_url='redis://localhost', listen_channel='trade-ws', redis_info_key='trade-ws/info'):
    redis = okws.Redis(listen_channel, RedisCommand(redis_url, redis_info_key))
    await redis.run()
