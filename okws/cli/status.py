# 当 websocket 断开重连时，重新订阅
import asyncio
import logging
import json
import okws

logger = logging.getLogger(__name__)


async def okws_exist(client):
    info = await client.servers()
    # logger.info(info)
    return type(info['message']) == list


async def open_ws(client, config):
    # 连接到 ws 服务器
    for server in config['servers']:
        await client.open_ws(server['name'], server)


async def subscribe(client, config, name=None):
    # 发送订阅命令
    for sub in config.get('subscribes', []):
        if name is None or sub['server'] == name:
            await client.subscribe(sub['server'], sub['channels'])

    for sub in config['signals']:
        # logger.info(sub)
        if name is None or sub['server'] == name:
            await client.subscribe(sub['server'], sub['channels'])


# 订阅 okws 服务信息，使得 okws connect 时开启 ws 服务
async def okws_connect(client, config):
    async def app(ctx):
        # logger.warning(f"{ctx}")
        if ctx.get('_signal_') == 'ON_DATA' and ctx.get('_data_') == 'CONNECTED':
            logger.warning(f"检测到 okws CONNECTED!")
            await asyncio.sleep(1)
            await open_ws(client, config)
            # await subscribe(client, config)

    listen = okws.Redis(okws.settings.OKWS_INFO, app)
    await listen.run()


def ws_status(client, config):
    async def _connect(ctx):
        # logger.info(f"re_connect:{ctx}")
        if ctx.get('_signal_') == 'ON_DATA':
            channel = ctx.get('_channel_')
            if channel is not None:
                _, ws_server, *_ = channel.split('/')
                # logger.info(f"{ws_server} server connected!")
                data = json.loads(ctx.get('_data_'))
                if data.get('op') == 'CONNECTED':
                    logger.warning(f"检测到 ws {ws_server} CONNECTED!")
                    await asyncio.sleep(1)
                    await subscribe(client, config, ws_server)

    return _connect


async def ws_status_listener(client, config):
    # 订阅 ws 服务器 connect 事件，以便 ws 服务器断线时重联
    servers = map(lambda x: x['name'], config['servers'])
    channels = list(map(lambda x: f"okex/{x}/event", servers))
    # logger.info(f"listen: {channels}")

    listen = okws.Redis(
        channels,
        ws_status(client, config)
    )
    await listen.run()