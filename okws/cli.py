import asyncio
import getopt
import logging
import os
import os.path
import sys

from yaml import Loader, load

import okws

logger = logging.getLogger(__name__)


async def okws_exist(client):
    client.servers()
    await asyncio.sleep(1)
    info = client.get_return_info()
    return type(info['message']) == list


async def open_ws(client, config):
    # 连接到 ws 服务器
    for server in config['servers']:
        client.open_ws(server['name'], server)
    await asyncio.sleep(2)


async def subscribe(client, config, name=None):
    # 发送订阅命令
    for sub in config.get('subscribes', []):
        if name is None or sub['server'] == name:
            client.subscribe(sub['server'], sub['channels'])
            await asyncio.sleep(1)


def usage():
    print('okws -c <configfile>')
    exit(1)


def read_config(config_file):
    logger.info(f"config file: {config_file}")
    if os.path.isfile(config_file) and os.access(config_file, os.R_OK):
        with open(config_file, 'r') as f:
            return load(f, Loader=Loader)
    else:
        logger.error(f"{config_file} 文件不存在或不可读")
        exit(1)


def parse_argv(argv):
    try:
        opts, _args = getopt.getopt(argv[1:], "-h-c:", ['help'])
    except getopt.GetoptError:
        usage()

    for opt, arg in opts:
        if opt in ("-c"):
            return read_config(arg)
        else:
            usage()
    usage()


async def execute_config_task(config):
    redis_url = config['settings'].get('REDIS_URL', 'redis://localhost')
    await asyncio.sleep(1)
    client = okws.client(redis_url)
    if not await okws_exist(client):
        logger.warning(f"未检测到 okws 运行，程序退出。")
        return

    await open_ws(client, config)
    await asyncio.sleep(1)
    # 如果相应的 ws 已经连接的话，需要重新订阅一下
    await subscribe(client, config)


async def execute(config):
    logger.debug(config)
    redis_url = config['settings'].get('REDIS_URL', 'redis://localhost')
    listen_channel = config['settings'].get('LISTEN_CHANNEL', 'trade-ws')
    redis = okws.Redis(listen_channel, okws.RedisCommand(redis_url))
    await asyncio.gather(
        redis.run(),
        execute_config_task(config)
    )


def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(module)s[%(lineno)d] - %(levelname)s: %(message)s')

    config = parse_argv(sys.argv)
    # logger.info(config)
    try:
        asyncio.run(execute(config))
    except KeyboardInterrupt:
        logging.info('Ctrl+C 完成退出')
