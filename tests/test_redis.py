from okws.redis import Client
import pytest
import asyncio
import logging
import aioredis

pytestmark = pytest.mark.asyncio
redis_url = 'redis://localhost'
CHANNEL = 'TRADECHANNEL'
logger = logging.getLogger(__name__)


async def send_exit_cmd(client):
    # 测试当 redis 收到 'exit' 时退出
    await asyncio.sleep(0.5)
    redis = await aioredis.create_redis(redis_url)
    await redis.publish(CHANNEL, 'Hello')
    await asyncio.sleep(0)
    await redis.publish(CHANNEL, 'exit')
    redis.close()
    await redis.wait_closed()
    # clientX.stop()


async def on_cmd(request):
    if request['_signal_'] == 'ON_DATA':
        msg = request['_data_']
        logger.info(f"Got message:{msg}")
        assert msg == 'exit' or msg == 'Hello'
        if msg == 'exit':
            logger.info("close clientX")
            request['_server_'].close()
            # or  return -1
    return 0


async def test_cmd():
    try:
        client = Client(CHANNEL, on_cmd)
        await asyncio.gather(
            client.run(),
            send_exit_cmd(client))
    except asyncio.CancelledError:
        logger.info("catch CancelledError!!")
