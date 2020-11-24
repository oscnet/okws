import asyncio
import pytest
from okws.ws.okex.client import Client
import logging

# pytest -o log_cli=true --log-cli-level=info  -s

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
pytestmark = pytest.mark.asyncio


async def app(request):
    logging.info(request)


async def test_client():
    logger.info("start test")
    client = Client(app)

    async def stop():
        await asyncio.sleep(0.5)
        client.close()

    # with pytest.raises(CancelledError):
    await asyncio.gather(
        client.run(),
        stop()
    )
