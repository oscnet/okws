import logging

from ..interceptor import Interceptor
from ..okex import subscribe

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Subscribe(Interceptor):
    """
    记录 ws 连接所订阅频道，并在重联时重新订阅
    """

    def __init__(self):
        self.subscribed = set()

    async def enter(self, request):
        if request['_signal_'] == 'CONNECTED':
            if self.subscribed:
                logger.debug(f"CONNECTED, subscribe channels {self.subscribed}")
                subscribe(request, list(self.subscribed))
        elif request['_signal_'] == 'ON_DATA':
            if "event" in request['DATA'] and request['DATA'].get('event') == 'subscribe':
                # 订阅了频道
                self.subscribed.add(request['DATA'].get('channel'))
                logger.debug(f"subscribed channel {request['DATA'].get('channel')}")

            elif "event" in request['DATA'] and request['DATA'].get('event') == 'unsubscribe':
                self.subscribed.remove(request['DATA'].get('channel'))
                logger.debug(f"unsubscribe channel {request['DATA'].get('channel')}")
