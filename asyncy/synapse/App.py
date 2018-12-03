# -*- coding: utf-8 -*-
import asyncio
import signal

import tornado.ioloop
import tornado.web

from .DB import DB
from .Entities import Subscription
from .Kubernetes import Kubernetes
from .Logger import Logger
from .Subscriptions import Subscriptions
from .handlers.ClearSubscriptions import ClearSubscriptions
from .handlers.SubscriptionHandler import SubscriptionHandler
from .handlers.UnsubscribeHandler import UnsubscribeHandler

logger = Logger.get('App')


def make_app():
    return tornado.web.Application([
        (r'/subscribe', SubscriptionHandler),
        (r'/unsubscribe', UnsubscribeHandler),
        (r'/clear_all', ClearSubscriptions)
    ])


async def init():
    async for s in DB.stream_all_subscriptions():
        assert isinstance(s, Subscription)
        try:
            container_id = await Kubernetes.get_container_id(
                s.app_uuid, s.pod_name)

            if container_id != s.container_id:
                await Subscriptions.resubscribe(s.uuid, container_id)
        except BaseException as e:
            logger.error(f'Failed to maintain subscription for '
                         f'{s.uuid}@{s.pod_name}', exc_info=e)
        finally:
            await Kubernetes.create_watch(s.pod_name, s.app_uuid, s.uuid)


def sig_handler(*args, **kwargs):
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)
    pass


async def shutdown():
    io_loop = tornado.ioloop.IOLoop.instance()
    io_loop.stop()
    loop = asyncio.get_event_loop()
    loop.stop()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(Kubernetes.init())
    loop.create_task(init())

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    app = make_app()
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
