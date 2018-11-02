# -*- coding: utf-8 -*-
import asyncio

import tornado.ioloop
import tornado.web

from .DB import DB
from .Entities import Subscription
from .Kubernetes import Kubernetes
from .Subscriptions import Subscriptions
from .handlers.ClearSubscriptions import ClearSubscriptions
from .handlers.SubscriptionHandler import SubscriptionHandler
from .handlers.UnsubscribeHandler import UnsubscribeHandler


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
        finally:
            await Kubernetes.create_watch(s.pod_name, s.app_uuid, s.uuid)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(Kubernetes.init())
    loop.create_task(init())
    app = make_app()
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
