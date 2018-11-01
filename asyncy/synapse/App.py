# -*- coding: utf-8 -*-
import asyncio

import tornado.ioloop
import tornado.web

from .Kubernetes import Kubernetes
from .handlers.SubscriptionHandler import SubscriptionHandler


def make_app():
    return tornado.web.Application([
        (r'/subscribe', SubscriptionHandler),
    ])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(Kubernetes.init())
    # loop.run_forever()
    app = make_app()
    app.listen(8080)
    tornado.ioloop.IOLoop.current().start()
