# -*- coding: utf-8 -*-
import json

import tornado.web

from ..Kubernetes import Kubernetes
from ..DB import DB


class ClearSubscriptions(tornado.web.RequestHandler):

    async def post(self):
        payload = json.loads(self.request.body)
        app_id = payload['app_id']
        await Kubernetes.remove_watches(app_id)
        await DB.delete_all_subscriptions(app_id)
