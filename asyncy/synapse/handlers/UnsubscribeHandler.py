# -*- coding: utf-8 -*-
import json

import tornado.web

from ..Kubernetes import Kubernetes


class UnsubscribeHandler(tornado.web.RequestHandler):

    async def post(self):
        payload = json.loads(self.request.body)
        app_id = payload['app_id']
        sub_id = payload['sub_id']
        await Kubernetes.remove_watch(app_id, sub_id)
