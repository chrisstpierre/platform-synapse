# -*- coding: utf-8 -*-
import json

import tornado.web

from ..DB import DB
from ..Kubernetes import Kubernetes


class UnsubscribeHandler(tornado.web.RequestHandler):

    async def post(self):
        payload = json.loads(self.request.body)
        app_id = payload['app_id']
        pod_name = payload['pod_name']
        sub_id = payload['sub_id']
        await Kubernetes.remove_watch(app_id, pod_name, sub_id)
        await DB.delete_one_subscription(app_id, sub_id)
