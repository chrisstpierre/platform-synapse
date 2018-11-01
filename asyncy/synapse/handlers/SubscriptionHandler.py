# -*- coding: utf-8 -*-
import json
import tornado.web

from ..Kubernetes import Kubernetes
from ..Subscriptions import Subscriptions


class SubscriptionHandler(tornado.web.RequestHandler):

    async def post(self):
        payload = json.loads(self.request.body)
        sub_id = payload['sub_id']
        sub_url = payload['sub_url']
        sub_method = payload['sub_method']
        sub_body = payload['sub_body']
        pod_name = payload['pod_name']
        app_id = payload['app_id']
        # Get the current container ID for this pod.
        c_id = await Kubernetes.get_container_id(app_id, pod_name)
        await Subscriptions.create(sub_id=sub_id, sub_url=sub_url,
                                   sub_body=sub_body, pod_name=pod_name,
                                   app_id=app_id, sub_method=sub_method,
                                   container_id=c_id)
        await Kubernetes.create_watch(pod_name, app_id, sub_id)
