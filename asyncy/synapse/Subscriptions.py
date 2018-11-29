# -*- coding: utf-8 -*-
import json
from tornado.httpclient import AsyncHTTPClient

from .Entities import Subscription
from .helpers.HttpHelper import HttpHelper
from .DB import DB

from .Logger import Logger

logger = Logger.get('Subscriptions')


class Subscriptions:

    @classmethod
    async def create(cls, sub_id: str, sub_url: str, sub_body: dict,
                     sub_method: str, pod_name: str, app_id: str,
                     container_id: str):
        s = Subscription(uuid=sub_id, url=sub_url,
                         method=sub_method,
                         payload=sub_body, pod_name=pod_name,
                         app_uuid=app_id, container_id=container_id)

        await DB.insert_subscription(s)

        await cls._subscribe(sub_url, sub_method, sub_body)

    @classmethod
    async def _subscribe(cls, url: str, method: str, payload: dict):
        client = AsyncHTTPClient()
        kwargs = {
            'method': method.upper(),
            'headers': {
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps(payload)
        }
        res = await HttpHelper.fetch_with_retry(30, logger, url,
                                                client, kwargs)
        if int(res.code / 100) != 2:
            raise Exception(f'Failed to subscribe to service! res={res}')

    @classmethod
    async def resubscribe(cls, sub_id, container_id):
        """
        Check if the container ID has actually changed against the DB,
        and if it has, resubscribe with retry.
        """
        s = await DB.get_subscription(sub_id)
        if s.container_id != container_id:
            logger.debug(f'Re-subscribing {sub_id}...')
            await cls._subscribe(s.url, s.method.upper(), s.payload)
            await DB.update_container(sub_id, container_id)
