# -*- coding: utf-8 -*-
import json

import psycopg2

from .Entities import Subscription
from .Config import Config


class DB:

    @classmethod
    def conn(cls):
        return psycopg2.connect(Config.POSTGRES)

    @classmethod
    async def insert_subscription(cls, s: Subscription):
        conn = cls.conn()
        cur = conn.cursor()
        try:
            # TODO: make this async/await
            cur.execute('insert into app_runtime.subscriptions '
                        '(uuid, app_uuid, container_id, url, '
                        'method, payload, pod_name)'
                        ' values '
                        '(%s, %s, %s, %s, %s, %s, %s)',
                        (s.uuid, s.app_uuid, s.container_id, s.url, s.method,
                         json.dumps(s.payload), s.pod_name))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @classmethod
    async def get_subscription(cls, sub_id: str) -> Subscription:
        conn = cls.conn()
        cur = conn.cursor()
        # TODO: make this async/await
        try:
            cur.execute('select '
                        'uuid, app_uuid, container_id, url, '
                        'method, payload, pod_name '
                        'from app_runtime.subscriptions '
                        'where uuid=%s', (sub_id,))
            r = cur.fetchone()
            assert r is not None, 'Subscription not found!'
            return Subscription(
                uuid=sub_id, app_uuid=r[1], container_id=r[2],
                url=r[3], method=r[4], payload=r[5], pod_name=r[6])
        finally:
            cur.close()
            conn.close()

    @classmethod
    async def update_container(cls, sub_id, container_id):
        conn = cls.conn()
        cur = conn.cursor()
        try:
            # TODO: make this async/await
            cur.execute('update app_runtime.subscriptions '
                        'set container_id=%s '
                        'where uuid=%s',
                        (container_id, sub_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
