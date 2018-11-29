# -*- coding: utf-8 -*-
import json
from typing import AsyncIterator

import psycopg2

from .Config import Config
from .Entities import Subscription


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
                        '(uuid, app_uuid, k8s_container_id, url, '
                        'method, payload, k8s_pod_name)'
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
                        'uuid, app_uuid, k8s_container_id, url, '
                        'method, payload, k8s_pod_name '
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
        # TODO: add optimistic concurrency here to ensure
        # TODO: that two tasks don't compete.
        try:
            # TODO: make this async/await
            cur.execute('update app_runtime.subscriptions '
                        'set k8s_container_id=%s '
                        'where uuid=%s',
                        (container_id, sub_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @classmethod
    async def stream_all_subscriptions(cls) -> AsyncIterator[Subscription]:
        conn = cls.conn()
        cur = conn.cursor()
        try:
            # TODO: make this async/await
            cur.execute('select '
                        'uuid, app_uuid, k8s_container_id, url, '
                        'method, payload, k8s_pod_name '
                        'from app_runtime.subscriptions')

            while True:
                subscriptions = cur.fetchmany(size=100)

                if len(subscriptions) == 0:
                    break

                for s in subscriptions:
                    yield Subscription(
                        uuid=s[0], app_uuid=s[1], container_id=s[2],
                        url=s[3], method=s[4], payload=s[5], pod_name=s[6])

        finally:
            cur.close()
            conn.close()

    @classmethod
    async def delete_all_subscriptions(cls, app_id: str):
        conn = cls.conn()
        cur = conn.cursor()
        try:
            # TODO: make this async/await
            cur.execute('delete from app_runtime.subscriptions '
                        'where app_uuid=%s',
                        (app_id,))
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @classmethod
    async def delete_one_subscription(cls, app_id: str, sub_id: str):
        conn = cls.conn()
        cur = conn.cursor()
        try:
            # TODO: make this async/await
            cur.execute('delete from app_runtime.subscriptions '
                        'where app_uuid=%s and uuid=%s',
                        (app_id, sub_id))
            conn.commit()
        finally:
            cur.close()
            conn.close()
