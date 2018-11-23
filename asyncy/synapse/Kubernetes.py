# -*- coding: utf-8 -*-
import asyncio

from kubernetes_asyncio.client import Configuration
from typing import MutableMapping

from kubernetes_asyncio import client, config, watch

from .Config import Config
from .Logger import Logger
from .Subscriptions import Subscriptions

logger = Logger.get('Kubernetes')


class Kubernetes:
    v1: client.CoreV1Api = None
    loop = asyncio.new_event_loop()

    sub_lock = asyncio.Lock()

    subscriptions: MutableMapping[str, MutableMapping[str, asyncio.Task]] = {}
    """
    Map: <app_id, <subscription_id: task>>
    Holds a reference for every Kubernetes#_watch() task created, keyed
    by the app_id, followed by the subscription ID.

    This helps in removing an active watch from an individual subscription.
    """

    @classmethod
    async def init(cls):
        logger.info('Init!')
        if Config.CLUSTER_CERT != '':
            configuration = Configuration()
            configuration.host = f'https://{Config.CLUSTER_HOST}'
            configuration.ssl_ca_cert = f'{Config.CLUSTER_CERT}'
            configuration.api_key['authorization'] = \
                f'bearer {Config.CLUSTER_AUTH_TOKEN}'
            Configuration.set_default(configuration)
        else:
            await config.load_kube_config()
        cls.v1 = client.CoreV1Api()

    @classmethod
    async def get_container_id(cls, namespace: str, pod_name: str) -> str:
        """
        Asyncy provisions Pods via a Deployment, with a single container
        in each Pod. This method returns the first container ID associated
        with this Pod.
        """
        r = await cls.v1.list_namespaced_pod(namespace,
                                             label_selector=f'app={pod_name}')
        assert len(r.items) == 1
        pod = r.items[0]
        return pod.status.container_statuses[0].container_id

    @classmethod
    async def ensure_one_pod(cls, namespace, label):
        r = await cls.v1.list_namespaced_pod(namespace,
                                             label_selector=f'app={label}')
        assert len(r.items) == 1, \
            f'{len(r.items)} pods found for label app={label}'

    @classmethod
    async def _watch(cls, pod_name, namespace, sub_id):
        logger.debug(f'Watching for Pod modified events '
                     f'on {pod_name} for {sub_id}')

        await cls.ensure_one_pod(namespace, pod_name)

        w = watch.Watch()
        try:
            async for event in w.stream(cls.v1.list_namespaced_pod, namespace,
                                        label_selector=f'app={pod_name}'):
                et = event['type'].lower()

                if et != 'modified':
                    continue

                # Pod might have been modified.
                s = event['object'].status.container_statuses
                if s and s[0].state.running:
                    logger.info(f'Potentially re-subscribing {sub_id}...')
                    await Subscriptions.resubscribe(sub_id, s[0].container_id)
        except asyncio.CancelledError:
            w.stop()
        finally:
            logger.debug(f'Stopped watching for Pod changes '
                         f'on {pod_name} for {sub_id} ')

    @classmethod
    async def create_watch(cls, pod_name, namespace, sub_id):
        loop = asyncio.get_event_loop()
        task = loop.create_task(cls._watch(pod_name, namespace, sub_id))

        async with cls.sub_lock:
            app_subs = cls.subscriptions.setdefault(namespace, {})
            task_to_be_cancelled = app_subs.get(sub_id)
            app_subs[sub_id] = task

        # Ideally there will never be a pending watch task to be cancelled,
        # as subscription IDs are unique. This is just in case, as we do
        # not want to have unnecessary watches setup with Kubernetes.
        if task_to_be_cancelled is not None:
            task_to_be_cancelled.cancel()

    @classmethod
    async def remove_watches(cls, app_id: str):
        async with cls.sub_lock:
            subscriptions = cls.subscriptions.get(app_id)
            if subscriptions:
                del cls.subscriptions[app_id]

        if subscriptions:
            for sub_id, task in subscriptions.items():
                task.cancel()

    @classmethod
    async def remove_watch(cls, app_id: str, sub_id: str):
        async with cls.sub_lock:
            subscriptions = cls.subscriptions.get(app_id, {})
            task = subscriptions.get(sub_id)
            if task:
                del subscriptions[sub_id]

        if task:
            task.cancel()
