# -*- coding: utf-8 -*-
import asyncio
from kubernetes_asyncio import client, config, watch

from .Subscriptions import Subscriptions
from .Logger import Logger

logger = Logger.get('Kubernetes')


class Kubernetes:
    v1: client.CoreV1Api = None
    loop = asyncio.new_event_loop()

    @classmethod
    async def init(cls):
        # TODO: support in-cluster config
        logger.info('Init!')
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

    @classmethod
    async def create_watch(cls, pod_name, namespace, sub_id):
        loop = asyncio.get_event_loop()
        loop.create_task(cls._watch(pod_name, namespace, sub_id))
