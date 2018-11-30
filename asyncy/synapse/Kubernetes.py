# -*- coding: utf-8 -*-
import asyncio
import threading
from typing import List, MutableMapping

from kubernetes_asyncio import client, config, watch

from .Config import Config
from .Exceptions import NotFoundException
from .Logger import Logger
from .Subscriptions import Subscriptions

logger = Logger.get('Kubernetes')


class Kubernetes:
    v1: client.CoreV1Api = None
    loop = asyncio.new_event_loop()

    sub_lock = asyncio.Lock()

    subscriptions: MutableMapping[str, MutableMapping[str, List[str]]] = {}
    """
    Map: <namespace, <pod_name: [subscription_ids]>>
    """

    @classmethod
    async def init(cls):
        logger.info('Init!')
        if Config.IN_CLUSTER:
            config.load_incluster_config()
        else:
            await config.load_kube_config()
        cls.v1 = client.CoreV1Api()
        t = threading.Thread(target=cls.init_watch_all_pods)
        t.setDaemon(True)
        t.start()

    @staticmethod
    def resolve_namespace(app_id: str, pod_name: str) -> str:
        if pod_name == 'gateway':
            return 'asyncy-system'

        return app_id

    @classmethod
    async def get_container_id(cls, app_id: str, pod_name: str) -> str:
        """
        Asyncy provisions Pods via a Deployment, with a single container
        in each Pod. This method returns the first container ID associated
        with this Pod.
        """
        namespace = cls.resolve_namespace(app_id, pod_name)
        r = await cls.v1.list_namespaced_pod(namespace,
                                             label_selector=f'app={pod_name}')
        assert len(r.items) == 1
        pod = r.items[0]
        return pod.status.container_statuses[0].container_id

    @classmethod
    def init_watch_all_pods(cls):
        """
        Must be called in a new thread.
        """
        asyncio.set_event_loop(asyncio.new_event_loop())
        loop = asyncio.get_event_loop()
        loop.run_until_complete(cls.watch_all_pods())

    @classmethod
    async def watch_all_pods(cls):
        while True:
            try:
                await cls._watch_pods()
            except BaseException as e:
                logger.error('Exception in watching all pods! Will restart!',
                             exc_info=e)

    @classmethod
    async def _watch_pods(cls):
        logger.info('Watching for all Pod changes...')
        v1 = client.CoreV1Api()
        w = watch.Watch()
        async for event in w.stream(v1.list_pod_for_all_namespaces):
            et = event['type'].lower()

            if et != 'modified':
                continue

            # Pod might have been modified.
            s = event['object'].status.container_statuses

            if s and s[0].state.running:
                pod_name = event['raw_object']['metadata']['labels'].get('app')

                if pod_name is None:
                    # Not one of the pods controlled by Asyncy apps.
                    continue

                namespace = event['raw_object']['metadata']['namespace']
                logger.info(f'Potentially re-subscribing {pod_name} '
                            f'for {namespace}...')

                pods = cls.subscriptions.get(namespace, {})
                sub_ids = pods.get(pod_name, [])
                sub_ids_copy = sub_ids.copy()  # Use this for iteration.

                for sub_id in sub_ids_copy:
                    try:
                        await Subscriptions.resubscribe(sub_id,
                                                        s[0].container_id)
                    except NotFoundException:  # Stale subscription.
                        async with cls.sub_lock:
                            sub_ids.remove(sub_id)
                    except BaseException as e:
                        logger.error(f'Exception in resubscribe for '
                                     f'pod_name={pod_name}; sub_id={sub_id}!',
                                     exc_info=e)

        logger.debug(f'Stopped watching for Pod changes! Will restart...')

    @classmethod
    async def create_watch(cls, pod_name, app_id, sub_id):
        namespace = cls.resolve_namespace(app_id, pod_name)

        async with cls.sub_lock:
            app_subs = cls.subscriptions.setdefault(namespace, {})
            sub_ids_for_pod_name = app_subs.setdefault(pod_name, [])
            sub_ids_for_pod_name.append(sub_id)

    @classmethod
    async def remove_watches(cls, app_id: str):
        # Warning: Do not resolve app_id to namespace here.
        # We CANNOT delete asyncy-system under any circumstances.
        async with cls.sub_lock:
            subscriptions = cls.subscriptions.get(app_id)
            if subscriptions:
                del cls.subscriptions[app_id]

    @classmethod
    async def remove_watch(cls, app_id: str, pod_name: str, sub_id: str):
        namespace = cls.resolve_namespace(app_id, sub_id)
        async with cls.sub_lock:
            subscriptions = cls.subscriptions.get(namespace, {})
            sub_ids = subscriptions.get(pod_name, [])
            sub_ids.remove(sub_id)
