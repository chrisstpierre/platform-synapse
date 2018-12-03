# -*- coding: utf-8 -*-
import asyncio

from tornado.httpclient import HTTPError

from ..Logger import Logger

logger = Logger.get('HttpHelper')


class HttpHelper:

    @staticmethod
    async def fetch_with_retry(tries, url, http_client, kwargs):
        kwargs['raise_error'] = False
        attempts = 0
        while attempts < tries:
            attempts = attempts + 1
            try:
                res = await http_client.fetch(url, **kwargs)
                if int(res.code / 100) == 5:
                    raise HTTPError(res.code, message='Response code is 5xx',
                                    response=res)
                return res
            except HTTPError as e:
                await asyncio.sleep(0.5)
                logger.error(
                    f'Failed to call {url}; attempt={attempts}; err={str(e)}',
                    exc_info=e
                )

        raise HTTPError(500, message=f'Failed to call {url}!')
