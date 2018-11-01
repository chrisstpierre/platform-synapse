# -*- coding: utf-8 -*-
import asyncio

from tornado.httpclient import HTTPError


class HttpHelper:

    @staticmethod
    async def fetch_with_retry(tries, logger, url, http_client, kwargs):
        kwargs['raise_error'] = False
        attempts = 0
        while attempts < tries:
            attempts = attempts + 1
            try:
                return await http_client.fetch(url, **kwargs)
            except HTTPError as e:
                await asyncio.sleep(0.5)
                logger.log_raw(
                    'error',
                    f'Failed to call {url}; attempt={attempts}; err={str(e)}'
                )

        raise HTTPError(500, message=f'Failed to call {url}!')
