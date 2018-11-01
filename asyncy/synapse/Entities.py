# -*- coding: utf-8 -*-
import typing

Subscription = typing.NamedTuple('Subscription', [
    ('uuid', str),
    ('app_uuid', str),
    ('container_id', str),
    ('url', str),
    ('method', str),
    ('payload', dict),
    ('pod_name', str)
])
