# -*- coding: utf-8 -*-
import os


class Config:
    POSTGRES = os.getenv('POSTGRES', 'options='
                                     '--search_path=app_public,app_hidden'
                                     ',app_private,app_runtime,public'
                                     ' dbname=postgres user=postgres')
    CLUSTER_CERT = os.getenv('CLUSTER_CERT', '')
    CLUSTER_AUTH_TOKEN = os.getenv('CLUSTER_AUTH_TOKEN', '')
    CLUSTER_HOST = os.getenv('CLUSTER_HOST', 'kubernetes.default.svc')
