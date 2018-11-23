# -*- coding: utf-8 -*-
import os


class Config:
    POSTGRES = os.getenv('POSTGRES', 'options='
                                     '--search_path=app_public,app_hidden'
                                     ',app_private,app_runtime,public'
                                     ' dbname=postgres user=postgres')
    IN_CLUSTER = os.getenv('IN_CLUSTER', 'false') == 'true'
