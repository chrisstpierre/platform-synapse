# -*- coding: utf-8 -*-
import logging


class Logger:

    @staticmethod
    def get(name) -> logging.Logger:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        return logger
