# -*- coding: utf-8 -*-


class NotFoundException(Exception):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
