#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys


def build_logger(name=None, level=None):
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s - %(process)d:%(filename)s:%(funcName)s:%(lineno)s'
    logging.basicConfig(stream=sys.stdout, format=FORMAT)
    logger = logging.getLogger(name or __name__)
    logger.setLevel(level or logging.DEBUG)
    return logger


class cached_property(object):
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    Python 3 has this builtin, but Django has helpfully backported this for us.
    Excellent write up here: http://ericplumb.com/blog/understanding-djangos-cached_property-decorator.html
    """
    def __init__(self, func):
        self.func = func

    def __get__(self, instance, type=None):
        if instance is None:
            return self
        res = instance.__dict__[self.func.__name__] = self.func(instance)
        return res
