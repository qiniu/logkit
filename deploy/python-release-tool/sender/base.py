#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
from abc import ABCMeta, abstractmethod


class Sender(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    def send(self, file_path):
        pass


def create_sender(conf):
    senders = dict()
    if not conf:
        return senders
    try:
        for c in conf.items('sender'):
            if not conf.getboolean('sender', c[0]):
                continue
            if c[0] == 'kodo':
                from sender.kodo import Kodo
                senders['kodo'] = Kodo(conf)
            elif c[0] == 'dir':
                from sender.dir import Dir
                senders['dir'] = Dir(conf)
            else:
                print 'Warn: unknown sender type', c[0]
    except ConfigParser.NoSectionError:
        pass
    return senders
