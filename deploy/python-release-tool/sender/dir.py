#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import ConfigParser
from base import Sender


class Dir(Sender):
    def __init__(self, cf):
        Sender.__init__(self)
        try:
            self.dst_path = cf.get('sender_dir', 'dst_path')
        except ConfigParser.NoSectionError:
            raise Exception('dst_path is empty')
        except ConfigParser.NoOptionError:
            raise Exception('dst_path is empty')
        if os.path.exists(self.dst_path):
            if os.path.isfile(self.dst_path):
                raise Exception('path %s is an exists file path' % (self.dst_path, ))
        else:
            os.makedirs(self.dst_path)

    def send(self, file_path):
        if not os.path.exists(file_path):
            print 'file %s is not exists' % (file_path, )
        print 'begin to copy %s to %s' % (file_path, self.dst_path)
        if os.path.isfile(file_path):
            shutil.copy(file_path, self.dst_path)
        else:
            shutil.copytree(file_path, self.dst_path)
        print 'copy %s to %s finished' % (file_path, self.dst_path)
