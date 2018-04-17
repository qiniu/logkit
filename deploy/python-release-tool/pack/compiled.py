#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import platform
import ConfigParser

from tools import models


class Compiled(object):

    def __init__(self, code_path, cf, version):
        os.chdir(code_path)
        self.system = platform.system().lower()
        try:
            remote = cf.get('code', 'remote')
            self._prepare(remote, version)
        except ConfigParser.NoSectionError:
            pass
        except ConfigParser.NoOptionError:
            pass

    def _prepare(self, remote, version):
        """ 代码准备, 切换到指定的 release 之类的 """
        pass

    def _exec_shell(self, arch):
        print 'compile %s %s' % (self.system, arch)
        if self.system == 'windows':
            os.environ['GOARCH'] = arch
            os.popen('go build -o logkit.exe logkit.go')
        else:
            os.popen('GOARCH=%s go build -o logkit logkit.go' % (arch, ))

    def main(self, arch):
        if arch == models.I386:
            self._exec_shell('386')
        elif arch == models.AMD64:
            self._exec_shell('amd64')
        else:
            print 'unknown arch type %s' % (arch, )

