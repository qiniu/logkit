#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import getopt
import ConfigParser

from tools import models

from pack.pack import Pack
from pack.compiled import Compiled
from sender.base import create_sender


def run(conf):
    cf = ConfigParser.ConfigParser()
    cf.read(conf)

    try:
        code_path = cf.get('code', 'path')
    except ConfigParser.NoSectionError:
        raise Exception('code path is empty')
    except ConfigParser.NoOptionError:
        raise Exception('code path is empty')

    arch_list = []
    arch_type = [models.I386, models.AMD64]
    for t in arch_type:
        try:
            at = cf.getboolean('code', t)
        except ConfigParser.NoSectionError:
            pass
        except ConfigParser.NoOptionError:
            pass
        else:
            if at:
                arch_list.append(t)

    version = raw_input('please input version: ')
    com = Compiled(code_path, cf, version)
    pack = Pack(code_path, cf, version)
    senders = create_sender(cf)
    for al in arch_list:
        com.main(al)
        pack_path = pack.main(al)
        for (k, s) in senders.items():
            try:
                s.send(pack_path)
            except AttributeError:
                print 'Warn:', k, 'has no attr send(file_path)'


def main(argv):
    try:
        opts, args = getopt.getopt(argv, 'hf:', ['configfile='])
    except getopt.GetoptError:
        print 'main.py -f <configfile>'
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print 'main.py -f <configfile>'
            sys.exit()
        elif opt in ('-f', '--configfile'):
            run(arg)
        else:
            print 'unknown args', opt
            sys.exit(2)


if __name__ == '__main__':
    main(sys.argv[1:])

