#!/usr/bin/env python
# -*- coding: utf-8 -*-


import os
import shutil
import tarfile
import zipfile
import tempfile
import ConfigParser

from tools import models


class Pack(object):

    def __init__(self, src_dir, cf, version):
        self.src_dir = src_dir
        try:
            content = cf.get('pack', 'content').replace(' ', '').split(',')
        except ConfigParser.NoSectionError:
            content = ['logkit']
        except ConfigParser.NoOptionError:
            content = ['logkit']

        try:
            i386_name = cf.get('pack', 'i386_name')
        except ConfigParser.NoSectionError:
            i386_name = 'i386_%s.tar.gz'
        except ConfigParser.NoOptionError:
            i386_name = 'i386_%s.tar.gz'

        try:
            amd64_name = cf.get('pack', 'amd64_name')
        except ConfigParser.NoSectionError:
            amd64_name = 'amd64_%s.tar.gz'
        except ConfigParser.NoOptionError:
            amd64_name = 'amd64_%s.tar.gz'

        self.content = content
        try:
            self.i386_name = i386_name % (version, )
        except TypeError:
            self.i386_name = i386_name
        try:
            self.amd64_name = amd64_name % (version, )
        except TypeError:
            self.amd64_name = amd64_name
        self.tmp_dir = tempfile.mkdtemp()
        self.pack_dir = os.path.join(self.tmp_dir, '_package').strip()

    def __del__(self):
        shutil.rmtree(self.tmp_dir)

    def _make_dir(self):
        is_exists = os.path.exists(self.pack_dir)
        if is_exists:
            shutil.rmtree(self.pack_dir)
        os.makedirs(self.pack_dir)

    def _prepare(self):
        """ 准备需要打包的内容 """
        print 'make package dir %s and copy files' % (self.pack_dir, )
        self._make_dir()
        for c in self.content:
            src_file = os.path.join(self.src_dir, c)
            dst_file = os.path.join(self.pack_dir, c)
            if not os.path.exists(src_file):
                print src_file, 'is not exist, ignore it!'
                continue
            if os.path.isfile(src_file):
                shutil.copy(src_file, dst_file)
            else:
                shutil.copytree(src_file, dst_file)
        print 'copy files finished'

    def _pack_tar_gz(self, pack_name):
        with tarfile.open(pack_name, "w:gz") as tar:
            tar.add(self.pack_dir, arcname=os.path.basename(self.pack_dir))

    def _zippy(self, base_path, path, archive):
        paths = os.listdir(path)
        for p in paths:
            p = os.path.join(path, p)
            if os.path.isdir(p):
                self._zippy(base_path, p, archive)
            else:
                archive.write(p, os.path.relpath(p, base_path))

    def _pack_zip(self, pack_name):
        with zipfile.ZipFile(pack_name, 'w', zipfile.ZIP_DEFLATED) as z:
            self._zippy(self.tmp_dir, self.pack_dir, z)

    def _pack(self, pack_name):
        """ 打包 """
        print 'begin to pack %s' % (pack_name, )
        pack_name = os.path.join(self.tmp_dir, pack_name)
        if pack_name.endswith('tar.gz'):
            self._pack_tar_gz(pack_name)
        elif pack_name.endswith('zip'):
            self._pack_zip(pack_name)
        else:
            print 'unknown type %s' % (pack_name, )
        print 'pack %s finished' % (pack_name, )
        return pack_name

    def main(self, arch):
        self._prepare()
        if arch == models.I386:
            return self._pack(self.i386_name)
        elif arch == models.AMD64:
            return self._pack(self.amd64_name)
        else:
            print 'unknown arch type %s' % (arch, )
            return ''
