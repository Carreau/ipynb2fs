#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
import errno
import io
import re
import json

cre = re.compile('/cell_(\d+)')

from fuse import FUSE, FuseOSError, Operations

from IPython.nbformat import current as v3


class Passthrough(Operations):
    def __init__(self, root, ipynb_name):
        self.root = root
        with io.open(ipynb_name, 'rb') as f:
            self.ipynb = v3.read(f, 'json')
        self.ipynb_name = ipynb_name
        self.ipynb_full = '/Users/bussonniermatthias/ipynb2fs/'+self.ipynb_name
        self.buffers = dict({})

    # Helpers
    # =======


    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        print("access path:",path," mode:", mode)
        full_path = self._full_path(path)
        if path.startswith(u'/cell'):
            print 'fake access'
            full_path = '/Users/bussonniermatthias/ipynb2fs/'+self.ipynb_name
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        print("getattr path:",path, fh)
        full_path = self._full_path(path)
        if path.startswith(u'/cell'):
            print 'fake getattr', full_path
            full_path = '/Users/bussonniermatthias/ipynb2fs/'+self.ipynb_name
            print 'to getattr', full_path

        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

    def readdir(self, path, fh):
        print("readdir path:",path," fh:", fh)
        full_path = self._full_path(path)
        
        if path == u'/' :
            print 'roooooot'
            for i,v in enumerate(self.ipynb.worksheets[0].cells):
                yield 'cell_{:03d}'.format(i)
        else:
            dirents = ['.', '..']
            if os.path.isdir(full_path):
                dirents.extend(os.listdir(full_path))
            for r in dirents:
                yield r

    def readlink(self, path):
        print("readlink path:",path)
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        print('statfs path:',path)
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, target, name):
        return os.symlink(self._full_path(target), self._full_path(name))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):

        print("open path:",path, flags)
        m = cre.match(path)
        if m is not None:
            #print '---'+cre.groups()[0]+'---' 
            crg = m.groups()[0] 
            num = int(crg)
            cell = self.ipynb.worksheets[0].cells[num]
            buff = io.StringIO()
            u = json.dumps(cell, indent=2).decode('ascii')
            buff.write(u)
            buff.seek(0)
            l = len(self.buffers)
            self.buffers[l] = buff
            return l

        full_path = self.ipynb_full
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        print("read path:",path, length, offset, fh)
        raise NotImplementedError('read not implemented')
        #os.lseek(fh, offset, os.SEEK_SET)
        #return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        raise NotImplementedError('write not implemented')
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        raise NotImplementedError('truncate not implemented')
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        raise NotImplementedError('release not implemented')
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        raise NotImplementedError('fsync not implemented')
        return self.flush(path, fh)


def main(mountpoint, root, ipynb):
    FUSE(Passthrough(root, ipynb), mountpoint, foreground=True)

if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1], sys.argv[3])
