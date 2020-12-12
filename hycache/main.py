import errno
import os
from os.path import join, islink, isdir, isfile
from queue import Queue

from dotdict import dotdict
from fuse import Operations, FuseOSError

from hycache.priority_queue import PriorityQueue


def path_relative(f):
    def wrapper(self, path, *args, **kwargs):
        if path.startswith("/"):
            path = path[1:]
        return f(self, path, *args, **kwargs)

    return wrapper


class PathInfo(object):
    def __init__(self, ssd, hdd):
        self.hdd = hdd
        self.ssd = ssd

    @property
    def link(self):
        return islink(self.ssd)

    @property
    def isdir(self):
        return isdir(self.ssd)


def path2info(f):
    def wrapper(self, path, *args, **kwargs):
        print(f"{f.__name__}: {path} {args}, {kwargs}")
        info = self.path_info(path)
        return f(self, info, *args, **kwargs)

    return wrapper


class Hycache(Operations):
    attributes = ('st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid')

    def __init__(self, ssd, hdd):
        self.ssd = ssd
        self.hdd = hdd
        self.queue = PriorityQueue(self.queue_path)
        self.operations_queue = Queue()

    @property
    def queue_path(self):
        return join(self.ssd, "____hycache", "queue.pickle")

    @path_relative
    def path_info(self, path) -> PathInfo:
        ssd = join(self.ssd, path)
        hdd = join(self.hdd, path)
        return PathInfo(ssd, hdd)

    # Filesystem methods
    # ==================

    @path2info
    def access(self, path, mode):
        res = os.access(path.ssd, mode)
        if not res:
            raise FuseOSError(errno.EACCES)
        return res

    @path2info
    def getattr(self, path, fh=None):
        st = os.lstat(path.ssd)
        res = dict((key, getattr(st, key)) for key in self.attributes)
        return res

    @path2info
    def readdir(self, path, fh):
        yield "."
        yield ".."
        if os.path.isdir(path.ssd):
            for r in os.listdir(path.ssd):
                if r[:4]=="____":
                    continue
                yield r

    @path_relative
    def mknod(self, path, mode, dev):
        return os.mknod(path, mode, dev)

    @path_relative
    def rmdir(self, path):
        info = self.path_info(path)
        res = os.rmdir(info.ssd)
        if info.link:
            self.queue_operation("rmdir", info.hdd)
        return res

    def queue_operation(self, operation, *args, **kwargs):
        self.operations_queue.put((operation, args, kwargs))

    @path2info
    def mkdir(self, info, mode):
        if not info.isdir:
            res = os.mkdir(info.ssd, mode)
            self.queue_operation("mkdir", info.hdd, mode)
        elif info.mode != mode:
            res = os.chmod(info.ssd, mode)
            self.queue_operation("chmod", info.hdd, mode)
        else:
            res = os.mkdir(info.ssd, mode)
        return res

    @path2info
    def statfs(self, path):
        stv = os.statvfs(path.ssd)
        res = dict((key, getattr(stv, key)) for key in (
        'f_bsize',
        'f_frsize',
        'f_blocks',
        'f_bfree',
        'f_bavail',
        'f_files',
        'f_ffree',
        'f_favail',
        'f_fsid',
        'f_flag',
        'f_namemax',
        ))
        return res


if __name__ == '__main__':
    Hycache(r"/home/xavier/PycharmProjects/pyHycache/ssd", r"/home/xavier/PycharmProjects/pyHycache/hdd")
