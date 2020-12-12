import pickle
from os import makedirs
from os.path import isfile, dirname, isdir
from pathlib import Path
from queue import PriorityQueue as VanillaPriorityQueue


class PriorityQueue(VanillaPriorityQueue):
    def __init__(self, filepath):
        self.filepath = filepath
        super(PriorityQueue, self).__init__()
        if isfile(filepath) and Path(filepath).stat().st_size>0:
            loaded = self.from_file(filepath)
            self.queue = loaded
        else:
            dir = dirname(filepath)
            if not isdir(dir):
                makedirs(dir)
            self.save()

    @classmethod
    def from_file(cls, path: str):
        with open(path, "rb") as fp:
            res = pickle.load(fp)
        return res

    def save(self):
        with open(self.filepath, "wb") as fp:
            pickle.dump(self.queue, fp)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save()
