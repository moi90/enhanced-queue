import multiprocessing

multiprocessing.Queue
import pickle

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


class EnhancedQueue:
    """
    Enhanced version of multiprocessing.Queue.

    Differences to multiprocessing.Queue
    ------------------------------------

    - Arbitrarily large objects can be put onto the queue.
      The default implementation is limited by the buffer size of the underlying pipe:
      https://stackoverflow.com/a/45202335/1116842


    """

    def __init__(self, maxsize=0, encode=pickle.dumps, decode=pickle.loads):
        self._maxsize = maxsize
        self._encode = encode
        self._decode = decode

    def get(self, block=True, timeout=None):
        # Read from pipe
        value = ...

        return self._decode(value)

    def put(self, obj, block=True, timeout=None):
        # Put obj into buffer
        ...
