import collections
import multiprocessing
import multiprocessing.connection
import multiprocessing.synchronize
import pickle
import threading
from multiprocessing import Pipe
from queue import Full
from typing import Callable

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

    def __init__(
        self, maxsize=0, encode=pickle.dumps, decode=pickle.loads, bufsize=1024
    ):
        if maxsize <= 0:
            maxsize = multiprocessing.synchronize.SEM_VALUE_MAX

        self._encode = encode
        self._decode = decode
        self._bufsize = bufsize

        self._sem = multiprocessing.BoundedSemaphore(maxsize)

        # Items are enqueued in this buffer before the feeder thread sends them over the pipe
        self._buffer = collections.deque()

        self._reader, self._writer = Pipe(duplex=False)

        # Notify _feeder that an item is in the buffer
        self._notempty = threading.Condition()

        # _feeder thread
        self._thread = None

        self._writelock = multiprocessing.Lock()
        self._readlock = multiprocessing.Lock()

    def put(self, obj, block=True, timeout=None):
        if not self._sem.acquire(block, timeout):
            raise Full

        # Put obj into buffer. It will be encoded in a separate thread.
        with self._notempty:
            if self._thread is None:
                self._start_thread()
            self._buffer.append(obj)
            self._notempty.notify()

    def get(self, block=True, timeout=None):
        # Read from pipe

        if block and timeout is None:
            with self._readlock:
                buf = []

                while True:
                    chunk = self._reader.recv_bytes()
                    if not chunk:
                        break
                    buf.append(chunk)

                buf = b"".join(buf)

            self._sem.release()

            return self._decode(buf)

        raise NotImplementedError()

    @staticmethod
    def _feeder(
        buffer: collections.deque,
        notempty: threading.Condition,
        writelock: multiprocessing.synchronize.Lock,
        writer: multiprocessing.connection.Connection,
        encode: Callable,
        bufsize: int,
    ):
        while True:
            with notempty:
                while not buffer:
                    notempty.wait()

                obj = buffer.popleft()

                # TODO: Check sentinel

                # Serialize
                buf = encode(obj)

                with writelock:
                    # Send data in bufsize chunks
                    bytes_left = len(buf)
                    while bytes_left > 0:
                        bufsize = min(bytes_left, bufsize)
                        writer.send_bytes(buf[-bytes_left:], 0, bufsize)
                        bytes_left -= bufsize
                    # Send empty value to signal end of buffer
                    writer.send_bytes(b"")

    def _start_thread(self):
        """Start thread which transfers data from buffer to pipe."""

        self._thread = threading.Thread(
            target=EnhancedQueue._feeder,
            args=(
                self._buffer,
                self._notempty,
                self._writelock,
                self._writer,
                self._encode,
                self._bufsize,
            ),
            name="EnhancedQueue._feeder",
        )
        self._thread.daemon = True

        self._thread.start()
