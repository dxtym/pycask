import os
from filelock import FileLock

class LockFile:
    def __init__(self, path):
        self.path = path
        self._lock_file = os.path.join(self.path, ".lock")
        self._lock = FileLock(self._lock_file)

    def __enter__(self):
        with self._lock:
            return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._lock.release()
        return False
