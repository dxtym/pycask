from .keydir import KeyDir


THRESHOLD = 1024 * 1024 * 10  # 10MB


class PyCask:
    _instances = {}

    def __new__(cls, path: str):
        if path not in cls._instances:
            cls._instances[path] = super().__new__(cls)
        return cls._instances[path]

    def __init__(self, path: str):
        self.path = path
        self.keydir = KeyDir()
        self.active_file = self._find_active_file()

    def _find_active_file(self):
        raise NotImplementedError

    def _merge(self):
        raise NotImplementedError

    def put(self, key: str, value: bytes):
        raise NotImplementedError

    def get(self, key: str):
        raise NotImplementedError

    def delete(self, key: str):
        raise NotImplementedError
