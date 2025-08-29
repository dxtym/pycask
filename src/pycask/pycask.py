import os
import struct
from .keydir import KeyDir, KeyEntry
from .lockfile import LockFile

HEADER_SIZE = 16
HEADER_FORMAT = "<LLL"
THRESHOLD = 1024 * 1024 * 10  # 10MB


class PyCask:
    _instances = {}

    def __new__(cls, path):
        if path not in cls._instances:
            cls._instances[path] = super().__new__(cls)
        return cls._instances[path]

    def __init__(self, path):
        self.keydir = KeyDir()

        self.cursor = 0
        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self._load_keydir()
        self._active_file = self._get_active_file()

    def _decode_header(self, header_bytes):
        return struct.unpack(HEADER_FORMAT, header_bytes)

    def _encode_header(self, timestamp, key_size, value_size):
        return struct.pack(HEADER_FORMAT, timestamp, key_size, value_size)

    def _convert_filename_to_id(self, filename):
        return int(filename.split('.')[0])

    def _convert_id_to_filename(self, file_id):
        return "{:06d}.data".format(file_id)

    def _get_files(self):
        return [f for f in os.listdir(self.path) if f.endswith(".data")]

    def _load_keydir(self):
        with LockFile(self.path):
            files = self._get_files()
            for file in files:
                file_id = self._convert_filename_to_id(file)
                file_path = os.path.join(self.path, file)
                with open(file_path, "rb") as f:
                    while chunk := f.read(HEADER_SIZE):
                        if len(chunk) < HEADER_SIZE:
                            break

                        timestamp, key_size, value_size = self._decode_header(chunk)
                        key = f.read(key_size).decode("utf-8")
                        value_pos = f.tell()

                        self.keydir[key] = KeyEntry(
                            file_id=file_id,
                            value_size=value_size,
                            value_pos=value_pos,
                            timestamp=timestamp
                        )

                        f.seek(value_size, os.SEEK_CUR)
                        self.cursor = f.tell()

    def _create_file(self, file_id=0):
        file_path = os.path.join(self.path, self._convert_id_to_filename(file_id))
        return open(file_path, "ab+")

    def _get_active_file(self):
        files = self._get_files()
        if not files:
            return self._create_file()

        files.sort()
        latest_file = files[-1]
        latest_file_id = self._convert_filename_to_id(latest_file)
        latest_file_path = os.path.join(self.path, latest_file)
        if os.path.getsize(latest_file_path) >= THRESHOLD:
            return self._create_file(latest_file_id + 1)

        return open(latest_file_path, "ab+")

    def put(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def delete(self, key):
        raise NotImplementedError
