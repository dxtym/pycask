import os
import time
import struct
from .keydir import KeyDir, KeyEntry

TOMBSTONE = 0  # value size of 0 indicates a deleted entry
HEADER_SIZE = 12  # 4 bytes for timestamp, key size, value size
HEADER_FORMAT = "<LLL"  # little endian order with 3 unsigned long
THRESHOLD = 1024 * 1024 * 10  # 10MB file size threshold for rotation


class PyCask:
    _instance = None

    def __new__(cls, path):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, path):
        self.keydir = KeyDir()

        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self._load_keydir()
        self._active_file = self._get_active_file()

    def _decode_header(self, header_bytes):
        return struct.unpack(HEADER_FORMAT, header_bytes)

    def _encode_header(self, timestamp, key_size, value_size):
        return struct.pack(HEADER_FORMAT, timestamp, key_size, value_size)

    def _filename_to_id(self, filename):
        return int(filename.split('.')[0])

    def _id_to_filename(self, file_id):
        return "{:06d}.data".format(file_id)

    def _get_files(self):
        return sorted([f for f in os.listdir(self.path) if f.endswith(".data")])

    def _load_keydir(self):
        files = self._get_files()
        for file in files:
            file_id = self._filename_to_id(file)
            file_path = os.path.join(self.path, file)

            with open(file_path, "rb") as f:
                while header_bytes := f.read(HEADER_SIZE):
                    timestamp, key_size, value_size = self._decode_header(header_bytes)
                    if value_size == 0:
                        f.seek(key_size, os.SEEK_CUR)
                        continue

                    key = f.read(key_size).decode("utf-8")
                    value_pos = f.tell()

                    self.keydir[key] = KeyEntry(
                        file_id=file_id,
                        value_size=value_size,
                        value_pos=value_pos,
                        timestamp=timestamp
                    )

                    f.seek(value_size, os.SEEK_CUR)

    def _create_file(self, file_id=0):
        file_path = os.path.join(self.path, self._id_to_filename(file_id))
        return open(file_path, "ab+")

    def _get_active_file(self):
        files = self._get_files()
        if not files:
            return self._create_file()

        latest_file = files[-1]
        latest_file_path = os.path.join(self.path, latest_file)
        latest_file_id = self._filename_to_id(latest_file)
        latest_file_size = self._get_expected_file_size()

        if latest_file_size >= THRESHOLD:
            return self._create_file(latest_file_id + 1)

        return open(latest_file_path, "ab+")

    def _get_expected_file_size(self, key_size=0, value_size=0):
        file_size = os.path.getsize(self._active_file.name)
        entry_size = HEADER_SIZE + key_size + value_size
        return file_size + entry_size

    def put(self, key, value):
        now = int(time.time())
        key_bytes, value_bytes = key.encode("utf-8"), value.encode("utf-8")
        key_size, value_size = len(key_bytes), len(value_bytes)
        header = self._encode_header(now, key_size, value_size)

        filename = os.path.basename(self._active_file.name)
        file_id = self._filename_to_id(filename)
        file_size = self._get_expected_file_size(key_size, value_size)

        if file_size >= THRESHOLD:
            self._active_file.close()
            self._active_file = self._create_file(file_id + 1)

        self._active_file.write(header)
        self._active_file.write(key_bytes)
        value_pos = self._active_file.tell()
        self._active_file.write(value_bytes)
        self._active_file.flush()

        self.keydir[key] = KeyEntry(
                file_id=file_id,
                value_size=value_size,
                value_pos=value_pos,
                timestamp=now
            )

    def get(self, key):
        key_entry = self.keydir.get(key, None)
        if key_entry is None:
            raise KeyError(f"Key '{key}' not found.")

        if key_entry.value_size == 0:
            raise KeyError(f"Key '{key}' has been deleted.")

        filename = self._id_to_filename(key_entry.file_id)
        file_path = os.path.join(self.path, filename)

        with open(file_path, "rb") as f:
            f.seek(key_entry.value_pos)
            value = f.read(key_entry.value_size).decode("utf-8")

        return value

    def delete(self, key):
        value = self.keydir.pop(key, None)
        if value is None:
            raise KeyError(f"Key '{key}' not found.")

        now = int(time.time())
        key_size = len(key.encode("utf-8"))
        value_size = TOMBSTONE
        header = self._encode_header(now, key_size, value_size)

        key_bytes = key.encode("utf-8")
        filename = os.path.basename(self._active_file.name)
        file_id = self._filename_to_id(filename)
        file_size = os.path.getsize(self._active_file.name)
        entry_size = HEADER_SIZE + key_size + value_size

        if file_size + entry_size >= THRESHOLD:
            self._active_file.close()
            self._active_file = self._create_file(file_id + 1)

        self._active_file.write(header)
        self._active_file.write(key_bytes)
        self._active_file.flush()
