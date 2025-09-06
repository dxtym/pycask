import os
import time
import struct
import pickle
import threading
from typing import Any, TextIO
from .keydir import KeyDir, KeyEntry
from .const import (
    DEFAULT_PATH,
    HEADER_FORMAT,
    HEADER_SIZE,
    THRESHOLD,
    LIMIT,
    TOMBSTONE
)



class Pycask:
    _instance = None

    def __new__(cls, path: str = DEFAULT_PATH):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, path: str = DEFAULT_PATH):
        self.keydir = KeyDir()

        self.path = os.path.abspath(path)
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        self._load_keydir()
        self._run_merge()
        self._active_file = self._get_active_file()

    def _decode_header(self, header_bytes: bytes) -> tuple[int, int, int]:
        """Decode the header bytes into timestamp, key size, and value size."""
        return struct.unpack(HEADER_FORMAT, header_bytes)

    def _encode_header(self, timestamp: int, key_size: int, value_size: int) -> bytes:
        """Encode the timestamp, key size, and value size into header bytes."""
        return struct.pack(HEADER_FORMAT, timestamp, key_size, value_size)

    def _filename_to_id(self, filename: str) -> int:
        """Extract the file ID from the filename."""
        return int(filename.split('.')[0])

    def _id_to_filename(self, file_id: int) -> str:
        """Convert the file ID to the filename."""
        return "{:06d}.data".format(file_id)

    def _get_files(self) -> list[str]:
        """Get a sorted list of data files."""
        return sorted([f for f in os.listdir(self.path) if f.endswith(".data")])

    def _load_keydir(self) -> None:
        """Load the keydir from existing data files."""
        files = self._get_files()
        for file in files:
            file_id = self._filename_to_id(file)
            file_path = os.path.join(self.path, file)

            with open(file_path, "rb") as f:
                while header_bytes := f.read(HEADER_SIZE):
                    timestamp, key_size, value_size = self._decode_header(header_bytes)
                    if value_size == TOMBSTONE:
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

    def _create_file(self, file_id: int = 0) -> TextIO:
        """Create a new data file with the given file ID."""
        file_path = os.path.join(self.path, self._id_to_filename(file_id))
        return open(file_path, "ab+")

    def _get_active_file(self) -> TextIO:
        """Get the current active file for writing."""
        files = self._get_files()
        if not files:
            return self._create_file()

        latest_file = files[-1]
        latest_filename = os.path.basename(latest_file)
        latest_file_size = self._get_expected_file_size(latest_filename)

        if latest_file_size >= THRESHOLD:
            return self._create_file(self._filename_to_id(latest_filename) + 1)

        latest_file_path = os.path.join(self.path, latest_file)
        return open(latest_file_path, "ab+")

    def _get_expected_file_size(self, filename: str, key_size: int = 0, value_size: int = 0) -> int:
        """Calculate the expected file size after adding a new entry."""
        file_size = os.path.getsize(os.path.join(self.path, filename))
        entry_size = HEADER_SIZE + key_size + value_size
        return file_size + entry_size

    def _run_merge(self, interval: int = 60) -> None:
        """Run the merge process on a separate thread."""
        def wrapper():
            while True:
                time.sleep(interval)
                files = self._get_files()
                if len(files) >= LIMIT:
                    self._merge()
        t = threading.Thread(target=wrapper, daemon=True)
        t.start()

    def _merge(self):
        """Merge data files to remove obsolete entries and reduce storage."""
        files = self._get_files()
        files.remove(os.path.basename(self._active_file.name))

        filename = os.path.basename(self._active_file.name)
        new_file = self._create_file(self._filename_to_id(filename) + 1)

        for key, entry in self.keydir.items():
            old_filename = self._id_to_filename(entry.file_id)
            old_file_path = os.path.join(self.path, old_filename)

            with open(old_file_path, "rb") as f:
                f.seek(entry.value_pos)
                value = pickle.loads(f.read(entry.value_size))

            key_bytes, value_bytes = key.encode("utf-8"), pickle.dumps(value)
            key_size, value_size = len(key_bytes), len(value_bytes)
            header = self._encode_header(entry.timestamp, key_size, value_size)

            new_filename = os.path.basename(new_file.name)
            new_file_size = self._get_expected_file_size(new_filename, key_size, value_size)
            if new_file_size >= THRESHOLD:
                new_file.close()
                new_file = self._create_file(self._filename_to_id(new_filename) + 1)

            file_id = self._filename_to_id(os.path.basename(new_file.name))

            new_file.write(header)
            new_file.write(key_bytes)
            value_pos = new_file.tell()
            new_file.write(value_bytes)
            new_file.flush()

            self.keydir[key] = KeyEntry(
                file_id=file_id,
                value_size=value_size,
                value_pos=value_pos,
                timestamp=entry.timestamp
            )

        for file in files: os.remove(os.path.join(self.path, file))

    def put(self, key: str, value: Any) -> None:
        """Store a key-value pair."""
        now = int(time.time())
        key_bytes, value_bytes = key.encode("utf-8"), pickle.dumps(value)
        key_size, value_size = len(key_bytes), len(value_bytes)
        header = self._encode_header(now, key_size, value_size)

        filename = os.path.basename(self._active_file.name)
        file_size = self._get_expected_file_size(filename, key_size, value_size)

        if file_size >= THRESHOLD:
            self._active_file.close()
            self._active_file = self._create_file(self._filename_to_id(filename) + 1)

        file_id = self._filename_to_id(os.path.basename(self._active_file.name))

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

    def get(self, key: str) -> Any:
        """Retrieve the value associated with the given key."""
        entry = self.keydir.get(key, None)
        if entry is None:
            raise KeyError(f"Key '{key}' not found.")
        if entry.value_size == TOMBSTONE:
            raise KeyError(f"Key '{key}' has been deleted.")

        filename = self._id_to_filename(entry.file_id)
        file_path = os.path.join(self.path, filename)

        with open(file_path, "rb") as f:
            f.seek(entry.value_pos)
            value = pickle.loads(f.read(entry.value_size))

        return value

    def delete(self, key: str) -> None:
        """Delete the value associated with the given key."""
        entry = self.keydir.pop(key, None)
        if entry is None:
            raise KeyError(f"Key '{key}' not found.")

        now = int(time.time())
        key_bytes = key.encode("utf-8")
        key_size, value_size = len(key_bytes), TOMBSTONE
        header = self._encode_header(now, key_size, value_size)

        filename = os.path.basename(self._active_file.name)
        file_size = self._get_expected_file_size(filename, key_size, value_size)

        if file_size >= THRESHOLD:
            self._active_file.close()
            self._active_file = self._create_file(self._filename_to_id(filename) + 1)

        self._active_file.write(header)
        self._active_file.write(key_bytes)
        self._active_file.flush()
