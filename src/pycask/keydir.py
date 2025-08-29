from dataclasses import dataclass


@dataclass
class KeyEntry:
    file_id: int
    value_size: int
    value_pos: int
    timestamp: int


class KeyDir(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)

    def __getitem__(self, key):
        return super().__getitem__(key)

    def __delitem__(self, key):
        super().__delitem__(key)
