from dataclasses import dataclass


@dataclass
class Value:
    file_id: int
    value_size: int
    value_pos: int
    timestamp: int


class KeyDir(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __setitem__(self, key: str, value: Value):
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> Value:
        return super().__getitem__(key)

    def __delitem__(self, key: str):
        super().__delitem__(key)
