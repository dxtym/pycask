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

    def __setitem__(self, key: str, value: KeyEntry) -> None:
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> KeyEntry:
        return super().__getitem__(key)

    def __delitem__(self, key: str) -> None:
        super().__delitem__(key)
