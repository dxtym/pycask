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
        """Set a key-value pair in the keydir."""
        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> KeyEntry:
        """Get a key-value pair from the keydir."""
        return super().__getitem__(key)

    def __delitem__(self, key: str) -> None:
        """Delete a key-value pair from the keydir."""
        super().__delitem__(key)
