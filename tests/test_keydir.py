import pytest
from pycask import KeyDir, KeyEntry

@pytest.mark.parametrize("key, value", [
    ("a", KeyEntry(file_id=1, value_size=10, value_pos=10, timestamp=1234567890)),
    ("b", KeyEntry(file_id=2, value_size=20, value_pos=20, timestamp=1234567891)),
    ("c", KeyEntry(file_id=3, value_size=30, value_pos=30, timestamp=1234567892)),
])
def test_key_set_and_get(key, value):
    keydir = KeyDir()
    keydir[key] = value
    assert keydir[key] == value

@pytest.mark.parametrize("key, value", [
    ("a", KeyEntry(file_id=1, value_size=10, value_pos=10, timestamp=1234567890)),
    ("b", KeyEntry(file_id=2, value_size=20, value_pos=20, timestamp=1234567891)),
    ("c", KeyEntry(file_id=3, value_size=30, value_pos=30, timestamp=1234567892)),
])
def test_key_delete(key, value):
    keydir = KeyDir()
    keydir[key] = value
    del keydir[key]
