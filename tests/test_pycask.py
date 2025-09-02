import time
import pytest
import tempfile
from pycask import Pycask

@pytest.mark.parametrize("key, value", [
    ("key1", "value1"),
    ("key2", "value2"),
    ("key3", "value3"),
])
def test_key_value_put_and_get(key, value):
    with tempfile.TemporaryDirectory() as temp_dir:
        p = Pycask(temp_dir)
        p.put(key, value)
        assert p.get(key) == value

@pytest.mark.parametrize("key, value", [
    ("key1", "value1"),
    ("key2", "value2"),
    ("key3", "value3"),
])
def test_key_value_delete(key, value):
    with tempfile.TemporaryDirectory() as temp_dir:
        p = Pycask(temp_dir)
        p.put(key, value)
        p.delete(key)
        with pytest.raises(KeyError):
            p.get(key)

@pytest.mark.parametrize("keys, values", [
    (
        ["key1", "key2", "key3", "key1", "key4", "key6", "key7", "key8", "key5", "key2"],
        ["value1", "value2", "value3", "value5", "value4", "value6", "value7", "value8", "value9", "value10"]
    ),
])
def test_merge(keys, values):
    with tempfile.TemporaryDirectory() as temp_dir:
        p = Pycask(temp_dir)
        mem = {}
        for key, value in zip(keys, values):
            p.put(key, value)
            mem[key] = value
        time.sleep(60)
        for key, value in mem.items():
            assert p.get(key) == value
