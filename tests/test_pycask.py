import os
import pytest
import tempfile
from pycask import Pycask

@pytest.mark.parametrize("key, value", [
    ("1", 1),
    ("2", "2"),
    ("3", [1, 2, 3]),
    ("4", {"a": 1, "b": 2}),
])
def test_key_value_put_and_get(key, value):
    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        p = Pycask(temp_dir)
        p.put(key, value)
        assert p.get(key) == value

@pytest.mark.parametrize("key, value", [
    ("1", 1),
    ("2", "2"),
    ("3", [1, 2, 3]),
    ("4", {"a": 1, "b": 2}),
])
def test_key_value_delete(key, value):
    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        p = Pycask(temp_dir)
        p.put(key, value)
        p.delete(key)
        with pytest.raises(KeyError):
            p.get(key)

@pytest.mark.parametrize("keys, values", [
    (["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"], [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
    (["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"], ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]),
    (["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"], [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]]),
    (["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"], [{"a": 1}, {"b": 2}, {"c": 3}, {"d": 4}, {"e": 5}, {"f": 6}, {"g": 7}, {"h": 8}, {"i": 9}, {"j": 10}]),
])
def test_merge(keys, values):
    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        p = Pycask(temp_dir)
        mem = {}
        for key, value in zip(keys, values):
            p.put(key, value)
            mem[key] = value
        p._merge()

        assert len(os.listdir(temp_dir)) == 2
        for key, value in mem.items():
            assert p.get(key) == value
