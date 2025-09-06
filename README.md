# Pycask

A log-structured key-value storage engine based on the Bitcask paper. Read more [here](https://riak.com/assets/bitcask-intro.pdf).

## Features

- **Simple API**: Easy-to-use key-value interface
- **Fast Operations**: Constant time reads and writes
- **Merge Process**: Automatic compaction to reclaim disk space

## Installation

1. Clone the repository:
```bash
git clone https://github.com/dxtym/pycask.git
```

2. Install the package:
```bash
pip install -e pycask
```

## Quickstart

```python
from pycask import Pycask

p = Pycask('/path/to/data')

p.put('key1', 'value1')
p.put('key2', 'value2')

p.delete('key2')

value = p.get('key1')
```

## Development

1. Install with dev dependencies:
```bash
poetry install --extras dev
```

2. Run unit tests:
```bash
pytest tests/ -v
```

## License

Unlicense. See [LICENSE](LICENSE) for details.
