test:
	PYTHONPATH=${HOME}/pycask/src python3 -m unittest discover tests/

.PHONY: test
