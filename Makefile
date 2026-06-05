.PHONY: install install-dev test test-all lint clean

install:
	pip install -e .

install-dev:
	pip install -e ../bifrost-trade-core -e . -e ../bifrost-trade-api -e ".[dev]"

test:
	pytest -m 'not ib and not db'

test-all:
	pytest

lint:
	ruff check src/ tests/

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
