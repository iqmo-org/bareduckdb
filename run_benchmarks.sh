#!/bin/sh

deactivate 

# Baseline against duckdb
uv venv --clear .venv-duckdb -p 3.14
uv pip install duckdb pyarrow pytest polars pytest-forked pytest-repeat --python .venv-duckdb/bin/python
UV_PROJECT_ENVIRONMENT=.venv-duckdb uv run pytest tests/benchmarks \
  --confcutdir=tests/benchmarks \
  -o "addopts=" \
  --forked \
  --count=3 \
  -v \
  --use-duckdb --benchmark-suffix=duckdb \
  --registration-modes=polars_lazy,arrow,parquet


# Current code
uv venv .venv314 --clear -p cp314
UV_PROJECT_ENVIRONMENT=.venv314 uv sync --reinstall
uv pip install polars --python .venv314/bin/python

UV_PROJECT_ENVIRONMENT=.venv314 uv run pytest tests/benchmarks \
  --forked \
  --count=3 \
  -n 0 \
  --no-cov \
  -v \
  --benchmark-suffix=dev314 \
  --registration-modes=polars_lazy,arrow,parquet
    

uv run python tests/benchmarks/compare_results.py
