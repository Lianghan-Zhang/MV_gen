"""Deterministic SQL feature extraction built on sqlglot."""

from sqlparser.extractor import DEFAULT_DIALECT, extract_sql_features

__all__ = ["DEFAULT_DIALECT", "extract_sql_features"]
